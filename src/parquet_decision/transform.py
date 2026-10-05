"""Fresh-worker batch rewrite; no defaults, semantic conversion or filtering."""
from __future__ import annotations

import hashlib
import io
from pathlib import Path
import tempfile

from .model import (ApprovedPlan, Limits, LimitError, ModelError, PartRecord, RuntimeInfo,
                    Snapshot, UnsupportedError, WorkerRequest, canonical_json, schema_digest)


class _Budget:
    """Charge all bytes before writes, including native footers and control JSON."""
    def __init__(self, limits, owned, dataset, *, reserve_bytes=0):
        from .source import _staging_bytes
        self.limits, self.owned, self.dataset = limits, owned, dataset
        self.reserve_bytes = reserve_bytes
        dataset_bytes = _staging_bytes(dataset, limits)
        owned_bytes = _staging_bytes(owned, limits)
        self.output_bytes = dataset_bytes
        self.staging_bytes = owned_bytes + (0 if dataset.is_relative_to(owned) else dataset_bytes)
        self.check(0, output=False)

    def check(self, size, *, output):
        if self.staging_bytes + size + self.reserve_bytes > self.limits.max_staging_bytes:
            raise LimitError('total owned staging bytes exceed approved cap')
        if self.output_bytes + (size if output else 0) > self.limits.max_output_bytes:
            raise LimitError('total bundle output bytes exceed approved cap')

    def consume(self, size, *, output=True):
        self.check(size, output=output)
        self.staging_bytes += size
        if output: self.output_bytes += size

    def write(self, path, raw, *, output):
        if len(raw) > self.limits.max_plan_bytes:
            raise LimitError('bundle/control JSON bytes exceed approved cap')
        self.check(len(raw), output=output)
        with path.open('xb') as stream:
            self.consume(len(raw), output=output)
            stream.write(raw)
            stream.flush()


class _Sink(io.RawIOBase):
    def __init__(self, path, budget):
        super().__init__()
        self.stream, self.budget = path.open('xb'), budget

    def writable(self): return True
    def tell(self): return self.stream.tell()

    def write(self, raw):
        self.budget.consume(len(raw))
        size = self.stream.write(raw)
        if size != len(raw): raise OSError('short native output write')
        return size

    def flush(self):
        if not self.stream.closed: self.stream.flush()

    def close(self):
        if not self.closed:
            try: super().close()
            finally: self.stream.close()


def _align(array, source_type, target_type):
    """Forward-only alignment. The independent verifier never calls this code."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from .model import field_to_arrow, type_to_arrow
    target = type_to_arrow(target_type)
    if source_type is None or source_type.kind == 'null':
        return pa.nulls(len(array), type=target)
    if source_type.kind == 'struct':
        original = {field.name: (index, field) for index, field in enumerate(source_type.fields)}
        valid = pc.is_valid(array)
        children = []
        for field in target_type.fields:
            entry = original.get(field.name)
            if entry is None:
                children.append(pa.nulls(len(array), type=type_to_arrow(field.type)))
            else:
                index, old = entry
                child = array.field(index)
                # Hidden values beneath a null parent are not observations.
                child = pc.if_else(valid, child, pa.scalar(None, type=child.type))
                children.append(_align(child, old.type, field.type))
        return pa.StructArray.from_arrays(children, fields=[field_to_arrow(f) for f in target_type.fields], mask=pc.is_null(array))
    if source_type.kind == 'timestamp' and source_type != target_type:
        powers = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
        factor = 10 ** (powers[target_type.unit] - powers[source_type.unit])
        if factor < 1: raise ModelError('rewrite cannot narrow timestamp units')
        lower, upper = -(2**63 // factor), (2**63 - 1) // factor
        for tick in pc.cast(array, pa.int64()).to_pylist():
            if tick is not None and not lower <= tick <= upper:
                raise ModelError('rewrite timestamp multiplication overflows int64')
    return array if source_type == target_type else pc.cast(array, target, safe=True)


def _rewrite(approved: ApprovedPlan, snapshots: list[Snapshot], staging: Path, *, owned=None, budget=None) -> list[PartRecord]:
    """Worker-local transformer, called only beneath the fixed outer watchdog."""
    from .supervise import _require_worker
    _require_worker()
    from .source import _directory, iter_source_batches
    from .model import schema_to_arrow, type_to_arrow
    if not isinstance(approved, ApprovedPlan): raise ModelError('rewrite requires an ApprovedPlan')
    if approved.plan.runtime != RuntimeInfo(): raise UnsupportedError('rewrite runtime differs from qualified profile')
    if type(snapshots) is not list or tuple(s.source for s in snapshots) != approved.plan.sources:
        raise ModelError('rewrite snapshots differ from approved ordered inventory')
    if any(s.local_path is None for s in snapshots): raise FileNotFoundError('rewrite snapshots are unavailable')
    stage = _directory(staging)
    if list(stage.iterdir()): raise ModelError('rewrite dataset must be empty')
    if any(s.local_path.is_relative_to(stage) for s in snapshots):
        raise ModelError('rewrite snapshots must be outside dataset')
    import pyarrow as pa
    import pyarrow.parquet as pq
    if pa.__version__ != '25.0.0': raise UnsupportedError('rewrite requires PyArrow 25.0.0')
    pa.set_cpu_count(1); pa.set_io_thread_count(1)
    limits, proposal = approved.plan.limits, approved.plan.proposal
    budget = budget or _Budget(limits, stage if owned is None else owned, stage)
    schema = schema_to_arrow(proposal.fields, proposal.schema_metadata).append(
        pa.field('__pc_source', pa.uint32(), nullable=False)).append(
        pa.field('__pc_row', pa.uint64(), nullable=False))
    target_digest = schema_digest(proposal.fields, proposal.schema_metadata)
    parts, total_rows = [], 0
    for snapshot in snapshots:
        source = snapshot.source
        path = stage / f'part-{source.source_id:05d}.parquet'
        mapping = {f.name: (i, f) for i, f in enumerate(source.fields)}
        rows = 0
        with _Sink(path, budget) as sink:
            with pq.ParquetWriter(sink, schema, version='2.6', compression='NONE', use_dictionary=False) as writer:
                for batch in iter_source_batches(snapshot, limits.batch_rows):
                    if batch.first_row != rows: raise ModelError('rewrite source row addresses do not reconcile')
                    total_rows += batch.rows
                    if total_rows > limits.max_rows: raise LimitError('rewrite row cap exceeded')
                    arrays = []
                    for field in proposal.fields:
                        old = mapping.get(field.name)
                        arrays.append(pa.nulls(batch.rows, type=type_to_arrow(field.type)) if old is None else
                                      _align(batch.record_batch.column(old[0]), old[1].type, field.type))
                    arrays.extend((pa.array([source.source_id] * batch.rows, type=pa.uint32()),
                                   pa.array(range(rows, rows + batch.rows), type=pa.uint64())))
                    writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
                    rows += batch.rows
        if rows != source.num_rows: raise ModelError('rewrite source row count differs from approved inventory')
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            while chunk := stream.read(1024 * 1024): digest.update(chunk)
        parts.append(PartRecord(path.name, source.source_id, rows, path.stat().st_size,
                                digest.hexdigest(), target_digest, path))
    if total_rows != sum(s.num_rows for s in approved.plan.sources): raise ModelError('rewrite total rows differ')
    return parts


def rewrite(approved: ApprovedPlan, snapshots: list[Snapshot], staging: Path) -> list[PartRecord]:
    """Run only the fixed rewrite worker; standalone parts are not a verified bundle.

    The caller owns an existing empty dataset directory. Borrowed snapshots are
    read-only; the dataset and this call's private control bytes are accounted.
    On failure partial parts remain caller-owned and no completion is claimed.
    """
    from .bundle import _control_write, _read_response, _validate_terminal, _hash
    from .source import _directory
    from .supervise import run_worker
    if not isinstance(approved, ApprovedPlan): raise ModelError('rewrite requires an ApprovedPlan')
    if type(snapshots) is not list or tuple(s.source for s in snapshots) != approved.plan.sources:
        raise ModelError('rewrite snapshots differ from approved inventory')
    if any(s.local_path is None for s in snapshots): raise FileNotFoundError('rewrite snapshots are unavailable')
    dataset = _directory(staging)
    if list(dataset.iterdir()): raise ModelError('rewrite dataset must be empty')
    limits = approved.plan.limits
    with tempfile.TemporaryDirectory(prefix='.pdecision-rewrite-', dir=dataset.parent) as directory:
        stage = Path(directory)
        _control_write(stage / 'approved.json', canonical_json(approved.to_dict()), stage, limits)
        request = WorkerRequest(1, 'rewrite', tuple(str(s.local_path.absolute()) for s in snapshots),
                                (('approved_plan', str(stage / 'approved.json')), ('staging', str(stage)),
                                 ('dataset', str(dataset)), ('destination', str(stage / 'result.json'))))
        terminal = run_worker(request, limits)
        if terminal.status != 'COMPLETE':
            message = '; '.join(terminal.diagnostics) or terminal.status
            if terminal.status == 'LIMIT_EXCEEDED': raise LimitError(message)
            if terminal.status == 'UNSUPPORTED': raise UnsupportedError(message)
            raise ModelError(message)
        envelope = _read_response(stage / 'result.json', request, limits)
        if type(envelope['result']) is not list or envelope['metadata_sha256'] != {}:
            raise ModelError('invalid standalone rewrite result')
        if len(envelope['result']) != len(snapshots):
            raise ModelError('raw standalone part count differs from approved snapshot inventory')
        parts = [PartRecord.from_dict(p) for p in envelope['result']]
        if (len(parts) != len(snapshots)
                or [p.source_id for p in parts] != [s.source.source_id for s in snapshots]
                or any(p.rows != s.source.num_rows for p, s in zip(parts, snapshots, strict=True))
                or any(p.target_schema_digest != schema_digest(approved.plan.proposal.fields, approved.plan.proposal.schema_metadata) for p in parts)):
            raise ModelError('standalone rewrite part/source bindings differ')
        if {p.name for p in dataset.iterdir()} != {p.filename for p in parts}:
            raise ModelError('standalone rewrite exact part inventory differs')
        for part in parts:
            if _hash(dataset / part.filename, limits.max_output_bytes) != (part.bytes, part.sha256):
                raise ModelError('standalone rewrite actual part hashes differ')
        from .source import _staging_bytes
        if (_staging_bytes(dataset, limits) != envelope['bundle_bytes']
                or _staging_bytes(stage, limits) + _staging_bytes(dataset, limits) != envelope['staging_bytes']):
            raise ModelError('standalone rewrite output/staging accounting differs')
        counts = dict(files=len(snapshots), rows=sum(s.source.num_rows for s in snapshots),
                      input_bytes=sum(s.source.size_bytes for s in snapshots), part_bytes=sum(p.bytes for p in parts),
                      bundle_bytes=envelope['bundle_bytes'], staging_bytes=envelope['staging_bytes'])
        _validate_terminal(terminal, counts, limits)
        from dataclasses import replace
        return [replace(p, local_path=dataset / p.filename) for p in parts]
