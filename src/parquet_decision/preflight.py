"""Complete source-bound feasibility before a plan can enter review.

The public wrapper is native-free; its fixed fresh worker hashes/copies inputs,
reads only those snapshots, reconciles every row and exercises safe casts and
Parquet writes. Metadata decisions never waive feasibility.
"""
from __future__ import annotations

import io
import errno
from pathlib import Path
import tempfile

from .model import (Limits, LimitError, ModelError, Plan, PreflightCounters,
                    Snapshot, SourceSpec, UnsupportedError, WorkerRequest,
                    WorkerResult, dump_canonical, load_plan, validate_inventory, _relative_path)


class _DataIncompatible(ValueError):
    pass


def _failure(status: str, limits: Limits, diagnostics: tuple[str, ...]) -> Plan:
    return Plan(status=status, limits=limits, diagnostics=diagnostics)


def preflight(inputs: list[Path], source_root: Path, limits: Limits) -> Plan:
    """Inspect explicit relative filenames in a supervised fresh worker."""
    if type(inputs) is not list or not inputs or not all(isinstance(p, Path) for p in inputs):
        raise ModelError('preflight needs a nonempty ordered list of relative Paths')
    if not isinstance(source_root, Path) or not isinstance(limits, Limits):
        raise ModelError('preflight requires a source root Path and strict Limits')
    if len(inputs) > limits.max_files:
        return _failure('LIMIT_EXCEEDED', limits, (f'input files {len(inputs)} exceed cap {limits.max_files}',))
    from .supervise import run_worker, _os_error_status, _diagnostic
    try:
        with tempfile.TemporaryDirectory(prefix='parquet-decision-preflight-') as directory:
            staging = Path(directory)
            destination = staging / 'plan.json'
            request = WorkerRequest(1, 'preflight', tuple(str(p) for p in inputs),
                                    (('source_root', str(source_root.absolute())), ('staging', str(staging)),
                                     ('destination', str(destination))))
            result = run_worker(request, limits)
            # Hard terminal failures can leave private success artifacts. Their
            # observed cause wins; reading an artifact must not reclassify them.
            if result.status in ('ERROR', 'CANCELLED', 'INCOMPLETE', 'LIMIT_EXCEEDED'):
                return _failure(result.status, limits, result.diagnostics)
            if not destination.exists():
                if result.status == 'COMPLETE':
                    return _failure('ERROR', limits, ('complete worker omitted its request-bound plan artifact',))
                return _failure(result.status, limits, result.diagnostics)
            try:
                plan = load_plan(destination, limits)
                expected = 'COMPLETE' if plan.preflight_complete else plan.status
                if result.status != expected or plan.limits != limits:
                    raise ModelError('preflight artifact status/limits do not match terminal result')
                counters = dict(result.counters)
                actual = plan.preflight_counters
                if (counters.get('files'), counters.get('rows'), counters.get('input_bytes')) != (actual.files, actual.rows, actual.input_bytes):
                    raise ModelError('preflight artifact counters do not match terminal result')
                return plan
            except OSError as exc:
                return _failure(_os_error_status(exc), limits, (_diagnostic(f'parent preflight artifact read failed: {exc}'),))
            except ModelError as exc:
                return _failure('ERROR', limits, (_diagnostic(f'invalid preflight artifact: {exc}'),))
    except OSError as exc:
        return _failure(_os_error_status(exc), limits, (_diagnostic(f'parent preflight setup/cleanup failed: {exc}'),))


class _WriteBudget:
    def __init__(self, limits: Limits, baseline: int):
        self.limits, self.baseline, self.output_bytes = limits, baseline, 0

    def consume(self, size: int) -> None:
        measured = self.output_bytes + size
        if measured > self.limits.max_output_bytes:
            raise LimitError(f'preflight output bytes {measured} exceed cap {self.limits.max_output_bytes}')
        if self.baseline + measured > self.limits.max_staging_bytes:
            raise LimitError(f'owned staging bytes {self.baseline + measured} exceed cap {self.limits.max_staging_bytes}')
        self.output_bytes = measured


class _BoundedSink(io.RawIOBase):
    """Count native writer bytes before each write, not only after buffering."""
    def __init__(self, path: Path, budget: _WriteBudget):
        super().__init__()
        self.stream, self.budget = path.open('xb'), budget

    def writable(self):
        return True

    def tell(self):
        return self.stream.tell()

    def write(self, data):
        self.budget.consume(len(data))
        return self.stream.write(data)

    def flush(self):
        if not self.stream.closed:
            self.stream.flush()

    def close(self):
        if not self.closed:
            super().close()
            self.stream.close()


def _feasible_array(array, source_type, target_type, path: tuple[str, ...]):
    """Preflight-only align/cast; never a verifier oracle or transformer helper."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from .model import type_to_arrow, field_to_arrow
    target = type_to_arrow(target_type)
    if source_type is None or source_type.kind == 'null':
        return pa.nulls(len(array), type=target)
    if source_type.kind == 'struct':
        by_name = {f.name: (i, f) for i, f in enumerate(source_type.fields)}
        arrays = []
        validity = pc.is_valid(array)
        for field in target_type.fields:
            original = by_name.get(field.name)
            if original is None:
                arrays.append(pa.nulls(len(array), type=type_to_arrow(field.type)))
            else:
                index, original_field = original
                child = array.field(index)
                # Hidden values under null parents are not observations and must
                # not cause false overflow/cast refusals.
                child = pc.if_else(validity, child, pa.scalar(None, type=child.type))
                arrays.append(_feasible_array(child, original_field.type, field.type, (*path, field.name)))
        return pa.StructArray.from_arrays(arrays, fields=[field_to_arrow(f) for f in target_type.fields], mask=pc.is_null(array))
    if source_type.kind == 'timestamp' and source_type != target_type:
        exponent = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
        factor = 10 ** (exponent[target_type.unit] - exponent[source_type.unit])
        lower = -(2**63 // factor)  # ceil(-2**63 / factor), integer only
        upper = (2**63 - 1) // factor
        for tick in pc.cast(array, pa.int64()).to_pylist():
            if tick is not None and not lower <= tick <= upper:
                raise _DataIncompatible(f'timestamp multiplication overflow at {path!r}: ticks {tick}, factor {factor}')
    if source_type == target_type:
        return array
    return pc.cast(array, target, safe=True)


def _native_scan(copies, staging: Path, limits: Limits) -> Plan:
    from .supervise import _require_worker, _diagnostic
    _require_worker()
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .model import _fields_from_arrow, field_to_arrow, schema_to_arrow
    from .schema import plan_schema
    from .source import _open_snapshot, _staging_bytes, iter_source_batches
    sources, proposal = [], None
    files, rows = len(copies), 0
    input_bytes = sum(c.size_bytes for c in copies)
    status, diagnostics = 'ERROR', ()
    try:
        if pa.__version__ != '25.0.0':
            raise UnsupportedError('fresh worker requires PyArrow 25.0.0')
        pa.set_cpu_count(1); pa.set_io_thread_count(1)
        for copy in copies:
            # A placeholder record binds only bytes before the native schema has
            # been read. It is never admitted to the returned source inventory.
            placeholder = SourceSpec(copy.source_id, copy.relative_path, copy.sha256, copy.size_bytes, 0, ())
            with _open_snapshot(Snapshot(placeholder, copy.local_path)) as stream:
                reader = pq.ParquetFile(stream, arrow_extensions_enabled=False)
                fields, metadata = _fields_from_arrow(reader.schema_arrow)
                num_rows = reader.metadata.num_rows
            source = SourceSpec(copy.source_id, copy.relative_path, copy.sha256, copy.size_bytes, num_rows, fields, metadata)
            validate_inventory(tuple([*sources, source]), limits)
            sources.append(source)
        proposal = plan_schema(sources)
        # Enforce lowered target caps before any output allocation.
        Plan('INCOMPLETE', sources=tuple(sources), proposal=proposal, limits=limits)
        schema = schema_to_arrow(proposal.fields, proposal.schema_metadata)
        schema = schema.append(pa.field('__pc_source', pa.uint32(), nullable=False)).append(pa.field('__pc_row', pa.uint64(), nullable=False))
        budget = _WriteBudget(limits, _staging_bytes(staging, limits))
        for source, copy in zip(sources, copies, strict=True):
            sink = _BoundedSink(staging / f'feasibility-{source.source_id:05d}.parquet', budget)
            with sink:
                with pq.ParquetWriter(sink, schema, version='2.6', compression='NONE', use_dictionary=False) as writer:
                    mapping = {f.name: (i, f) for i, f in enumerate(source.fields)}
                    source_rows = 0
                    for batch in iter_source_batches(Snapshot(source, copy.local_path), limits.batch_rows):
                        measured = rows + batch.rows
                        if measured > limits.max_rows:
                            raise LimitError(f'scanned rows {measured} exceed cap {limits.max_rows}')
                        rows = measured
                        source_rows += batch.rows
                        arrays = []
                        for target_field in proposal.fields:
                            original = mapping.get(target_field.name)
                            if original is None:
                                arrays.append(pa.nulls(batch.rows, type=field_to_arrow(target_field).type))
                            else:
                                index, field = original
                                arrays.append(_feasible_array(batch.record_batch.column(index), field.type, target_field.type, (target_field.name,)))
                        arrays.extend([pa.array([source.source_id] * batch.rows, type=pa.uint32()),
                                       pa.array(range(batch.first_row, batch.first_row + batch.rows), type=pa.uint64())])
                        writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
                    if source_rows != source.num_rows:
                        raise ModelError('scanned rows do not reconcile individual source inventory')
        if rows != sum(s.num_rows for s in sources):
            raise ModelError('complete preflight row counters do not reconcile inventory')
        status = 'NEEDS_DECISIONS' if proposal.conflicts else 'READY_FOR_REVIEW'
    except UnsupportedError as exc:
        status, diagnostics = 'UNSUPPORTED', (_diagnostic(exc),)
    except LimitError as exc:
        status, diagnostics = 'LIMIT_EXCEEDED', (_diagnostic(exc),)
    except _DataIncompatible as exc:
        status, diagnostics = 'DATA_INCOMPATIBLE', (_diagnostic(exc),)
    except (pa.ArrowNotImplementedError, NotImplementedError) as exc:
        status, diagnostics = 'UNSUPPORTED', (_diagnostic(f'Parquet feasibility unsupported: {exc}'),)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        status, diagnostics = 'DATA_INCOMPATIBLE', (_diagnostic(f'native safe cast/write refused: {exc}'),)
    except (MemoryError, pa.ArrowMemoryError) as exc:
        status, diagnostics = 'LIMIT_EXCEEDED', (_diagnostic(f'{type(exc).__name__}: recognized allocation refusal under RLIMIT_AS'),)
    except FileNotFoundError as exc:
        status, diagnostics = 'INCOMPLETE', (_diagnostic(exc),)
    except OSError as exc:
        status = 'LIMIT_EXCEEDED' if exc.errno in (errno.ENOMEM, errno.ENOSPC, errno.EDQUOT) else 'ERROR'
        diagnostics = (_diagnostic(f'{type(exc).__name__}: {exc}'),)
    except Exception as exc:
        status, diagnostics = 'ERROR', (_diagnostic(f'{type(exc).__name__}: {exc}'),)
    # Proposal/partial inventory may itself exceed the JSON envelope. Dropping
    # unadmitted records from a failure is honest; it never yields review status.
    try:
        return Plan(status, tuple(sources), proposal, status in ('READY_FOR_REVIEW', 'NEEDS_DECISIONS'), limits=limits,
                    diagnostics=diagnostics, preflight_counters=PreflightCounters(files, rows, input_bytes))
    except LimitError as exc:
        return _failure('LIMIT_EXCEEDED', limits, (_diagnostic(exc),))


def _preflight_worker(request: WorkerRequest, limits: Limits) -> WorkerResult:
    """Fixed operation: private snapshots and one exclusive request-bound artifact."""
    from .supervise import _require_worker, _diagnostic
    from .source import _directory, _snapshot_inputs, _staging_bytes
    _require_worker()
    args = dict(request.args)
    if set(args) != {'source_root', 'staging', 'destination'}:
        raise ModelError('preflight requires exactly source_root, staging and destination')
    # These are source identifiers, unlike generic internal WorkerRequest paths.
    # Validate their retained JSON spellings before pathlib can normalize them.
    identifiers = [_relative_path(identifier) for identifier in request.paths]
    staging, root = _directory(Path(args['staging'])), _directory(Path(args['source_root']))
    destination = Path(args['destination']).absolute()
    if destination.parent != staging or destination.exists() or destination.is_symlink():
        raise ModelError('preflight destination must be a new direct child of owned staging')
    if staging == root or staging.is_relative_to(root):
        raise ModelError('preflight staging must be outside source root')
    try:
        copies = _snapshot_inputs([Path(p) for p in identifiers], root, staging, limits)
        plan = _native_scan(copies, staging, limits)
    except UnsupportedError as exc:
        plan = _failure('UNSUPPORTED', limits, (_diagnostic(exc),))
    except LimitError as exc:
        plan = _failure('LIMIT_EXCEEDED', limits, (_diagnostic(exc),))
    except FileNotFoundError as exc:
        plan = _failure('INCOMPLETE', limits, (_diagnostic(exc),))
    except ModelError as exc:
        plan = _failure('ERROR', limits, (_diagnostic(exc),))
    data = dump_canonical(plan)
    measured = _staging_bytes(staging, limits) + len(data)
    if measured > limits.max_staging_bytes:
        raise LimitError(f'owned staging bytes {measured} exceed cap {limits.max_staging_bytes} including plan artifact')
    with destination.open('xb') as output:
        output.write(data)
    counters = plan.preflight_counters
    return WorkerResult(1, 'preflight', 'COMPLETE' if plan.preflight_complete else plan.status,
                        (('files', counters.files), ('input_bytes', counters.input_bytes), ('rows', counters.rows)), plan.diagnostics)
