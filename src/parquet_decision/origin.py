"""Output integrity and original interpretation, with explicit fresh-source mode."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import time

from .bundle import read_bundle, _elapsed
from .model import (Limits, ModelError, OriginRecord, PartRecord, Snapshot, VerificationReport,
                    WorkerRequest, WorkerResult, canonical_json, digest_json, load_approved_plan)
from .source import _directory
from .supervise import run_worker
from .verify_values import (_exception_status, _failure, _private_write, _read_private,
                            verify_streams)


class OriginCheckError(ModelError):
    """A bounded integrity operation refused with its original status."""
    def __init__(self, status: str, message: str):
        self.status = status
        super().__init__(message)


def _coordinates(approved, source_id, row):
    if type(source_id) is not int or not 0 <= source_id < len(approved.plan.sources):
        raise ModelError('source-id must be an integer in the approved source inventory')
    source = approved.plan.sources[source_id]
    if type(row) is not int or not 0 <= row < source.num_rows:
        raise ModelError('row must be an integer in the selected original source range')
    return source


def verify_bundle(bundle: Path, source_root: Path, approved=None) -> VerificationReport:
    """Fresh complete actual-source schema/value/provenance check of a bundle.

    Source paths are reopened and hash/identity checked inside the qualified
    verifier. They must remain trusted and quiescent for the complete operation.
    The stored report is used only for integrity bindings, never as the oracle.
    """
    start = time.monotonic()
    try:
        stored, parts, historical, manifest = read_bundle(bundle, deadline=start + Limits().wall_seconds)
        deadline = start + stored.plan.limits.wall_seconds
        _elapsed(deadline)
        if approved is not None and approved != stored:
            raise ModelError('supplied approval differs from bundle decision record')
        root = _directory(source_root)
        snapshots = [Snapshot(source, root / source.relative_path) for source in stored.plan.sources]
        report = verify_streams(stored, snapshots, parts)
        _elapsed(deadline)
        return report
    except Exception as exc:
        return _failure(_exception_status(exc), exc, approved)


def _check_output_provenance(approved, parts):
    """Fresh fixed worker verifies all output row coordinates without sources."""
    limits = approved.plan.limits
    with tempfile.TemporaryDirectory(prefix='parquet-decision-origin-') as directory:
        stage = Path(directory)
        _private_write(stage / 'approved.json', canonical_json(approved.to_dict()), stage, limits)
        _private_write(stage / 'request.json', canonical_json({'mode': 'origin', 'parts': [p.to_dict() for p in parts]}), stage, limits)
        request = WorkerRequest(1, 'verify', tuple(str(p.local_path.absolute()) for p in parts),
                                (('plan', str(stage / 'request.json')), ('staging', str(stage)),
                                 ('destination', str(stage / 'result.json')), ('approved_plan', str(stage / 'approved.json'))))
        terminal = run_worker(request, limits)
        if terminal.status != 'COMPLETE':
            raise OriginCheckError(terminal.status, '; '.join(terminal.diagnostics) or terminal.status)
        envelope = _read_private(stage / 'result.json', limits)
        expected = {'files': len(parts), 'rows': sum(p.rows for p in parts)}
        counters = dict(terminal.counters)
        if (type(envelope) is not dict or set(envelope) != {'request_digest', 'result'}
                or envelope['request_digest'] != digest_json(request.to_dict())
                or envelope['result'] != {'integrity_level': 'OUTPUT_HASHES_MATCH', 'counters': expected}
                or set(counters) != set(expected) | {'address_space_bytes', 'cpu_seconds', 'peak_rss_kib'}
                or any(counters[k] != v for k, v in expected.items())
                or counters['address_space_bytes'] != limits.address_space_bytes
                or counters['cpu_seconds'] != limits.cpu_seconds):
            raise OriginCheckError('ERROR', 'origin worker terminal/artifact bindings differ')


def _origin_worker(request, limits, descriptor, stage, destination):
    """Fixed output-provenance mode of the existing isolated verify operation."""
    from .supervise import _require_worker
    from .verify_values import _check_part_inventory, _check_schema, _open_part, _fail
    _require_worker()
    args = dict(request.args)
    if set(descriptor) != {'mode', 'parts'} or 'approved_plan' not in args:
        raise ModelError('invalid origin descriptor fields')
    approved_path = Path(args['approved_plan'])
    if approved_path.parent != stage or approved_path in (Path(args['plan']), destination):
        raise ModelError('origin approval must be a distinct direct staging child')
    approved = load_approved_plan(approved_path, limits)
    if approved.plan.limits != limits:
        raise ModelError('origin worker limits differ from approved limits')
    n = len(approved.plan.sources)
    if type(descriptor['parts']) is not list or len(descriptor['parts']) != n or len(request.paths) != n:
        raise ModelError('origin part/path inventory differs')
    parts = [replace(PartRecord.from_dict(part), local_path=Path(path))
             for part, path in zip(descriptor['parts'], request.paths, strict=True)]
    if [p.source_id for p in parts] != list(range(n)) or any(p.rows != s.num_rows for p, s in zip(parts, approved.plan.sources, strict=True)):
        raise ModelError('origin parts differ from source inventory')
    _check_part_inventory(parts, limits)
    import pyarrow.parquet as pq
    rows = 0
    for part in parts:
        with _open_part(part) as stream:
            reader = pq.ParquetFile(stream, arrow_extensions_enabled=False)
            _check_schema(reader.schema_arrow, approved.plan.proposal, part.source_id)
            if reader.metadata.num_rows != part.rows:
                _fail(part.source_id, 'inventory', (), 'origin part row count differs')
            first = 0
            for batch in reader.iter_batches(batch_size=limits.batch_rows, columns=['__pc_source', '__pc_row'], use_threads=False):
                for offset in range(batch.num_rows):
                    if batch.column(0)[offset].as_py() != part.source_id or batch.column(1)[offset].as_py() != first + offset:
                        _fail(part.source_id, first + offset, (), 'origin provenance coordinate differs')
                first += batch.num_rows
            if first != part.rows:
                _fail(part.source_id, 'inventory', (), 'origin scanned row count differs')
            rows += first
    counters = {'files': n, 'rows': rows}
    result = {'integrity_level': 'OUTPUT_HASHES_MATCH', 'counters': counters}
    _private_write(destination, canonical_json({'request_digest': digest_json(request.to_dict()), 'result': result}), stage, limits)
    return WorkerResult(1, 'verify', 'COMPLETE', tuple(sorted(counters.items())))


def lookup_origin(bundle: Path, source_id: int, row: int, source_root: Path | None = None) -> OriginRecord:
    """Retrieve source interpretation; source truth requires full fresh verify."""
    start = time.monotonic()
    try:
        try:
            approved, parts, historical, manifest = read_bundle(bundle, deadline=start + Limits().wall_seconds)
        except ModelError as exc:
            raise OriginCheckError(_exception_status(exc), str(exc)) from exc
        deadline = start + approved.plan.limits.wall_seconds
        _elapsed(deadline)
        source = _coordinates(approved, source_id, row)
        if source_root is None:
            _check_output_provenance(approved, parts)
            level, label = 'OUTPUT_HASHES_MATCH', 'HISTORICAL'
        else:
            report = verify_bundle(bundle, source_root, approved)
            if not report.verified:
                raise OriginCheckError(report.status, '; '.join(report.diagnostics) or report.status)
            level, label = 'VERIFIED', 'FRESH_SOURCE_BOUND'
        _elapsed(deadline)
        return OriginRecord(level, source, row, source.fields, source.schema_metadata, approved.resolutions, label)
    except OriginCheckError:
        raise
    except ModelError:
        raise
    except Exception as exc:
        raise OriginCheckError(_exception_status(exc), str(exc)) from exc


def render_origin(record: OriginRecord) -> str:
    """Readable byte previews accompany exact structured metadata."""
    from .decisions import _byte_preview as _preview
    lines = [f'Origin integrity: {record.integrity_level}',
             'Metadata provenance is preserved. Scientific semantic compatibility is not established.',
             'Historical verification: the stored bundle report is historical, including its saved VERIFIED status.',
             f'Source {record.source.source_id}: {record.source.relative_path}; SHA256 {record.source.sha256}; original row {record.row}']
    if record.integrity_level == 'OUTPUT_HASHES_MATCH':
        lines += ['Provenance: OUTPUT_ONLY_CHECKED', 'Source value correctness was not checked.']
    else:
        lines += ['Verification: FRESH_SOURCE_BOUND; the whole source/output inventory was checked freshly.']
    for key, value in record.original_schema_metadata:
        lines.append('Original schema metadata: ' + _preview(key) + ' = ' + _preview(value))
    def fields(records, path=()):
        for field in records:
            child = (*path, field.name)
            for key, value in field.metadata:
                lines.append('Original field ' + canonical_json(list(child)).decode() + ': ' + _preview(key) + ' = ' + _preview(value))
            if field.type.kind == 'struct': fields(field.type.fields, child)
    fields(record.original_fields)
    lines.append('Origin record: ' + canonical_json(record.to_dict()).decode())
    return '\n'.join(lines) + '\n'
