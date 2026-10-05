"""Fresh, source-bound, independently derived streaming verification.

No forward alignment/cast routine or planner conclusion is an oracle here.
Decimal coefficients and timestamp ticks use integer arithmetic. Batch pairs are
zipped without a whole-file table, and all target fields are visited.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import math
import os
from pathlib import Path
import tempfile

from .model import (ApprovedPlan, Limits, LimitError, ModelError, PartRecord,
                    RuntimeInfo, SchemaProposal, Snapshot, SourceSpec, UnsupportedError,
                    VerificationReport, WorkerRequest, WorkerResult, canonical_json,
                    digest_json, load_approved_plan, parse_json, schema_digest,
                    validate_inventory)


class _Conflict(ModelError):
    pass


def _fail(source_id, row, path, message):
    location = canonical_json(list(path)).decode('utf-8')
    raise _Conflict(f'source={source_id} row={row} path={location} {message}')


def _failure(status, diagnostic, approved=None):
    from .supervise import _diagnostic
    return VerificationReport(status, plan_digest=approved.plan.plan_digest if approved else '',
                              approval_digest=approved.approval_digest if approved else '',
                              diagnostics=(_diagnostic(diagnostic),))


def _exception_status(exc):
    from .supervise import _os_error_status
    if isinstance(exc, UnsupportedError): return 'UNSUPPORTED'
    if isinstance(exc, (LimitError, MemoryError)): return 'LIMIT_EXCEEDED'
    if isinstance(exc, FileNotFoundError): return 'INCOMPLETE'
    if isinstance(exc, OSError): return _os_error_status(exc)
    if isinstance(exc, ModelError): return 'CONFLICT'
    # ArrowMemoryError is handled at the fixed worker entry boundary too.
    import sys
    pa = sys.modules.get('pyarrow')
    if pa is not None and isinstance(exc, pa.ArrowMemoryError): return 'LIMIT_EXCEEDED'
    return 'ERROR'


def _batch_size(value, limits):
    if type(value) is not int or not 1 <= value <= limits.batch_rows:
        raise ModelError('verification batch size exceeds the approved batch-row cap')
    return value


def verify_streams(approved: ApprovedPlan, snapshots: list[Snapshot], parts: list[PartRecord],
                   *, source_batch_rows: int | None = None,
                   output_batch_rows: int | None = None) -> VerificationReport:
    """Check complete snapshot/part streams inside a fresh supervised worker.

    Optional independent read sizes only lower the approved batch-row cap; they
    do not change logical verification, order or its counts. No output-only mode
    of this function can report VERIFIED.
    """
    if not isinstance(approved, ApprovedPlan):
        raise ModelError('verification requires a strict ApprovedPlan')
    try:
        _validate_inputs(approved, snapshots, parts)
        limits = approved.plan.limits
        source_rows = _batch_size(limits.batch_rows if source_batch_rows is None else source_batch_rows, limits)
        output_rows = _batch_size(limits.batch_rows if output_batch_rows is None else output_batch_rows, limits)
        result, artifact = _invoke('verify', snapshots, parts, limits, approved=approved,
                                   source_batch_rows=source_rows, output_batch_rows=output_rows)
        if result.status != 'COMPLETE':
            return _failure(result.status, '; '.join(result.diagnostics) or result.status, approved)
        report = VerificationReport.from_dict(artifact)
        if (not report.verified or report.plan_digest != approved.plan.plan_digest
                or report.approval_digest != approved.approval_digest
                or report.source_digest != digest_json([s.source.to_dict() for s in snapshots])
                or report.output_digest != digest_json([p.to_dict() for p in parts])):
            return _failure('ERROR', 'worker verification artifact binding differs', approved)
        return report
    except Exception as exc:
        return _failure(_exception_status(exc), exc, approved)


def _validate_inputs(approved, snapshots, parts):
    if approved.plan.runtime != RuntimeInfo():
        raise UnsupportedError('approved runtime differs from qualified verifier')
    if type(snapshots) is not list or type(parts) is not list:
        raise ModelError('verification needs explicit ordered snapshot and part lists')
    if not snapshots:
        raise FileNotFoundError('fresh source-bound verification requires source snapshots')
    if any(not isinstance(s, Snapshot) for s in snapshots) or any(not isinstance(p, PartRecord) for p in parts):
        raise ModelError('verification inventory has invalid record types')
    if tuple(s.source for s in snapshots) != approved.plan.sources:
        raise _Conflict('snapshot source inventory differs from the approved ordered inventory')
    if len(parts) != len(snapshots) or [p.source_id for p in parts] != [s.source.source_id for s in snapshots]:
        raise _Conflict('part inventory differs from the exact ordered source inventory')
    if any(s.local_path is None for s in snapshots):
        raise FileNotFoundError('source snapshot local path is unavailable')
    if any(p.local_path is None for p in parts):
        raise FileNotFoundError('output part local path is unavailable')
    if sum(p.bytes for p in parts) > approved.plan.limits.max_output_bytes:
        raise LimitError('part bytes exceed the approved output-byte cap')
    for snapshot, part in zip(snapshots, parts, strict=True):
        if part.rows != snapshot.source.num_rows:
            raise _Conflict(f'source={part.source_id} part row count differs from source inventory')


def _invoke(mode, snapshots, parts, limits, *, approved=None, source_batch_rows=None, output_batch_rows=None):
    """Native-free bounded transport; private records never duplicate source schemas."""
    from .supervise import run_worker, _diagnostic, _os_error_status
    try:
        validate_inventory(tuple(s.source for s in snapshots), limits)
        if any(s.local_path is None for s in snapshots) or any(p.local_path is None for p in parts):
            raise FileNotFoundError('local verification inputs are unavailable')
        with tempfile.TemporaryDirectory(prefix='parquet-decision-verify-') as directory:
            stage = Path(directory)
            descriptor = {'mode': mode}
            if mode == 'derive':
                descriptor['sources'] = [s.source.to_dict() for s in snapshots]
            elif mode == 'verify':
                descriptor.update(parts=[p.to_dict() for p in parts],
                                  source_batch_rows=source_batch_rows, output_batch_rows=output_batch_rows)
            else:
                raise ModelError('unknown private verifier operation')
            args = [('plan', str(stage / 'request.json')), ('staging', str(stage)), ('destination', str(stage / 'result.json'))]
            paths = tuple(str(s.local_path.absolute()) for s in snapshots) + tuple(str(p.local_path.absolute()) for p in parts)
            if approved is not None:
                raw = canonical_json(approved.to_dict())
                _private_write(stage / 'approved.json', raw, stage, limits)
                args.append(('approved_plan', str(stage / 'approved.json')))
            _private_write(stage / 'request.json', canonical_json(descriptor), stage, limits)
            request = WorkerRequest(1, 'verify', paths, tuple(args))
            result = run_worker(request, limits)
            # Never inspect or promote a leftover artifact after terminal failure.
            if result.status != 'COMPLETE':
                return result, None
            try:
                envelope = _read_private(stage / 'result.json', limits)
                if type(envelope) is not dict or set(envelope) != {'request_digest', 'result'}:
                    raise ModelError('invalid verifier artifact envelope')
                if envelope['request_digest'] != digest_json(request.to_dict()):
                    raise ModelError('verifier artifact belongs to another request')
                if mode == 'verify':
                    report = VerificationReport.from_dict(envelope['result'])
                    expected_counters = report.counters
                    report_counts = dict(expected_counters)
                    if set(report_counts) != {'files', 'rows', 'input_bytes', 'part_bytes', 'values'}:
                        raise ModelError('verification report counter names differ from the exact contract')
                    fixed_counts = dict(_source_counters(snapshots))
                    fixed_counts['part_bytes'] = sum(p.bytes for p in parts)
                    if any(report_counts[k] != v for k, v in fixed_counts.items()):
                        raise ModelError('verification counters do not reconcile input inventories')
                    # The parent can establish schema/row-derived bounds only.
                    # Child observations below null parents are data-dependent.
                    from .model import _schema_stats
                    fields = approved.plan.proposal.fields
                    minimum = fixed_counts['rows'] * len(fields)
                    maximum = fixed_counts['rows'] * _schema_stats(fields)[0]
                    if not minimum <= report_counts['values'] <= maximum:
                        raise ModelError('verification values counter is outside row/schema bounds')
                    if not report.verified:
                        raise ModelError('COMPLETE terminal state lacks verified artifact')
                else:
                    SchemaProposal.from_dict(envelope['result'])
                    expected_counters = _source_counters(snapshots)
                supervision_names = {'address_space_bytes', 'cpu_seconds', 'peak_rss_kib'}
                terminal_counts = dict(result.counters)
                if set(terminal_counts) != {key for key, _ in expected_counters} | supervision_names:
                    raise ModelError('worker terminal counter names differ from the exact contract')
                if (terminal_counts['address_space_bytes'] != limits.address_space_bytes
                        or terminal_counts['cpu_seconds'] != limits.cpu_seconds):
                    raise ModelError('worker supervision counters differ from requested limits')
                observed = tuple((k, v) for k, v in result.counters if k not in supervision_names)
                if observed != expected_counters:
                    raise ModelError('worker/artifact counters differ')
                artifact = envelope['result']
            except MemoryError as exc:
                return WorkerResult(1, 'verify', 'LIMIT_EXCEEDED', diagnostics=(_diagnostic(f'parent verification artifact allocation failed: {exc}'),)), None
            except OSError as exc:
                return WorkerResult(1, 'verify', _os_error_status(exc), diagnostics=(_diagnostic(f'parent verification artifact read failed: {exc}'),)), None
            except Exception as exc:
                return WorkerResult(1, 'verify', 'ERROR', diagnostics=(_diagnostic(f'invalid verification artifact: {exc}'),)), None
        # Reaching here means temporary-directory cleanup also succeeded.
        return result, artifact
    except Exception as exc:
        return WorkerResult(1, 'verify', _exception_status(exc), diagnostics=(_diagnostic(f'verification transport failed: {exc}'),)), None


def _private_write(path, raw, stage, limits):
    from .source import _staging_bytes
    if len(raw) > limits.max_plan_bytes:
        raise LimitError('private verification JSON exceeds approved plan-byte cap')
    if _staging_bytes(stage, limits) + len(raw) > limits.max_staging_bytes:
        raise LimitError('private verification files exceed approved staging-byte cap')
    with path.open('xb') as output:
        output.write(raw)
        output.flush()


def _read_private(path, limits):
    with path.open('rb') as stream:
        return parse_json(stream.read(limits.max_plan_bytes + 1), max_bytes=limits.max_plan_bytes)


def _source_counters(snapshots):
    return tuple(sorted((('files', len(snapshots)), ('rows', sum(s.source.num_rows for s in snapshots)),
                         ('input_bytes', sum(s.source.size_bytes for s in snapshots)))))


def _verify_worker(request, limits):
    """Only fixed verify operation: strict descriptors, no caller-selected code."""
    from .source import _directory
    from .supervise import _require_worker
    from .verify_schema import _derive_expected_schema
    _require_worker()
    args = dict(request.args)
    if set(args) not in ({'plan', 'staging', 'destination'}, {'plan', 'staging', 'destination', 'approved_plan'}):
        raise ModelError('invalid fixed verify worker args')
    stage = _directory(Path(args['staging']))
    descriptor_path, destination = Path(args['plan']), Path(args['destination'])
    if descriptor_path.parent != stage or destination.parent != stage or descriptor_path == destination:
        raise ModelError('verification descriptor/results must be distinct direct children of owned staging')
    descriptor = _read_private(descriptor_path, limits)
    if type(descriptor) is not dict:
        raise ModelError('private verifier descriptor must be an object')
    mode = descriptor.get('mode')
    if mode == 'origin':
        from .origin import _origin_worker
        try:
            return _origin_worker(request, limits, descriptor, stage, destination)
        except _Conflict as exc:
            from .supervise import _diagnostic
            return WorkerResult(1, 'verify', 'CONFLICT', diagnostics=(_diagnostic(exc),))
    if mode == 'derive':
        if set(descriptor) != {'mode', 'sources'} or 'approved_plan' in args or type(descriptor['sources']) is not list:
            raise ModelError('invalid derive descriptor fields')
        if not 1 <= len(descriptor['sources']) <= limits.max_files or len(request.paths) != len(descriptor['sources']):
            raise ModelError('derive source/path inventory differs')
        sources = tuple(SourceSpec.from_dict(s) for s in descriptor['sources'])
        validate_inventory(sources, limits)
        snapshots = [Snapshot(source, Path(path)) for source, path in zip(sources, request.paths, strict=True)]
        result = _derive_expected_schema(snapshots, limits).to_dict()
        counters = _source_counters(snapshots)
    elif mode == 'verify':
        if set(descriptor) != {'mode', 'parts', 'source_batch_rows', 'output_batch_rows'} or 'approved_plan' not in args:
            raise ModelError('invalid verify descriptor fields')
        approved_path = Path(args['approved_plan'])
        if approved_path.parent != stage or approved_path in (descriptor_path, destination):
            raise ModelError('approved record must be a distinct direct staging child')
        approved = load_approved_plan(approved_path, limits)
        if approved.plan.limits != limits:
            raise ModelError('worker limits differ from approved limits')
        n = len(approved.plan.sources)
        if type(descriptor['parts']) is not list or len(descriptor['parts']) != n or len(request.paths) != 2 * n:
            raise ModelError('verify source/part/path inventory differs')
        snapshots = [Snapshot(source, Path(path)) for source, path in zip(approved.plan.sources, request.paths[:n], strict=True)]
        parts = [replace(PartRecord.from_dict(part), local_path=Path(path))
                 for part, path in zip(descriptor['parts'], request.paths[n:], strict=True)]
        report = _verify_streams(approved, snapshots, parts,
                                 source_batch_rows=_batch_size(descriptor['source_batch_rows'], limits),
                                 output_batch_rows=_batch_size(descriptor['output_batch_rows'], limits))
        if not report.verified:
            return WorkerResult(1, 'verify', report.status, report.counters, report.diagnostics)
        result, counters = report.to_dict(), report.counters
    else:
        raise ModelError('unknown fixed verifier descriptor mode')
    raw = canonical_json({'request_digest': digest_json(request.to_dict()), 'result': result})
    _private_write(destination, raw, stage, limits)
    return WorkerResult(1, 'verify', 'COMPLETE', counters)


@contextmanager
def _open_part(part):
    from .source import _directory, _open_relative, _identity
    path = part.local_path
    _directory(path.parent)
    if path.name != part.filename:
        raise _Conflict(f'source={part.source_id} part local filename differs from inventory')
    with _open_relative(path.parent, path.name) as fd:
        before = os.fstat(fd)
        digest, size = hashlib.sha256(), 0
        while chunk := os.read(fd, 1024 * 1024):
            size += len(chunk)
            if size > part.bytes:
                raise _Conflict(f'source={part.source_id} part byte count differs')
            digest.update(chunk)
        if (size, digest.hexdigest()) != (part.bytes, part.sha256):
            raise _Conflict(f'source={part.source_id} part hash/byte count differs')
        if _identity(before) != _identity(os.fstat(fd)):
            raise _Conflict(f'source={part.source_id} output changed while hashing')
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(fd), 'rb') as stream:
            yield stream
        if _identity(before) != _identity(os.fstat(fd)):
            raise _Conflict(f'source={part.source_id} output changed during reading')


def _check_part_inventory(parts, limits=None):
    from .source import _directory
    roots = {_directory(p.local_path.parent) for p in parts}
    if len(roots) != 1:
        raise _Conflict('all output parts must belong to one dataset directory')
    root = next(iter(roots))
    import stat
    allowed_metadata = {'_decision.json', '_manifest.json', '_verification.json'}
    expected = {p.filename for p in parts}
    names = set()
    total_bytes = 0
    for path in root.iterdir():
        if path.name not in expected | allowed_metadata or not stat.S_ISREG(path.lstat().st_mode):
            raise _Conflict('actual output part inventory differs from the complete recorded inventory')
        total_bytes += path.lstat().st_size
        if limits is not None and total_bytes > limits.max_output_bytes:
            raise LimitError('total dataset bytes exceed the approved output-byte cap')
        names.add(path.name)
    if names - allowed_metadata != expected:
        raise _Conflict('actual output part inventory differs from the complete recorded inventory')


def _check_schema(schema, expected, source_id):
    from .model import _fields_from_arrow
    import pyarrow as pa
    names = [f.name for f in expected.fields] + ['__pc_source', '__pc_row']
    if schema.names != names:
        _fail(source_id, 'schema', (), 'output schema field order/names differ')
    for name, typ in (('__pc_source', pa.uint32()), ('__pc_row', pa.uint64())):
        actual = schema.field(name)
        if actual.type != typ or actual.nullable or actual.metadata:
            _fail(source_id, 'schema', (name,), 'provenance schema differs')
    fields, metadata = _fields_from_arrow(pa.schema(list(schema)[:-2], metadata=schema.metadata))
    if metadata != expected.schema_metadata:
        _fail(source_id, 'schema', (), 'output schema metadata differs')
    _check_fields(fields, expected.fields, source_id, ())


def _check_fields(actual, expected, source_id, path):
    if [f.name for f in actual] != [f.name for f in expected]:
        _fail(source_id, 'schema', path, 'output schema child order/names differ')
    for observed, wanted in zip(actual, expected, strict=True):
        child_path = (*path, wanted.name)
        if observed.nullable != wanted.nullable or observed.metadata != wanted.metadata:
            _fail(source_id, 'schema', child_path, 'output schema nullability/metadata differs')
        if observed.type.kind == wanted.type.kind == 'struct':
            _check_fields(observed.type.fields, wanted.type.fields, source_id, child_path)
        elif observed.type != wanted.type:
            _fail(source_id, 'schema', child_path, 'output schema type differs')


def _zip_batches(source_batches, output_batches):
    """Only current input/output batch retained; exhaustion is reconciled by caller."""
    source_iter, output_iter = iter(source_batches), iter(output_batches)
    source = output = None
    si = oi = 0
    while True:
        if source is None or si == source.num_rows:
            source = next(source_iter, None); si = 0
            while source is not None and source.num_rows == 0:
                source = next(source_iter, None)
        if output is None or oi == output.num_rows:
            output = next(output_iter, None); oi = 0
            while output is not None and output.num_rows == 0:
                output = next(output_iter, None)
        if source is None or output is None:
            if source is not None or output is not None:
                raise _Conflict('source/output stream lengths differ')
            return
        take = min(source.num_rows - si, output.num_rows - oi)
        yield source, si, output, oi, take
        si += take; oi += take


def _coefficient(value, scale):
    sign, digits, exponent = value.as_tuple()
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    shift = exponent + scale
    if shift < 0:
        coefficient, remainder = divmod(coefficient, 10 ** -shift)
        if remainder:
            raise _Conflict('decimal observation cannot be represented at its declared scale')
    else:
        coefficient *= 10 ** shift
    return -coefficient if sign else coefficient


def _compare_value(source_type, target_type, source, output, source_id, row, path):
    # Null parent children are not observations. In particular no Python dict
    # conversion, safe cast, or arithmetic visits hidden children of that parent.
    want_null = source is None or source_type is None or source_type.kind == 'null' or not source.is_valid
    if want_null:
        if output.is_valid:
            _fail(source_id, row, path, 'expected null observation')
        return 1
    if not output.is_valid:
        _fail(source_id, row, path, 'unexpected null observation')
    if target_type.kind == 'struct':
        source_fields = {f.name: f for f in source_type.fields}
        checks = 1
        for field in target_type.fields:
            original = source_fields.get(field.name)
            checks += _compare_value(None if original is None else original.type, field.type,
                                     None if original is None else source[field.name], output[field.name],
                                     source_id, row, (*path, field.name))
        return checks
    if target_type.kind == 'timestamp':
        powers = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
        expected = source.value * 10 ** (powers[target_type.unit] - powers[source_type.unit])
        if not -(1 << 63) <= expected < (1 << 63):
            _fail(source_id, row, path, 'expected timestamp ticks overflow int64')
        equal = expected == output.value
    elif target_type.kind.startswith('decimal'):
        if source_type.kind in ('int', 'uint'):
            expected = source.as_py() * 10 ** target_type.scale
        else:
            expected = _coefficient(source.as_py(), source_type.scale) * 10 ** (target_type.scale - source_type.scale)
        equal = expected == _coefficient(output.as_py(), target_type.scale)
    elif target_type.kind == 'float':
        left, right = source.as_py(), output.as_py()
        equal = (math.isnan(left) and math.isnan(right)) or (left == right and (
            left != 0.0 or math.copysign(1.0, left) == math.copysign(1.0, right)))
    else:
        equal = source.as_py() == output.as_py()
    if not equal:
        _fail(source_id, row, path, 'logical value differs')
    return 1


def _verify_streams(approved, snapshots, parts, *, source_batch_rows=None, output_batch_rows=None):
    """Worker-local full verification, for use under the apply operation watchdog."""
    from .supervise import _require_worker
    from .source import iter_source_batches
    from .verify_schema import _derive_expected_schema
    _require_worker()
    try:
        _validate_inputs(approved, snapshots, parts)
        limits = approved.plan.limits
        source_rows = _batch_size(limits.batch_rows if source_batch_rows is None else source_batch_rows, limits)
        output_rows = _batch_size(limits.batch_rows if output_batch_rows is None else output_batch_rows, limits)
        expected = _derive_expected_schema(snapshots, limits)
        if expected != approved.plan.proposal:
            raise _Conflict('independently derived schema/operations/conflicts differ from approved proposal')
        _check_part_inventory(parts, limits)
        target_digest = schema_digest(expected.fields, expected.schema_metadata)
        rows = values = 0
        import pyarrow.parquet as pq
        for snapshot, part in zip(snapshots, parts, strict=True):
            source_id = snapshot.source.source_id
            if part.target_schema_digest != target_digest:
                _fail(source_id, 'schema', (), 'part target-schema digest differs')
            with _open_part(part) as stream:
                reader = pq.ParquetFile(stream, arrow_extensions_enabled=False, pre_buffer=False)
                _check_schema(reader.schema_arrow, expected, source_id)
                if reader.metadata.num_rows != part.rows:
                    _fail(source_id, 'inventory', (), 'actual output row count differs')
                source_fields = {f.name: (i, f) for i, f in enumerate(snapshot.source.fields)}
                source_iter = (b.record_batch for b in iter_source_batches(snapshot, source_rows))
                output_iter = reader.iter_batches(batch_size=output_rows, use_threads=False)
                row = 0
                try:
                    for source_batch, si, output_batch, oi, length in _zip_batches(source_iter, output_iter):
                        for offset in range(length):
                            sr, out = si + offset, oi + offset
                            for index, name, expected_value in ((len(expected.fields), '__pc_source', source_id),
                                                                 (len(expected.fields) + 1, '__pc_row', row)):
                                scalar = output_batch.column(index)[out]
                                if not scalar.is_valid or scalar.as_py() != expected_value:
                                    _fail(source_id, row, (name,), 'provenance value differs')
                            for index, field in enumerate(expected.fields):
                                original = source_fields.get(field.name)
                                values += _compare_value(None if original is None else original[1].type, field.type,
                                    None if original is None else source_batch.column(original[0])[sr],
                                    output_batch.column(index)[out], source_id, row, (field.name,))
                            row += 1
                    if row != snapshot.source.num_rows:
                        _fail(source_id, row, (), 'streamed row count differs')
                finally:
                    source_iter.close()
                    output_iter.close()
                rows += row
        counters = tuple(sorted((*_source_counters(snapshots), ('part_bytes', sum(p.bytes for p in parts)), ('values', values))))
        if rows != dict(counters)['rows']:
            raise _Conflict('total streamed rows differ from source inventory')
        return VerificationReport('VERIFIED', approved.plan.plan_digest, approved.approval_digest,
                                  digest_json([s.source.to_dict() for s in snapshots]),
                                  digest_json([p.to_dict() for p in parts]), counters,
                                  checked_at_utc=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))
    except Exception as exc:
        return _failure(_exception_status(exc), exc, approved)
