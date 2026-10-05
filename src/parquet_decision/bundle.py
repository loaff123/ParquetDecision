"""Complete source-bound bundles, then qualified exclusive directory visibility.

Worker RLIMITs cover copy/read/write/hash/verify; parent JSON/integrity checks are
bounded/streamed and elapsed-time checked, not covered by the worker AS/CPU limit.
Atomic no-replace visibility is not a power-loss durability or authenticity claim.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import shutil
import stat
import tempfile
import time

from .artifacts import (ArtifactConflictError, _destination, publish_directory_exclusive,
                        qualify_noreplace)
from .model import (ApprovedPlan, Limits, LimitError, ModelError, PartRecord, RuntimeInfo,
                    Snapshot, UnsupportedError, VerificationReport, WorkerRequest, WorkerResult,
                    canonical_json, digest_json, parse_json, schema_digest)
from .source import _directory, _open_relative, _staging_bytes, snapshot_sources
from .supervise import _diagnostic, run_worker
from .transform import _Budget, _rewrite
from .verify_values import _exception_status, _verify_streams

_METADATA = {'_decision.json', '_manifest.json', '_verification.json'}
# Shared qualifier holds b'source' + b'target' simultaneously. Its separate
# sibling probe is transient owned staging, not part of the published dataset.
_PUBLICATION_PROBE_BYTES = 12
_RESPONSE_KEYS = {'request_digest', 'mode', 'result', 'bundle_bytes', 'staging_bytes', 'metadata_sha256'}


def _failure(status, message, approved):
    return VerificationReport(status, approved.plan.plan_digest, approved.approval_digest,
                              diagnostics=(_diagnostic(message),))


def _elapsed(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise LimitError('apply total operation elapsed-time cap exceeded')


def _control_write(path, raw, stage, limits):
    if len(raw) > limits.max_plan_bytes: raise LimitError('control JSON exceeds approved plan byte cap')
    if _staging_bytes(stage, limits) + len(raw) > limits.max_staging_bytes:
        raise LimitError('control bytes exceed total owned staging cap')
    with path.open('xb') as stream:
        stream.write(raw); stream.flush()


def _read_json(path, limits, deadline=None):
    _elapsed(deadline)
    with _open_relative(_directory(path.parent), path.name) as fd:
        with os.fdopen(os.dup(fd), 'rb') as stream:
            raw = stream.read(limits.max_plan_bytes + 1)
    record = parse_json(raw, max_bytes=limits.max_plan_bytes)
    if canonical_json(record) != raw: raise ModelError('bundle/control JSON must be canonical')
    _elapsed(deadline)
    return record, raw


def _read_approved(path, limits, deadline=None):
    """Keep bounded descriptor reads and the established raw pre-adapter gate."""
    from .model import _precheck_approved_plan, validate_inventory
    record, raw = _read_json(path, limits, deadline)
    _precheck_approved_plan(record, limits)
    declared = Limits.from_dict(record['plan']['limits'])
    if len(raw) > declared.max_plan_bytes:
        raise LimitError('approved control JSON exceeds declared byte cap')
    try:
        approved = ApprovedPlan.from_dict(record)
    except RecursionError as exc:
        raise ModelError('record nesting exceeds alpha profile') from exc
    validate_inventory(approved.plan.sources, limits)
    return approved, raw


def _hash(path, cap, deadline=None):
    _elapsed(deadline)
    digest, size = hashlib.sha256(), 0
    from .source import _identity
    with _open_relative(_directory(path.parent), path.name) as fd:
        before = os.fstat(fd)
        while chunk := os.read(fd, 1024 * 1024):
            size += len(chunk)
            if size > cap: raise LimitError('bundle file bytes exceed approved cap')
            digest.update(chunk); _elapsed(deadline)
        if _identity(before) != _identity(os.fstat(fd)): raise ModelError('bundle file changed while hashing')
    return size, digest.hexdigest()


def _info(raw): return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def _report_bindings(approved, parts, report):
    if not report.verified: raise ModelError('bundle does not contain completed successful verification')
    sources, limits = approved.plan.sources, approved.plan.limits
    if (report.plan_digest != approved.plan.plan_digest or report.approval_digest != approved.approval_digest
            or report.source_digest != digest_json([s.to_dict() for s in sources])
            or report.output_digest != digest_json([p.to_dict() for p in parts])):
        raise ModelError('bundle verification/decision/part digest bindings differ')
    if len(parts) != len(sources) or [p.source_id for p in parts] != [s.source_id for s in sources]:
        raise ModelError('bundle parts differ from approved source inventory')
    target = schema_digest(approved.plan.proposal.fields, approved.plan.proposal.schema_metadata)
    if any(p.rows != s.num_rows or p.target_schema_digest != target for s, p in zip(sources, parts, strict=True)):
        raise ModelError('bundle part rows/target schema differ')
    counts = dict(report.counters)
    expected = dict(files=len(sources), rows=sum(s.num_rows for s in sources),
                    input_bytes=sum(s.size_bytes for s in sources), part_bytes=sum(p.bytes for p in parts))
    if set(counts) != set(expected) | {'values'} or any(counts[k] != v for k, v in expected.items()):
        raise ModelError('bundle verification counter bindings differ')
    from .model import _schema_stats
    fields = approved.plan.proposal.fields
    if not expected['rows'] * len(fields) <= counts['values'] <= expected['rows'] * _schema_stats(fields)[0]:
        raise ModelError('bundle values counter is outside row/schema bounds')
    if expected['part_bytes'] > limits.max_output_bytes: raise LimitError('bundle part bytes exceed cap')


def _manifest(approved, parts, report, decision_raw, verification_raw):
    """Manifest self-size is a decimal-length fixed point, not a self-hash."""
    _report_bindings(approved, parts, report)
    part_bytes = sum(p.bytes for p in parts)
    value = dict(format_version=1, status='COMPLETE', plan_digest=report.plan_digest,
                 approval_digest=report.approval_digest, source_digest=report.source_digest,
                 output_digest=report.output_digest,
                 target_schema_digest=schema_digest(approved.plan.proposal.fields, approved.plan.proposal.schema_metadata),
                 parts=[p.to_dict() for p in parts],
                 metadata={'_decision.json': _info(decision_raw), '_verification.json': _info(verification_raw)},
                 accounting=dict(part_bytes=part_bytes, decision_bytes=len(decision_raw),
                                 verification_bytes=len(verification_raw), manifest_bytes=0, bundle_bytes=0),
                 verification_label='HISTORICAL')
    for _ in range(16):
        value['manifest_digest'] = digest_json({k: v for k, v in value.items() if k != 'manifest_digest'})
        size = len(canonical_json(value))
        total = part_bytes + len(decision_raw) + len(verification_raw) + size
        if value['accounting']['manifest_bytes'] == size and value['accounting']['bundle_bytes'] == total:
            return value
        value['accounting'].update(manifest_bytes=size, bundle_bytes=total)
    raise ModelError('manifest byte-accounting fixed point failed')


def _write_manifest(approved, parts, report, dataset, budget):
    decision_raw, verification_raw = canonical_json(approved.to_dict()), canonical_json(report.to_dict())
    budget.write(dataset / '_decision.json', decision_raw, output=True)
    budget.write(dataset / '_verification.json', verification_raw, output=True)
    manifest = _manifest(approved, parts, report, decision_raw, verification_raw)
    budget.write(dataset / '_manifest.json', canonical_json(manifest), output=True)
    return manifest


def read_bundle(dataset: Path, *, deadline=None) -> tuple[ApprovedPlan, list[PartRecord], VerificationReport, dict]:
    """Strict output-only integrity check; returned verification is HISTORICAL.

    No source-value correctness or fresh VERIFIED label is established here.
    Streaming hashes and complete cross-bindings are suitable for origin/verify.
    """
    dataset = _directory(dataset)
    approved, raw_decision = _read_approved(dataset / '_decision.json', Limits(), deadline)
    if approved.plan.runtime != RuntimeInfo(): raise UnsupportedError('bundle runtime differs from qualified profile')
    limits = approved.plan.limits
    if len(raw_decision) > limits.max_plan_bytes: raise LimitError('decision JSON exceeds approved cap')
    verification, raw_verification = _read_json(dataset / '_verification.json', limits, deadline)
    report = VerificationReport.from_dict(verification)
    manifest, raw_manifest = _read_json(dataset / '_manifest.json', limits, deadline)
    if type(manifest) is not dict or type(manifest.get('parts')) is not list:
        raise ModelError('invalid bundle manifest')
    if len(manifest['parts']) != len(approved.plan.sources):
        raise ModelError('raw manifest part count differs from approved source inventory')
    parts = [replace(PartRecord.from_dict(p), local_path=dataset / p['filename']) for p in manifest['parts']]
    _report_bindings(approved, parts, report)
    expected = _manifest(approved, parts, report, raw_decision, raw_verification)
    if manifest != expected or raw_manifest != canonical_json(expected):
        raise ModelError('strict manifest schema/digest/accounting cross-bindings differ')
    wanted = {p.filename for p in parts} | _METADATA
    actual, total = set(), 0
    for path in dataset.iterdir():
        info = path.lstat()
        if path.name not in wanted or not stat.S_ISREG(info.st_mode):
            raise ModelError('bundle inventory has unknown/nonregular entries')
        actual.add(path.name); total += info.st_size
        if total > limits.max_output_bytes: raise LimitError('total bundle bytes exceed approved output cap')
    if actual != wanted or total != manifest['accounting']['bundle_bytes']:
        raise ModelError('bundle exact inventory/total bytes differ')
    for part in parts:
        if _hash(part.local_path, limits.max_output_bytes, deadline) != (part.bytes, part.sha256):
            raise ModelError('bundle part hash/byte count differs')
    _elapsed(deadline)
    return approved, parts, report, manifest


def _read_response(path, request, limits):
    envelope, _ = _read_json(path, limits)
    if type(envelope) is not dict or set(envelope) != _RESPONSE_KEYS:
        raise ModelError('invalid rewrite artifact envelope')
    mode = 'apply' if 'source_root' in dict(request.args) else 'rewrite'
    if envelope['request_digest'] != digest_json(request.to_dict()) or envelope['mode'] != mode:
        raise ModelError('rewrite artifact is not bound to this request/mode')
    for key in ('bundle_bytes', 'staging_bytes'):
        if type(envelope[key]) is not int or envelope[key] < 0:
            raise ModelError('invalid rewrite byte accounting')
    if envelope['bundle_bytes'] > limits.max_output_bytes or envelope['staging_bytes'] > limits.max_staging_bytes:
        raise LimitError('rewrite final bytes exceed approved caps')
    return envelope


def _validate_terminal(terminal, expected, limits):
    names = {'address_space_bytes', 'cpu_seconds', 'peak_rss_kib'}
    actual = dict(terminal.counters)
    if (terminal.protocol_version != 1 or terminal.operation != 'rewrite' or terminal.status != 'COMPLETE'
            or terminal.diagnostics or set(actual) != set(expected) | names
            or any(actual[k] != v for k, v in expected.items())
            or actual['address_space_bytes'] != limits.address_space_bytes
            or actual['cpu_seconds'] != limits.cpu_seconds):
        raise ModelError('rewrite terminal status/counter bindings differ')


def _write_response(request, mode, result, dataset, destination, budget):
    metadata = {} if mode == 'rewrite' else {name: _hash(dataset / name, budget.limits.max_plan_bytes)[1] for name in sorted(_METADATA)}
    envelope = dict(request_digest=digest_json(request.to_dict()), mode=mode, result=result,
                    bundle_bytes=budget.output_bytes, staging_bytes=budget.staging_bytes,
                    metadata_sha256=metadata)
    for _ in range(16):
        raw = canonical_json(envelope)
        measured = budget.staging_bytes + len(raw)
        if envelope['staging_bytes'] == measured:
            budget.write(destination, raw, output=False)
            return
        envelope['staging_bytes'] = measured
    raise ModelError('response staging-byte fixed point failed')


def _rewrite_worker(request, limits):
    """Fixed rewrite/apply handler; accepts no caller-selected functions or code."""
    from .supervise import _require_worker
    _require_worker()
    args = dict(request.args)
    common = {'approved_plan', 'staging', 'dataset', 'destination'}
    if set(args) not in (common, common | {'source_root'}): raise ModelError('invalid fixed rewrite worker args')
    stage = _directory(Path(args['staging']))
    dataset = _directory(Path(args['dataset']))
    approved_path, destination = Path(args['approved_plan']), Path(args['destination'])
    if (approved_path.parent != stage or destination.parent != stage or approved_path == destination
            or os.path.lexists(destination)):
        raise ModelError('rewrite controls must be distinct exclusive direct staging children')
    approved, _ = _read_approved(approved_path, limits)
    if approved.plan.limits != limits: raise ModelError('rewrite limits differ from approved plan')
    if approved.plan.runtime != RuntimeInfo(): raise UnsupportedError('rewrite runtime differs from qualified profile')
    if 'source_root' in args:
        mode = 'apply'
        if request.paths or dataset.parent != stage or not (stage / 'INCOMPLETE').is_file():
            raise ModelError('apply requires outer marker/controls and inner dataset')
        try:
            snapshots = snapshot_sources(approved, Path(args['source_root']), stage)
        except ModelError as exc:
            if isinstance(exc, (LimitError, UnsupportedError)): raise
            return WorkerResult(1, 'rewrite', 'CONFLICT', diagnostics=(_diagnostic(exc),))
    else:
        mode = 'rewrite'
        if len(request.paths) != len(approved.plan.sources): raise ModelError('rewrite snapshot/path inventory differs')
        snapshots = [Snapshot(source, Path(path)) for source, path in zip(approved.plan.sources, request.paths, strict=True)]
    budget = _Budget(limits, stage, dataset, reserve_bytes=_PUBLICATION_PROBE_BYTES if mode == 'apply' else 0)
    parts = _rewrite(approved, snapshots, dataset, owned=stage, budget=budget)
    if mode == 'apply':
        report = _verify_streams(approved, snapshots, parts)
        if not report.verified: return WorkerResult(1, 'rewrite', report.status, report.counters, report.diagnostics)
        _write_manifest(approved, parts, report, dataset, budget)
        checked, final_parts, historical, manifest = read_bundle(dataset)
        if checked != approved or final_parts != parts or historical != report:
            raise ModelError('completed worker bundle cross-bindings differ')
        result = report.to_dict()
        counters = dict(report.counters)
    else:
        result = [p.to_dict() for p in parts]
        counters = dict(files=len(parts), rows=sum(p.rows for p in parts),
                        input_bytes=sum(s.source.size_bytes for s in snapshots), part_bytes=sum(p.bytes for p in parts))
    _write_response(request, mode, result, dataset, destination, budget)
    actual = _staging_bytes(stage, limits) + (0 if dataset.is_relative_to(stage) else _staging_bytes(dataset, limits))
    if actual != budget.staging_bytes or _staging_bytes(dataset, limits) != budget.output_bytes:
        raise ModelError('final rewrite output/staging accounting differs')
    counters.update(bundle_bytes=budget.output_bytes, staging_bytes=budget.staging_bytes)
    return WorkerResult(1, 'rewrite', 'COMPLETE', tuple(sorted(counters.items())))


def apply(approved: ApprovedPlan, source_root: Path, destination: Path) -> VerificationReport:
    """Publish only a fully verified bundle after strict worker terminal success."""
    if not isinstance(approved, ApprovedPlan): raise ModelError('apply requires a strict ApprovedPlan')
    stage = None
    dataset = None
    published = False
    start = time.monotonic()
    deadline = start + approved.plan.limits.wall_seconds
    try:
        # Reconstruct strict immutable bindings and admissibility before work.
        approved = ApprovedPlan.from_dict(approved.to_dict())
        limits = approved.plan.limits
        if approved.plan.runtime != RuntimeInfo(): raise UnsupportedError('apply runtime differs from qualified profile')
        root = _directory(source_root)
        destination = _destination(destination, [root])
        parent = _directory(destination.parent)
        if parent == root or parent.is_relative_to(root):
            raise ArtifactConflictError('destination parent/owned staging must be outside source root')
        if limits.max_staging_bytes < _PUBLICATION_PROBE_BYTES:
            raise LimitError('no-replace qualification probe bytes exceed staging cap')
        qualify_noreplace(parent)
        _elapsed(deadline)
        stage = Path(tempfile.mkdtemp(prefix='.pdecision-apply-', dir=parent))
        _control_write(stage / 'INCOMPLETE', b'INCOMPLETE\n', stage, limits)
        _control_write(stage / 'approved.json', canonical_json(approved.to_dict()), stage, limits)
        dataset = stage / 'dataset'; dataset.mkdir()
        request = WorkerRequest(1, 'rewrite', args=(('source_root', str(root)), ('staging', str(stage)),
                                ('approved_plan', str(stage / 'approved.json')), ('dataset', str(dataset)),
                                ('destination', str(stage / 'result.json'))))
        terminal = run_worker(request, limits)
        _elapsed(deadline)
        if terminal.status != 'COMPLETE':
            # Known late failures always override any earlier success artifact.
            return _cleanup_failure(stage, terminal.status, '; '.join(terminal.diagnostics) or terminal.status, approved)
        envelope = _read_response(stage / 'result.json', request, limits)
        report = VerificationReport.from_dict(envelope['result'])
        checked, parts, historical, manifest = read_bundle(dataset, deadline=deadline)
        if checked != approved or report != historical:
            raise ModelError('rewrite result/complete bundle bindings differ')
        actual_metadata = {name: _hash(dataset / name, limits.max_plan_bytes, deadline)[1] for name in sorted(_METADATA)}
        if envelope['metadata_sha256'] != actual_metadata: raise ModelError('rewrite metadata byte hashes differ')
        counts = dict(report.counters)
        counts.update(bundle_bytes=manifest['accounting']['bundle_bytes'], staging_bytes=_staging_bytes(stage, limits))
        if (envelope['bundle_bytes'], envelope['staging_bytes']) != (counts['bundle_bytes'], counts['staging_bytes']):
            raise ModelError('rewrite final artifact byte accounting differs')
        _validate_terminal(terminal, counts, limits)
        if counts['staging_bytes'] + _PUBLICATION_PROBE_BYTES > limits.max_staging_bytes:
            raise LimitError('final staging plus publication qualification probe exceeds cap')
        _elapsed(deadline)
        publish_directory_exclusive(dataset, destination, [root])
        published = True
        # Keep outer INCOMPLETE until the owned controls/snapshots are removed.
        shutil.rmtree(stage); stage = None
        _elapsed(deadline)
        return report
    except KeyboardInterrupt:
        status, message = 'CANCELLED', 'apply cancelled by keyboard interruption'
    except Exception as exc:
        status = ('ERROR' if isinstance(exc, ModelError) and not isinstance(exc, (ArtifactConflictError, LimitError, UnsupportedError)) else _exception_status(exc))
        message = f'{type(exc).__name__}: {exc}'
    # Rename may have committed before a reporting/cleanup exception was raised.
    # Never remove destination or claim an existing destination can be retried.
    if published or (dataset is not None and not dataset.exists() and destination.exists()):
        message += f'; bundle may be committed at {destination}; inspect it before retry, destination will never be overwritten'
    if stage is not None:
        return _cleanup_failure(stage, status, message, approved)
    return _failure(status, message, approved)


def _cleanup_failure(stage, status, message, approved):
    try:
        shutil.rmtree(stage)
    except Exception as exc:
        status = _exception_status(exc)
        if stage.is_dir() and not os.path.lexists(stage / 'INCOMPLETE'):
            try:
                _control_write(stage / 'INCOMPLETE', b'INCOMPLETE\n', stage, approved.plan.limits)
            except Exception as marker_exc:
                message += f'; INCOMPLETE marker recreation failed: {type(marker_exc).__name__}: {marker_exc}'
        message += f'; operation-owned INCOMPLETE staging retained at {stage}; cleanup failed: {type(exc).__name__}: {exc}'
    return _failure(status, message, approved)
