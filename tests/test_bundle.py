from dataclasses import replace
import hashlib
import json
import pyarrow.parquet as pq
import pytest
from parquet_decision.model import Limits, VerificationReport, WorkerResult, canonical_json
from tests.verify_fixtures import simple_case, union_case
from tests.test_transform import module


def test_only_verified_bundle_publishes(tmp_path):
    approved, snapshots, _, data = union_case(tmp_path)
    destination = tmp_path / 'result'
    report = module('bundle').apply(approved, snapshots[0].local_path.parent, destination)
    assert report.verified, report
    assert set(p.name for p in destination.iterdir()) == {'part-00000.parquet', 'part-00001.parquet', '_decision.json', '_manifest.json', '_verification.json'}
    decision, parts, historical, manifest = module('bundle').read_bundle(destination)
    assert decision == approved and historical == report
    assert dict(historical.counters)['part_bytes'] == sum(p.bytes for p in parts)
    accounting = manifest['accounting']
    assert accounting['bundle_bytes'] == sum(p.stat().st_size for p in destination.iterdir())
    assert accounting['part_bytes'] == sum(p.bytes for p in parts)
    assert manifest['verification_label'] == 'HISTORICAL'
    assert json.loads((destination / '_decision.json').read_text()) == approved.to_dict()
    assert not list(tmp_path.glob('.pdecision-apply-*'))
    assert pq.read_table(destination).num_rows == 4


@pytest.mark.parametrize('name', ['_decision.json', '_manifest.json', '_verification.json'])
def test_metadata_cross_bindings_refuse_edits(tmp_path, name):
    approved, snapshots, _, _ = simple_case(tmp_path)
    destination = tmp_path / 'result'
    assert module('bundle').apply(approved, snapshots[0].local_path.parent, destination).verified
    record = json.loads((destination / name).read_text())
    if name == '_decision.json': record['approval_digest'] = 'f' * 64
    elif name == '_manifest.json': record['output_digest'] = 'f' * 64
    else: record['source_digest'] = 'f' * 64
    (destination / name).write_bytes(canonical_json(record))
    with pytest.raises(Exception): module('bundle').read_bundle(destination)


@pytest.mark.parametrize('failure', ['ERROR', 'LIMIT_EXCEEDED', 'CANCELLED', 'INCOMPLETE'])
def test_terminal_failure_never_promotes_old_success_artifact(tmp_path, monkeypatch, failure):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle')
    original = bundle.run_worker
    def late_failure(request, limits):
        result = original(request, limits)
        assert result.status == 'COMPLETE', result
        return WorkerResult(1, 'rewrite', failure, diagnostics=('original controlled late failure',))
    monkeypatch.setattr(bundle, 'run_worker', late_failure)
    destination = tmp_path / 'result'
    report = bundle.apply(approved, snapshots[0].local_path.parent, destination)
    assert report.status == failure and not destination.exists()
    assert not list(tmp_path.glob('.pdecision-apply-*'))


@pytest.mark.parametrize('counter', ['files', 'rows', 'input_bytes', 'part_bytes', 'values', 'bundle_bytes', 'staging_bytes', 'address_space_bytes', 'cpu_seconds'])
def test_terminal_counter_bindings(tmp_path, monkeypatch, counter):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); original = bundle.run_worker
    def inconsistent(request, limits):
        result = original(request, limits)
        counts = dict(result.counters); counts[counter] += 1
        return replace(result, counters=tuple(sorted(counts.items())))
    monkeypatch.setattr(bundle, 'run_worker', inconsistent)
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'ERROR' and not (tmp_path / 'result').exists()


@pytest.mark.parametrize('kind', ['output', 'staging'])
def test_bundle_metadata_bytes_count_toward_limits(tmp_path, kind):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); root = snapshots[0].local_path.parent
    assert bundle.apply(approved, root, tmp_path / 'first').verified
    _, parts, _, manifest = bundle.read_bundle(tmp_path / 'first')
    caps = {'max_output_bytes': sum(p.bytes for p in parts)} if kind == 'output' else {'max_staging_bytes': manifest['accounting']['bundle_bytes']}
    approved = replace(approved, plan=replace(approved.plan, limits=replace(Limits(), **caps), plan_digest=''), approval_digest='')
    report = bundle.apply(approved, root, tmp_path / 'second')
    assert report.status == 'LIMIT_EXCEEDED' and not (tmp_path / 'second').exists()


def test_overlap_and_changed_source_refuse(tmp_path):
    approved, snapshots, _, _ = simple_case(tmp_path)
    root = snapshots[0].local_path.parent
    report = module('bundle').apply(approved, root, root / 'new')
    assert report.status == 'CONFLICT' and not (root / 'new').exists()
    source = snapshots[0].local_path; source.write_bytes(source.read_bytes() + b'changed')
    report = module('bundle').apply(approved, root, tmp_path / 'new')
    assert report.status == 'CONFLICT' and not (tmp_path / 'new').exists()


def test_complete_stage_cleanup_failure_retains_marker(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); real = bundle.shutil.rmtree
    def failed_cleanup(path, *args, **kwargs):
        if Path(path).name.startswith('.pdecision-apply-'):
            (Path(path) / 'INCOMPLETE').unlink(missing_ok=True)
            raise OSError(28, 'original controlled partially completed cleanup')
        return real(path, *args, **kwargs)
    from pathlib import Path
    monkeypatch.setattr(bundle.shutil, 'rmtree', failed_cleanup)
    destination = tmp_path / 'result'
    report = bundle.apply(approved, snapshots[0].local_path.parent, destination)
    assert report.status == 'LIMIT_EXCEEDED' and destination.exists()
    stage, = tmp_path.glob('.pdecision-apply-*')
    assert (stage / 'INCOMPLETE').read_bytes() == b'INCOMPLETE\n'
    assert 'committed' in ' '.join(report.diagnostics)
    assert 'retained' in ' '.join(report.diagnostics)
    assert not bundle.apply(approved, snapshots[0].local_path.parent, destination).verified


def test_standalone_rewrite_rechecks_exact_inventory_and_hashes(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    transform = module('transform'); bundle = module('bundle')
    import parquet_decision.supervise as supervisor
    real = supervisor.run_worker
    def corrupt_after_success(request, limits):
        result = real(request, limits)
        path = Path(dict(request.args)['dataset']) / 'part-00000.parquet'
        path.write_bytes(path.read_bytes() + b'original ordinary corruption')
        return result
    from pathlib import Path
    monkeypatch.setattr(supervisor, 'run_worker', corrupt_after_success)
    stage = tmp_path / 'rewrite'; stage.mkdir()
    with pytest.raises(Exception): transform.rewrite(approved, snapshots, stage)


def test_parent_operation_elapsed_failure_is_not_worker_resource_claim(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); real = bundle.run_worker
    now = [1000.0]
    monkeypatch.setattr(bundle.time, 'monotonic', lambda: now[0])
    def elapsed_after_success(request, limits):
        result = real(request, limits)
        now[0] += limits.wall_seconds + 1
        return result
    monkeypatch.setattr(bundle, 'run_worker', elapsed_after_success)
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'LIMIT_EXCEEDED' and not (tmp_path / 'result').exists()
    assert 'total operation elapsed' in ' '.join(report.diagnostics)


def test_apply_budget_reserves_shared_publication_probe_bytes(tmp_path):
    from parquet_decision.transform import _Budget
    from parquet_decision.model import LimitError
    owned = tmp_path / 'owned'; owned.mkdir(); dataset = owned / 'dataset'; dataset.mkdir()
    budget = _Budget(replace(Limits(), max_staging_bytes=100), owned, dataset, reserve_bytes=12)
    budget.consume(88)
    with pytest.raises(LimitError): budget.consume(1)
    assert budget.staging_bytes == 88


def test_shared_publication_probe_peak_is_twelve_file_bytes(tmp_path, monkeypatch):
    import parquet_decision.artifacts as artifacts
    original = artifacts._rename_noreplace
    peaks = []
    def observe(source, target):
        peaks.append(sum(p.stat().st_size for p in source.parent.rglob('*') if p.is_file()))
        return original(source, target)
    monkeypatch.setattr(artifacts, '_rename_noreplace', observe)
    artifacts.qualify_noreplace(tmp_path)
    assert max(peaks) == 12


@pytest.mark.parametrize('edit', ['request_digest', 'mode', 'unknown_key', 'report_digest', 'metadata_hash', 'bundle_bytes', 'staging_bytes', 'unverified'])
def test_complete_worker_artifact_protocol_edits_never_publish(tmp_path, monkeypatch, edit):
    from pathlib import Path
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); original = bundle.run_worker
    def corrupt_artifact(request, limits):
        result = original(request, limits)
        path = Path(dict(request.args)['destination']); record = json.loads(path.read_text())
        if edit == 'request_digest': record['request_digest'] = 'f' * 64
        elif edit == 'mode': record['mode'] = 'rewrite'
        elif edit == 'unknown_key': record['extra'] = 1
        elif edit == 'report_digest': record['result']['output_digest'] = 'f' * 64
        elif edit == 'metadata_hash': record['metadata_sha256']['_manifest.json'] = 'f' * 64
        elif edit == 'unverified': record['result']['status'] = 'INCOMPLETE'
        else: record[edit] += 1
        path.write_bytes(canonical_json(record))
        return result
    monkeypatch.setattr(bundle, 'run_worker', corrupt_artifact)
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'ERROR' and not (tmp_path / 'result').exists()


@pytest.mark.parametrize('entry', ['extra.parquet', 'unknown.txt', 'directory', 'symlink'])
def test_complete_bundle_exact_inventory_refuses_extra_entries(tmp_path, entry):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); destination = tmp_path / 'result'
    assert bundle.apply(approved, snapshots[0].local_path.parent, destination).verified
    path = destination / entry
    if entry == 'directory': path.mkdir()
    elif entry == 'symlink': path.symlink_to(destination / 'part-00000.parquet')
    else: path.write_bytes(b'original additional entry')
    with pytest.raises(Exception): bundle.read_bundle(destination)


@pytest.fixture
def bounded_original_bundle(tmp_path):
    """Two tiny rows with known recursive shape; no malformed native input."""
    from pathlib import Path
    import pyarrow as pa
    from parquet_decision.preflight import preflight
    from parquet_decision.decisions import approve
    root = tmp_path / 'bounded-source'; root.mkdir()
    schema = pa.schema([
        pa.field('outer', pa.struct([pa.field('inner', pa.struct([pa.field('value', pa.int32())]))])),
        pa.field('x', pa.int32(), metadata={b'unit': b'm'})])
    for source_id in range(2):
        table = pa.Table.from_pylist([{'outer': {'inner': {'value': source_id}}, 'x': source_id}], schema=schema)
        pq.write_table(table, root / f'{source_id}.parquet', version='2.6', compression='NONE', use_dictionary=False)
    approved = approve(preflight([Path('0.parquet'), Path('1.parquet')], root, Limits()), [])
    dataset = tmp_path / 'bounded-bundle'
    assert module('bundle').apply(approved, root, dataset).verified
    return approved, root, dataset


def _forbidden_deep_adapter(cls, value):
    raise AssertionError(f'known raw bound must precede {cls.__name__}.from_dict')


@pytest.mark.parametrize('limit_name,adapter_name', [
    ('max_rows', 'SourceSpec'), ('max_fields', 'FieldSpec'), ('max_depth', 'TypeSpec'),
    ('max_files', 'SourceSpec'), ('max_input_bytes', 'SourceSpec'),
    ('max_metadata_bytes', 'FieldSpec'), ('max_plan_bytes', 'SourceSpec')])
@pytest.mark.parametrize('ingress', ['bundle', 'worker'])
def test_declared_json_caps_precede_recursive_adapters(bounded_original_bundle, tmp_path, monkeypatch, limit_name, adapter_name, ingress):
    from pathlib import Path
    from parquet_decision import model, supervise
    approved, root, dataset = bounded_original_bundle
    raw = approved.to_dict(); raw['plan']['limits'][limit_name] = 1
    if ingress == 'bundle':
        (dataset / '_decision.json').write_bytes(canonical_json(raw))
        read = lambda: module('bundle').read_bundle(dataset)
    else:
        stage = tmp_path / 'bounded-control'; stage.mkdir(); output = stage / 'dataset'; output.mkdir()
        path = stage / 'approved.json'; path.write_bytes(canonical_json(raw))
        request = model.WorkerRequest(1, 'rewrite', tuple(str(root / s.relative_path) for s in approved.plan.sources),
                      (('approved_plan', str(path)), ('staging', str(stage)), ('dataset', str(output)),
                       ('destination', str(stage / 'result.json'))))
        monkeypatch.setattr(supervise, '_WORKER_READY', True)
        read = lambda: module('bundle')._rewrite_worker(request, Limits())
    monkeypatch.setattr(getattr(model, adapter_name), 'from_dict', classmethod(_forbidden_deep_adapter))
    with pytest.raises(model.LimitError): read()


@pytest.mark.parametrize('limit_name,adapter_name', [
    ('max_rows', 'SourceSpec'), ('max_fields', 'FieldSpec'), ('max_depth', 'TypeSpec'), ('max_files', 'SourceSpec')])
def test_worker_caller_json_caps_precede_recursive_adapters(bounded_original_bundle, tmp_path, monkeypatch, limit_name, adapter_name):
    from parquet_decision import model, supervise
    approved, root, _ = bounded_original_bundle
    stage = tmp_path / 'caller-control'; stage.mkdir(); dataset = stage / 'dataset'; dataset.mkdir()
    path = stage / 'approved.json'; path.write_bytes(canonical_json(approved.to_dict()))
    request = model.WorkerRequest(1, 'rewrite', tuple(str(root / s.relative_path) for s in approved.plan.sources),
              (('approved_plan', str(path)), ('staging', str(stage)), ('dataset', str(dataset)),
               ('destination', str(stage / 'result.json'))))
    monkeypatch.setattr(supervise, '_WORKER_READY', True)
    monkeypatch.setattr(getattr(model, adapter_name), 'from_dict', classmethod(_forbidden_deep_adapter))
    with pytest.raises(model.LimitError):
        module('bundle')._rewrite_worker(request, replace(Limits(), **{limit_name: 1}))


@pytest.mark.parametrize('ingress', ['bundle', 'worker'])
def test_raw_resolution_count_precedes_resolution_adapter(bounded_original_bundle, tmp_path, monkeypatch, ingress):
    from parquet_decision import model, supervise
    approved, root, dataset = bounded_original_bundle
    raw = approved.to_dict()
    raw['resolutions'] = [{'conflict_digest': 'f' * 64, 'action': 'archive_only'}] * 2
    # Lower this independent record cap to exercise its order with two tiny records.
    monkeypatch.setattr(model, 'MAX_RESOLUTIONS', 1)
    if ingress == 'bundle':
        (dataset / '_decision.json').write_bytes(canonical_json(raw))
        read = lambda: module('bundle').read_bundle(dataset)
    else:
        stage = tmp_path / 'resolution-control'; stage.mkdir(); output = stage / 'dataset'; output.mkdir()
        path = stage / 'approved.json'; path.write_bytes(canonical_json(raw))
        request = model.WorkerRequest(1, 'rewrite', tuple(str(root / s.relative_path) for s in approved.plan.sources),
                  (('approved_plan', str(path)), ('staging', str(stage)), ('dataset', str(output)),
                   ('destination', str(stage / 'result.json'))))
        monkeypatch.setattr(supervise, '_WORKER_READY', True)
        read = lambda: module('bundle')._rewrite_worker(request, Limits())
    monkeypatch.setattr(model.Resolution, 'from_dict', classmethod(_forbidden_deep_adapter))
    with pytest.raises(model.LimitError): read()


@pytest.mark.parametrize('ingress', ['manifest', 'standalone'])
@pytest.mark.parametrize('delta', [-1, 1])
def test_known_raw_part_count_precedes_part_adapter(bounded_original_bundle, tmp_path, monkeypatch, ingress, delta):
    from pathlib import Path
    from parquet_decision import model, supervise
    approved, root, dataset = bounded_original_bundle
    if ingress == 'manifest':
        path = dataset / '_manifest.json'; record = json.loads(path.read_bytes())
        if delta == 1: record['parts'].append(record['parts'][0])
        else: record['parts'].pop()
        path.write_bytes(canonical_json(record))
        read = lambda: module('bundle').read_bundle(dataset)
    else:
        real = supervise.run_worker
        def mismatch(request, limits):
            terminal = real(request, limits)
            path = Path(dict(request.args)['destination']); record = json.loads(path.read_bytes())
            if delta == 1: record['result'].append(record['result'][0])
            else: record['result'].pop()
            path.write_bytes(canonical_json(record))
            return terminal
        monkeypatch.setattr(supervise, 'run_worker', mismatch)
        output = tmp_path / 'part-count'; output.mkdir()
        snapshots = [model.Snapshot(source, root / source.relative_path) for source in approved.plan.sources]
        read = lambda: module('transform').rewrite(approved, snapshots, output)
    monkeypatch.setattr(model.PartRecord, 'from_dict', classmethod(_forbidden_deep_adapter))
    with pytest.raises(model.ModelError): read()


def test_fixed_control_precheck_keeps_fresh_worker_guard(tmp_path, monkeypatch):
    from parquet_decision import model, supervise
    stage = tmp_path / 'guarded-stage'; stage.mkdir(); dataset = stage / 'dataset'; dataset.mkdir()
    request = model.WorkerRequest(1, 'rewrite', args=(('approved_plan', str(stage / 'approved.json')),
                  ('staging', str(stage)), ('dataset', str(dataset)), ('destination', str(stage / 'result.json'))))
    bundle = module('bundle')
    monkeypatch.setattr(supervise, '_WORKER_READY', False)
    monkeypatch.setattr(bundle, '_read_approved', lambda *a, **k: pytest.fail('control read preceded fresh-worker guard'))
    with pytest.raises(model.ModelError, match='fresh qualified worker'):
        bundle._rewrite_worker(request, Limits())


def test_bundle_precheck_uses_one_bounded_decision_descriptor(bounded_original_bundle, monkeypatch):
    from contextlib import contextmanager
    from parquet_decision import model
    _, _, dataset = bounded_original_bundle
    bundle = module('bundle'); original = bundle._open_relative
    decision_reads = []
    @contextmanager
    def observe(root, name):
        if name == '_decision.json': decision_reads.append((root, name))
        with original(root, name) as fd: yield fd
    monkeypatch.setattr(bundle, '_open_relative', observe)
    monkeypatch.setattr(model, 'read_json', lambda *a, **k: pytest.fail('bounded decision descriptor was reopened through pathname loader'))
    bundle.read_bundle(dataset)
    assert decision_reads == [(dataset, '_decision.json')]
