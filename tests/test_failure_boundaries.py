from pathlib import Path
import errno
import hashlib
import os
import subprocess
import sys
import pytest
from parquet_decision.model import UnsupportedError
from tests.verify_fixtures import simple_case
from tests.test_transform import module


def test_publication_races_and_retry(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    root = snapshots[0].local_path.parent; bundle = module('bundle')
    destination = tmp_path / 'result'; destination.mkdir(); (destination / 'token').write_bytes(b'preserve')
    report = bundle.apply(approved, root, destination)
    assert report.status == 'CONFLICT' and (destination / 'token').read_bytes() == b'preserve'
    destination = tmp_path / 'raced'
    real = bundle.publish_directory_exclusive
    def competitor(stage, target, protected):
        subprocess.run([sys.executable, '-I', '-S', '-c', 'from pathlib import Path; import sys; p=Path(sys.argv[1]); p.mkdir(); (p/"token").write_bytes(b"competing")', str(target)], check=True)
        return real(stage, target, protected)
    monkeypatch.setattr(bundle, 'publish_directory_exclusive', competitor)
    assert bundle.apply(approved, root, destination).status == 'CONFLICT'
    assert (destination / 'token').read_bytes() == b'competing'


def test_rename_unavailable_refuses_before_worker(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle')
    monkeypatch.setattr(bundle, 'qualify_noreplace', lambda _: (_ for _ in ()).throw(UnsupportedError('unavailable')))
    monkeypatch.setattr(bundle, 'run_worker', lambda *args: pytest.fail('expensive worker started'))
    assert bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result').status == 'UNSUPPORTED'


def test_final_rename_followed_by_reporting_failure_is_discoverable(tmp_path, monkeypatch):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); real = bundle.publish_directory_exclusive; destination = tmp_path / 'result'
    def committed_then_failure(*args):
        real(*args)
        raise BrokenPipeError(errno.EPIPE, 'original controlled post-rename failure')
    monkeypatch.setattr(bundle, 'publish_directory_exclusive', committed_then_failure)
    report = bundle.apply(approved, snapshots[0].local_path.parent, destination)
    assert report.status == 'ERROR' and destination.exists()
    assert 'committed' in ' '.join(report.diagnostics).lower()
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in destination.iterdir()}
    module('bundle').read_bundle(destination)
    retry = bundle.apply(approved, snapshots[0].local_path.parent, destination)
    assert retry.status == 'CONFLICT'
    assert before == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in destination.iterdir()}


@pytest.mark.parametrize('boundary', ['snapshot', 'write', 'verify', 'manifest', 'publish'])
def test_interrupt_owned_staging(tmp_path, monkeypatch, boundary):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle'); root = snapshots[0].local_path.parent
    before = snapshots[0].local_path.read_bytes()
    if boundary == 'publish':
        monkeypatch.setattr(bundle, 'publish_directory_exclusive', lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    else:
        # The controlled worker runner executes the fixed handler in this original
        # test process to inject a real Python interruption at each exact boundary.
        import parquet_decision.supervise as supervisor
        from parquet_decision.model import WorkerResult
        def interrupted(request, limits):
            monkeypatch.setattr(supervisor, '_WORKER_READY', True)
            name = {'snapshot': 'snapshot_sources', 'write': '_rewrite', 'verify': '_verify_streams', 'manifest': '_write_manifest'}[boundary]
            monkeypatch.setattr(bundle, name, lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()))
            try: return bundle._rewrite_worker(request, limits)
            except KeyboardInterrupt: return WorkerResult(1, 'rewrite', 'CANCELLED')
        monkeypatch.setattr(bundle, 'run_worker', interrupted)
    report = bundle.apply(approved, root, tmp_path / 'result')
    assert report.status == 'CANCELLED' and not (tmp_path / 'result').exists()
    assert snapshots[0].local_path.read_bytes() == before
    assert not list(tmp_path.glob('.pdecision-apply-*'))


def controlled_fixed_worker(tmp_path, request, limits, *, injection, cancel=False):
    """Original benign child instrumentation, never a production code argument."""
    import json
    import sysconfig
    import threading
    import time
    from parquet_decision.model import canonical_json
    from parquet_decision.supervise import _supervise_process
    ready = tmp_path / 'boundary-ready'
    fixture = tmp_path / 'original_instrumented_worker.py'
    fixture.write_text('import sys, resource, os, json, time\n'
        f'resource.setrlimit(resource.RLIMIT_AS, ({limits.address_space_bytes}, {limits.address_space_bytes}))\n'
        f'resource.setrlimit(resource.RLIMIT_CPU, ({limits.cpu_seconds}, {limits.cpu_seconds + 1}))\n'
        f'sys.path.extend({[str(Path(__import__('parquet_decision').__file__).resolve().parents[1]), sysconfig.get_path("platlib")]!r})\n'
        'from pathlib import Path\nfrom parquet_decision import bundle, transform, supervise\n'
        'import pyarrow as pa\nimport pyarrow.parquet as pq\n'
        f'READY = Path({str(ready)!r})\n' + injection + '\n'
        'envelope = json.loads(sys.stdin.buffer.read())\n'
        'from parquet_decision.model import WorkerRequest, Limits, WorkerResult, canonical_json\n'
        'supervise._WORKER_READY = True\n'
        'request = WorkerRequest.from_dict(envelope["request"])\nlimits = Limits.from_dict(envelope["limits"])\n'
        'try:\n result = bundle._rewrite_worker(request, limits)\n'
        'except Exception as exc:\n result = WorkerResult(1, "rewrite", bundle._exception_status(exc), diagnostics=(str(exc),))\n'
        'observations = (("address_space_bytes", resource.getrlimit(resource.RLIMIT_AS)[0]), ("cpu_seconds", resource.getrlimit(resource.RLIMIT_CPU)[0]), ("peak_rss_kib", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))\n'
        'result = WorkerResult(1, "rewrite", result.status, tuple(sorted((*result.counters, *observations))), result.diagnostics)\n'
        'sys.stdout.buffer.write(canonical_json(result.to_dict())); sys.stdout.buffer.flush()\n')
    event = threading.Event()
    failures = []
    def wait_and_cancel():
        end = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < end: time.sleep(0.01)
        if not ready.exists(): failures.append('instrumented boundary never reached')
        event.set()
    watcher = threading.Thread(target=wait_and_cancel) if cancel else None
    if watcher: watcher.start()
    try:
        result = _supervise_process([sys.executable, '-I', '-S', str(fixture)],
                    canonical_json({'request': request.to_dict(), 'limits': limits.to_dict()}),
                    request, limits, cwd=tmp_path, env={}, cancel_event=event if cancel else None)
    finally:
        if watcher: watcher.join(timeout=12)
    assert not failures
    return result


@pytest.mark.parametrize('boundary', ['snapshot', 'write', 'verify', 'manifest'])
def test_real_worker_cancellation_at_operation_boundaries(tmp_path, monkeypatch, boundary):
    approved, snapshots, _, _ = simple_case(tmp_path)
    before = snapshots[0].local_path.read_bytes()
    bundle = module('bundle')
    name = {'snapshot': 'snapshot_sources', 'write': '_rewrite', 'verify': '_verify_streams', 'manifest': '_write_manifest'}[boundary]
    injection = f'''original = bundle.{name}
def pause(*args, **kwargs):
    value = original(*args, **kwargs)
    READY.write_text(str(os.getpid()))
    while True: time.sleep(1)
bundle.{name} = pause'''
    monkeypatch.setattr(bundle, 'run_worker', lambda r, l: controlled_fixed_worker(tmp_path, r, l, injection=injection, cancel=True))
    result = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert result.status == 'CANCELLED' and not (tmp_path / 'result').exists()
    assert snapshots[0].local_path.read_bytes() == before
    pid = int((tmp_path / 'boundary-ready').read_text())
    with pytest.raises(ProcessLookupError): os.kill(pid, 0)
    assert not list(tmp_path.glob('.pdecision-apply-*'))


def test_independent_verifier_blocks_ordinary_writer_mistake(tmp_path, monkeypatch):
    from tests.verify_fixtures import union_case
    approved, snapshots, _, _ = union_case(tmp_path)
    bundle = module('bundle')
    injection = '''original = bundle._rewrite
def mistaken(*args, **kwargs):
    parts = original(*args, **kwargs)
    part = parts[0]
    table = pq.read_table(part.local_path)
    table = table.set_column(0, table.schema.field(0), pa.array([999] * table.num_rows, type=table.schema.field(0).type))
    pq.write_table(table, part.local_path, version='2.6', compression='NONE', use_dictionary=False)
    from dataclasses import replace
    import hashlib
    parts[0] = replace(part, bytes=part.local_path.stat().st_size, sha256=hashlib.sha256(part.local_path.read_bytes()).hexdigest())
    return parts
bundle._rewrite = mistaken'''
    monkeypatch.setattr(bundle, 'run_worker', lambda r, l: controlled_fixed_worker(tmp_path, r, l, injection=injection))
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'CONFLICT' and not (tmp_path / 'result').exists()
    assert 'expected null' in ' '.join(report.diagnostics)


@pytest.mark.parametrize('filename', ['_decision.json', '_verification.json', '_manifest.json', 'result.json'])
def test_real_metadata_write_failure_leaves_no_complete_destination(tmp_path, monkeypatch, filename):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle')
    injection = f'''original = transform._Budget.write
def failed_write(self, path, raw, **kwargs):
    if path.name == {filename!r}: raise OSError(28, 'original controlled metadata disk failure')
    return original(self, path, raw, **kwargs)
transform._Budget.write = failed_write'''
    monkeypatch.setattr(bundle, 'run_worker', lambda r, l: controlled_fixed_worker(tmp_path, r, l, injection=injection))
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'LIMIT_EXCEEDED' and not (tmp_path / 'result').exists()
    assert not list(tmp_path.glob('.pdecision-apply-*'))


@pytest.mark.parametrize('failure', ['stderr', 'stdout', 'SIGKILL'])
def test_real_late_worker_failure_cannot_promote_completed_stage(tmp_path, monkeypatch, failure):
    approved, snapshots, _, _ = simple_case(tmp_path)
    bundle = module('bundle')
    tail = ('sys.stderr.write("x" * 200000); sys.stderr.flush()' if failure == 'stderr' else
            'sys.stdout.write("not terminal JSON"); sys.stdout.flush()' if failure == 'stdout' else
            'os.kill(os.getpid(), 9)')
    injection = f'''original = bundle._rewrite_worker
def late_failure(*args, **kwargs):
    result = original(*args, **kwargs)
    {tail}
    return result
bundle._rewrite_worker = late_failure'''
    monkeypatch.setattr(bundle, 'run_worker', lambda r, l: controlled_fixed_worker(tmp_path, r, l, injection=injection))
    report = bundle.apply(approved, snapshots[0].local_path.parent, tmp_path / 'result')
    assert report.status == 'ERROR' and not (tmp_path / 'result').exists()


def test_real_two_apply_processes_race_exclusively(tmp_path):
    from parquet_decision.model import canonical_json
    approved, snapshots, _, _ = simple_case(tmp_path)
    config = tmp_path / 'approved.json'; config.write_bytes(canonical_json(approved.to_dict()))
    fixture = tmp_path / 'original_apply_process.py'
    fixture.write_text('import sys, json\nfrom pathlib import Path\n'
        f'sys.path.insert(0, {str(Path(__import__('parquet_decision').__file__).resolve().parents[1])!r})\n'
        'from parquet_decision.model import ApprovedPlan, parse_json\nfrom parquet_decision.bundle import apply\n'
        'result = apply(ApprovedPlan.from_dict(parse_json(Path(sys.argv[1]).read_bytes())), Path(sys.argv[2]), Path(sys.argv[3]))\n'
        'print(json.dumps(result.to_dict()))\n')
    args = [sys.executable, '-I', str(fixture), str(config), str(snapshots[0].local_path.parent), str(tmp_path / 'result')]
    children = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    import json
    reports = []
    for child in children:
        stdout, stderr = child.communicate(timeout=15)
        assert child.returncode == 0, stderr
        reports.append(json.loads(stdout))
    assert sorted(r['status'] for r in reports) == ['CONFLICT', 'VERIFIED']
    module('bundle').read_bundle(tmp_path / 'result')
    assert not list(tmp_path.glob('.pdecision-apply-*'))


def test_real_parent_kill_before_publish_retains_owned_incomplete_container(tmp_path):
    import json
    import time
    from parquet_decision.model import canonical_json
    approved, snapshots, _, _ = simple_case(tmp_path)
    before = snapshots[0].local_path.read_bytes()
    config = tmp_path / 'approved.json'; config.write_bytes(canonical_json(approved.to_dict()))
    ready = tmp_path / 'ready'; fixture = tmp_path / 'original_killed_apply.py'
    fixture.write_text('import sys, time\nfrom pathlib import Path\n'
        f'sys.path.insert(0, {str(Path(__import__('parquet_decision').__file__).resolve().parents[1])!r})\n'
        'from parquet_decision import bundle\nfrom parquet_decision.model import ApprovedPlan, parse_json\n'
        f'def pause(*args):\n Path({str(ready)!r}).write_text(str(args[0])); time.sleep(30)\n'
        'bundle.publish_directory_exclusive = pause\n'
        'bundle.apply(ApprovedPlan.from_dict(parse_json(Path(sys.argv[1]).read_bytes())), Path(sys.argv[2]), Path(sys.argv[3]))\n')
    child = subprocess.Popen([sys.executable, '-I', str(fixture), str(config), str(snapshots[0].local_path.parent), str(tmp_path / 'result')])
    try:
        end = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < end: time.sleep(0.01)
        assert ready.exists()
        child.kill(); child.wait(timeout=5)
        dataset = Path(ready.read_text())
        assert dataset.parent.parent == tmp_path and (dataset.parent / 'INCOMPLETE').read_bytes() == b'INCOMPLETE\n'
        assert set(p.name for p in dataset.iterdir()) == {'part-00000.parquet', '_decision.json', '_verification.json', '_manifest.json'}
        assert not (tmp_path / 'result').exists() and snapshots[0].local_path.read_bytes() == before
    finally:
        if child.poll() is None: child.kill(); child.wait(timeout=5)


@pytest.mark.parametrize('operation', ['preflight', 'rewrite', 'verify'])
def test_fixed_worker_does_not_write_bytecode_outside_owned_staging(tmp_path, monkeypatch, operation):
    import shutil
    import parquet_decision.supervise as supervisor
    from parquet_decision.model import Limits
    from parquet_decision.preflight import preflight
    from parquet_decision.verify_values import verify_streams
    approved, snapshots, parts, _ = simple_case(tmp_path)
    package = tmp_path / 'fresh-product' / 'parquet_decision'
    shutil.copytree(Path(supervisor.__file__).parent, package, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    monkeypatch.setattr(supervisor, '__file__', str(package / 'supervise.py'))
    if operation == 'preflight':
        assert preflight([Path(snapshots[0].source.relative_path)], snapshots[0].local_path.parent, Limits()).preflight_complete
    elif operation == 'rewrite':
        assert module('bundle').apply(approved, snapshots[0].local_path.parent, tmp_path / 'result').verified
    else:
        assert verify_streams(approved, snapshots, parts).verified
    assert not list(package.rglob('*.pyc'))
    assert not list(package.rglob('__pycache__'))
