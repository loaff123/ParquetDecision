"""Controlled original process fixtures; none are product operation tags."""
from dataclasses import replace
from importlib import import_module, util
import json
from pathlib import Path
import sys
import threading
import time

import pytest

from parquet_decision.model import Limits, WorkerRequest


def supervision_module():
    assert util.find_spec('parquet_decision.supervise') is not None, 'bounded worker supervision is missing'
    return import_module('parquet_decision.supervise')


def run_original_fixture(tmp_path, code, *, limits=None, cancel_event=None):
    module = supervision_module()
    fixture = tmp_path / 'original_child.py'
    fixture.write_text(code)
    return module._supervise_process([sys.executable, '-I', '-S', str(fixture)], b'',
                                     WorkerRequest(1, 'preflight'), limits or Limits(),
                                     cwd=tmp_path, env={}, cancel_event=cancel_event)


def terminal(status='COMPLETE', operation='preflight', diagnostics=()):
    return json.dumps(dict(protocol_version=1, operation=operation, status=status, counters=[], diagnostics=list(diagnostics)))


def test_supervisor_statuses_wall_watchdog(tmp_path):
    start = time.monotonic()
    result = run_original_fixture(tmp_path, 'import time; time.sleep(10)', limits=replace(Limits(), wall_seconds=1))
    assert result.status == 'LIMIT_EXCEEDED'
    assert 'wall' in ' '.join(result.diagnostics).lower()
    assert time.monotonic() - start < 4


def test_supervisor_statuses_cpu_limit(tmp_path):
    code = 'import resource\nresource.setrlimit(resource.RLIMIT_CPU, (1, 2))\nwhile True: pass\n'
    result = run_original_fixture(tmp_path, code, limits=replace(Limits(), cpu_seconds=1, wall_seconds=5))
    assert result.status == 'LIMIT_EXCEEDED'
    assert 'SIGXCPU' in ' '.join(result.diagnostics)


def test_supervisor_statuses_recognized_allocation_failure(tmp_path):
    code = ('import resource\nresource.setrlimit(resource.RLIMIT_AS, (64*1024**2, 64*1024**2))\n'
            'try: value = bytearray(256*1024**2)\n'
            'except MemoryError:\n print(%r)\n' % terminal('LIMIT_EXCEEDED', diagnostics=('MemoryError: allocation refused under RLIMIT_AS',)))
    result = run_original_fixture(tmp_path, code)
    assert result.status == 'LIMIT_EXCEEDED'


def test_supervisor_statuses_explicit_child_error(tmp_path):
    result = run_original_fixture(tmp_path, 'print(%r)' % terminal('ERROR', diagnostics=('original controlled child failure',)))
    assert result.status == 'ERROR'
    assert 'original controlled child failure' in result.diagnostics


def test_supervisor_statuses_cancellation(tmp_path):
    event = threading.Event()
    timer = threading.Timer(0.1, event.set); timer.start()
    try:
        result = run_original_fixture(tmp_path, 'import time; time.sleep(10)', cancel_event=event)
    finally:
        timer.cancel(); timer.join()
    assert result.status == 'CANCELLED'


@pytest.mark.parametrize('output', ['not json', '{"protocol_version":1', terminal(operation='verify'), terminal(diagnostics=('x' * 4097,)), terminal() + terminal()])
def test_supervisor_invalid_truncated_or_mismatched_protocol_is_error(tmp_path, output):
    assert run_original_fixture(tmp_path, 'print(%r)' % output).status == 'ERROR'


@pytest.mark.parametrize('signal_name', ['SIGKILL', 'SIGTERM'])
def test_supervisor_unknown_signal_never_guesses_memory_exhaustion(tmp_path, signal_name):
    result = run_original_fixture(tmp_path, f'import os, signal; os.kill(os.getpid(), signal.{signal_name})')
    assert result.status == 'ERROR'
    assert signal_name in ' '.join(result.diagnostics)


@pytest.mark.parametrize('stream', ['stdout', 'stderr'])
def test_supervisor_drains_streams_but_bounds_protocol_and_diagnostics(tmp_path, stream):
    code = f'import sys\nsys.{stream}.write("x" * 2_000_000)\nsys.{stream}.flush()\n'
    result = run_original_fixture(tmp_path, code)
    assert result.status == 'ERROR'
    assert len(result.diagnostics) <= 32
    assert all(len(d.encode()) <= 4096 for d in result.diagnostics)


def test_production_fixed_allowlist_does_not_execute_caller_code(tmp_path):
    module = supervision_module()
    # Rewrite and verify are fixed handlers whose required
    # declarative args cannot be replaced by a caller-selected callable.
    result = module.run_worker(WorkerRequest(1, 'rewrite'), Limits())
    assert result.status == 'ERROR'
    assert 'invalid fixed rewrite worker args' in ' '.join(result.diagnostics)
    result = module.run_worker(WorkerRequest(1, 'verify'), Limits())
    assert result.status == 'ERROR'
    assert 'invalid fixed verify worker args' in ' '.join(result.diagnostics)
    result = module.run_worker(WorkerRequest(1, 'preflight', args=(('source_root', str(tmp_path)),)), Limits())
    assert result.status == 'ERROR'


def test_real_worker_limits_before_native_import_and_resource_observations(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root, stage = tmp_path / 'sources', tmp_path / 'stage'; root.mkdir(); stage.mkdir()
    pq.write_table(pa.table({'x': [1, 2]}), root / 'a.parquet')
    request = WorkerRequest(1, 'preflight', ('a.parquet',),
                            (('source_root', str(root)), ('staging', str(stage)), ('destination', str(stage / 'plan.json'))))
    result = supervision_module().run_worker(request, Limits())
    assert result.status == 'COMPLETE', result.diagnostics
    counters = dict(result.counters)
    assert counters['address_space_bytes'] == 4 * 1024**3
    assert counters['cpu_seconds'] == 120
    assert 0 < counters['peak_rss_kib'] < 4 * 1024**2
    assert (counters['files'], counters['rows']) == (1, 2)


def test_child_nonzero_exit_overrides_valid_protocol(tmp_path):
    result = run_original_fixture(tmp_path, 'import sys\nprint(%r)\nsys.exit(7)' % terminal())
    assert result.status == 'ERROR'
    assert 'exit 7' in ' '.join(result.diagnostics)


def test_invalid_record_counter_is_protocol_error(tmp_path):
    record = json.loads(terminal()); record['counters'] = [['rows', True]]
    assert run_original_fixture(tmp_path, 'print(%r)' % json.dumps(record)).status == 'ERROR'


def test_maximum_diagnostic_bytes_are_preserved_at_exact_boundary(tmp_path):
    original = 'x' * 4096
    result = run_original_fixture(tmp_path, 'print(%r)' % terminal('ERROR', diagnostics=(original,)))
    assert result.status == 'ERROR' and result.diagnostics == (original,)


def test_drain_large_stderr_and_stdout_without_deadlock(tmp_path):
    # Both streams are substantial but remain inside the separate envelopes.
    code = 'import sys\nsys.stderr.write("d" * 120000)\nsys.stderr.flush()\nprint(%r)' % terminal()
    result = run_original_fixture(tmp_path, code, limits=replace(Limits(), wall_seconds=2))
    assert result.status == 'COMPLETE'


def test_worker_dependency_discovery_ignores_parent_import_path(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import subprocess
    root = tmp_path / 'sources'; root.mkdir()
    pq.write_table(pa.table({'x': [1]}), root / 'a.parquet')
    (root / 'pyarrow').mkdir(); (root / 'pyarrow' / '__init__.py').write_text("raise RuntimeError('original caller module must be ignored')\n")
    code = ("import sys; from pathlib import Path; "
            "sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[2]); "
            "from parquet_decision.preflight import preflight; from parquet_decision.model import Limits; "
            "plan=preflight([Path('a.parquet')], Path(sys.argv[2]), Limits()); "
            "assert plan.status == 'READY_FOR_REVIEW', (plan.status, plan.diagnostics)")
    process = subprocess.run([sys.executable, '-c', code, str(Path(__import__('parquet_decision').__file__).resolve().parents[1]), str(root)], capture_output=True, text=True)
    assert process.returncode == 0, process.stderr


def test_watchdog_still_applies_after_child_closes_pipes(tmp_path):
    code = 'import sys, time\nsys.stdin.close(); sys.stdout.close(); sys.stderr.close()\ntime.sleep(10)'
    assert run_original_fixture(tmp_path, code, limits=replace(Limits(), wall_seconds=1)).status == 'LIMIT_EXCEEDED'


def test_cancelled_child_is_reaped_without_reading_threads(tmp_path):
    import os
    event = threading.Event()
    timer = threading.Timer(0.15, event.set); timer.start()
    pid_file = tmp_path / 'child.pid'
    code = 'import os, time\nfrom pathlib import Path\nPath(%r).write_text(str(os.getpid()))\ntime.sleep(10)' % str(pid_file)
    try:
        result = run_original_fixture(tmp_path, code, cancel_event=event)
    finally:
        timer.cancel(); timer.join()
    assert result.status == 'CANCELLED'
    assert pid_file.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


def test_full_model_diagnostic_envelope_is_accepted(tmp_path):
    diagnostics = tuple('x' * 4096 for _ in range(32))
    result = run_original_fixture(tmp_path, 'print(%r)' % terminal('ERROR', diagnostics=diagnostics))
    assert result.diagnostics == diagnostics
    assert sum(len(d.encode()) for d in result.diagnostics) == 131072


def test_bootstrap_known_python_allocation_failure_is_a_limit_result(tmp_path):
    # Extremely lowered AS prevents even native-free product import. A typed
    # MemoryError observed by the fixed bootstrap must retain its known cause.
    result = supervision_module().run_worker(WorkerRequest(1, 'preflight'), replace(Limits(), address_space_bytes=1))
    assert result.status == 'LIMIT_EXCEEDED', result.diagnostics
    assert any(label in ' '.join(result.diagnostics) for label in ('MemoryError', 'ENOMEM'))


def test_worker_refuses_unqualified_python_patch_without_claiming_pinned_runtime(monkeypatch):
    module = supervision_module()
    monkeypatch.setattr(module.sys, 'version_info', (3, 12, 99))
    result = module.run_worker(WorkerRequest(1, 'preflight'), Limits())
    assert result.status == 'UNSUPPORTED'


@pytest.mark.parametrize('number', [12, 28, 122, 13])
def test_parent_spawn_preserves_exact_os_resource_cause(monkeypatch, number):
    module = supervision_module()
    def refuse_spawn(*args, **kwargs):
        raise OSError(number, 'original controlled parent spawn refusal')
    monkeypatch.setattr(module.subprocess, 'Popen', refuse_spawn)
    result = module.run_worker(WorkerRequest(1, 'preflight'), Limits())
    assert result.status == ('LIMIT_EXCEEDED' if number in (12, 28, 122) else 'ERROR')
    assert all(len(item.encode()) <= 4096 for item in result.diagnostics)
    assert 'RLIMIT_AS' not in ' '.join(result.diagnostics)


@pytest.mark.parametrize('number', [12, 28, 122, 13])
def test_parent_worker_directory_creation_preserves_exact_os_cause(monkeypatch, number):
    module = supervision_module()
    def refuse_directory(*args, **kwargs):
        raise OSError(number, 'original controlled parent directory refusal')
    monkeypatch.setattr(module.tempfile, 'TemporaryDirectory', refuse_directory)
    result = module.run_worker(WorkerRequest(1, 'preflight'), Limits())
    assert result.status == ('LIMIT_EXCEEDED' if number in (12, 28, 122) else 'ERROR')
    assert all(len(item.encode()) <= 4096 for item in result.diagnostics)


@pytest.mark.parametrize('number', [12, 28, 122, 13])
def test_parent_pipe_setup_preserves_os_cause_and_reaps_child(tmp_path, monkeypatch, number):
    module = supervision_module(); captured = []
    real_popen = module.subprocess.Popen
    def capture_child(*args, **kwargs):
        child = real_popen(*args, **kwargs); captured.append(child); return child
    def refuse_selector(*args, **kwargs):
        raise OSError(number, 'original controlled parent selector refusal')
    monkeypatch.setattr(module.subprocess, 'Popen', capture_child)
    monkeypatch.setattr(module.selectors, 'DefaultSelector', refuse_selector)
    result = run_original_fixture(tmp_path, 'import time; time.sleep(10)')
    assert len(captured) == 1 and captured[0].poll() is not None
    assert result.status == ('LIMIT_EXCEEDED' if number in (12, 28, 122) else 'ERROR')


def test_unnamed_realtime_signal_retains_number_and_returns_error(tmp_path):
    import signal
    number = int(signal.SIGRTMIN) + 1
    result = run_original_fixture(tmp_path, 'import os; os.kill(os.getpid(), %d)' % number)
    assert result.status == 'ERROR'
    assert str(number) in ' '.join(result.diagnostics)
