"""Fresh-process public CLI journey and protected report destinations."""
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest
from parquet_decision import cli
from parquet_decision.model import Limits, canonical_json
from tests.test_origin import bundle_case


def command(*args, cwd=None):
    import parquet_decision
    env = dict(os.environ, PYTHONPATH=str(Path(parquet_decision.__file__).parents[1]))
    return subprocess.run([sys.executable, '-m', 'parquet_decision', *map(str, args)],
                          cwd=cwd, env=env, capture_output=True, text=True, timeout=30)


def test_newcomer_journey(tmp_path):
    import importlib.util
    assert importlib.util.find_spec('parquet_decision.examples'), 'installed original example is missing'
    generated = subprocess.run([sys.executable, '-m', 'parquet_decision.examples', 'sources'], cwd=tmp_path,
                              env=dict(os.environ, PYTHONPATH=str(Path(cli.__file__).parents[1])), capture_output=True, text=True)
    assert generated.returncode == 0, generated.stderr
    result = command('plan', '--sources', 'sources', '--input', 'legacy.parquet', '--input', 'new.parquet', '--out', 'plan.json', cwd=tmp_path)
    assert result.returncode == 1 and 'NEEDS_DECISIONS' in result.stdout
    result = command('inspect', 'plan.json', cwd=tmp_path)
    assert result.returncode == 0 and 'cm' in result.stdout and 'Action:' in result.stdout
    p = json.loads((tmp_path / 'plan.json').read_text())
    assert len(p['proposal']['conflicts']) == 2
    choices = {'format_version': 1, 'plan_digest': p['plan_digest'], 'resolutions': [
        {'conflict_digest': p['proposal']['conflicts'][0]['conflict_digest'], 'action': 'archive_only'},
        {'conflict_digest': p['proposal']['conflicts'][1]['conflict_digest'], 'action': 'archive_only'}]}
    (tmp_path / 'decisions.json').write_text(json.dumps(choices))
    result = command('decide', 'plan.json', '--decisions', 'decisions.json', '--out', 'approved.json', cwd=tmp_path)
    assert result.returncode == 0 and 'APPROVED' in result.stdout
    result = command('apply', 'approved.json', '--sources', 'sources', '--out', 'result', cwd=tmp_path)
    assert result.returncode == 0 and 'VERIFIED' in result.stdout
    import pyarrow.parquet as pq
    assert pq.read_table(tmp_path / 'result').num_rows == 3
    result = command('origin', 'result', '--source-id', '1', '--row', '0', cwd=tmp_path)
    assert result.returncode == 0 and 'OUTPUT_HASHES_MATCH' in result.stdout and 'cm' in result.stdout
    result = command('verify', 'approved.json', '--sources', 'sources', '--dataset', 'result', '--report', 'fresh.json', cwd=tmp_path)
    assert result.returncode == 0 and 'VERIFIED' in result.stdout
    assert json.loads((tmp_path / 'fresh.json').read_text())['status'] == 'VERIFIED'
    (tmp_path / 'unresolved.json').write_text(json.dumps(dict(choices, resolutions=choices['resolutions'][:1])))
    result = command('decide', 'plan.json', '--decisions', 'unresolved.json', '--out', 'never-approved.json', cwd=tmp_path)
    assert result.returncode == 1 and not (tmp_path / 'never-approved.json').exists()
    (tmp_path / 'sources' / 'legacy.parquet').write_bytes(b'changed')
    result = command('apply', 'approved.json', '--sources', 'sources', '--out', 'never-complete', cwd=tmp_path)
    assert result.returncode == 1 and not (tmp_path / 'never-complete').exists()
    result = command('verify', 'approved.json', '--sources', 'missing', '--dataset', 'result', cwd=tmp_path)
    assert result.returncode == 3 and 'INCOMPLETE' in result.stdout


@pytest.mark.parametrize('target', ['dataset', 'bundle_child', 'approved', 'source', 'occupied'])
def test_verification_report_destination_is_protected(tmp_path, target, capsys):
    from parquet_decision.bundle import read_bundle
    destination, root = bundle_case(tmp_path)
    approved, _, _, _ = read_bundle(destination)
    approval_path = tmp_path / 'approval.json'; approval_path.write_bytes(canonical_json(approved.to_dict()))
    occupied = tmp_path / 'occupied.json'; occupied.write_bytes(b'keep')
    output = {'dataset': destination, 'bundle_child': destination / 'new-report.json',
              'approved': approval_path, 'source': root / 'source-1.parquet', 'occupied': occupied}[target]
    assert cli.main(['verify', str(approval_path), '--sources', str(root), '--dataset', str(destination), '--report', str(output)]) == 1
    assert 'CONFLICT' in capsys.readouterr().out
    assert not (destination / 'new-report.json').exists()
    assert occupied.read_bytes() == b'keep'


def test_inspect_bundle_labels_historical_before_stored_status(tmp_path, capsys):
    destination, _ = bundle_case(tmp_path)
    assert cli.main(['inspect', str(destination)]) == 0
    text = capsys.readouterr().out
    assert text.index('HISTORICAL') < text.index('Stored verification status: VERIFIED')
    assert 'OUTPUT_HASHES_MATCH' in text and 'Source value correctness was not checked' in text


@pytest.mark.parametrize('failure,expected,status', [('closed_pipe', 4, 'ERROR'), ('dev_full', 3, 'LIMIT_EXCEEDED')])
def test_apply_post_commit_reporting_failure(failure, expected, status, tmp_path):
    destination, root = bundle_case(tmp_path)
    from parquet_decision.bundle import read_bundle
    approved, _, _, _ = read_bundle(destination)
    path = tmp_path / 'approved.json'; path.write_bytes(canonical_json(approved.to_dict()))
    new = tmp_path / 'new-result'
    env = dict(os.environ, PYTHONPATH=str(Path(cli.__file__).parents[1]))
    args = [sys.executable, '-m', 'parquet_decision', 'apply', str(path), '--sources', str(root), '--out', str(new)]
    if failure == 'closed_pipe':
        process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.stdout.close(); stderr = process.stderr.read().decode(); code = process.wait(timeout=30)
    else:
        with open('/dev/full', 'wb') as full:
            process = subprocess.run(args, env=env, stdout=full, stderr=subprocess.PIPE, timeout=30)
        code, stderr = process.returncode, process.stderr.decode()
    assert new.is_dir() and code == expected
    assert status in stderr and 'artifact already published:' in stderr and str(new) in stderr


@pytest.mark.parametrize('signal_name', ['SIGINT', 'SIGTERM'])
def test_real_cli_cancellation_reaps_worker(tmp_path, signal_name):
    import signal
    import time
    import pyarrow as pa
    import pyarrow.parquet as pq
    from parquet_decision.preflight import preflight
    from parquet_decision.decisions import approve
    root = tmp_path / 'sources'; root.mkdir()
    pq.write_table(pa.table({'x': range(1_000_000)}), root / 'million.parquet', compression='NONE')
    plan = preflight([Path('million.parquet')], root, Limits())
    approved = approve(plan, [])
    path = tmp_path / 'approved.json'; path.write_bytes(canonical_json(approved.to_dict()))
    destination = tmp_path / 'cancelled'
    env = dict(os.environ, PYTHONPATH=str(Path(cli.__file__).parents[1]))
    ready = tmp_path / 'worker-pid'
    code = ("import sys\nfrom pathlib import Path\nfrom parquet_decision import cli, supervise\n"
            "original=supervise.subprocess.Popen\n"
            "def spawn(*args, **kwargs):\n p=original(*args, **kwargs)\n Path(sys.argv[1]).write_text(str(p.pid))\n return p\n"
            "supervise.subprocess.Popen=spawn\nraise SystemExit(cli.main(sys.argv[2:]))")
    process = subprocess.Popen([sys.executable, '-c', code, str(ready), 'apply', str(path), '--sources', str(root), '--out', str(destination)],
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    child = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and process.poll() is None:
        if ready.exists() and ready.read_text():
            child = int(ready.read_text()); break
        time.sleep(0.001)
    if child is None:
        out, err = process.communicate(timeout=10)
        pytest.fail('worker startup was not observed: ' + out + err)
    os.kill(process.pid, getattr(signal, signal_name))
    out, err = process.communicate(timeout=10)
    assert process.returncode == 3 and 'CANCELLED' in out, (process.returncode, out, err)
    assert not destination.exists()
    assert not list(tmp_path.glob('.pdecision-apply-*'))
    with pytest.raises(ProcessLookupError): os.kill(child, 0)


def test_report_honors_approved_lowered_staging_cap(tmp_path, capsys):
    from dataclasses import replace
    from parquet_decision.bundle import read_bundle
    destination, root = bundle_case(tmp_path)
    approved, _, _, _ = read_bundle(destination)
    # A successful fresh report under a tiny caller-lowered cap cannot be
    # published using default caps, even if verification itself refuses early.
    approved = replace(approved, plan=replace(approved.plan, limits=replace(Limits(), max_staging_bytes=1), plan_digest=''), approval_digest='')
    path = tmp_path / 'approval.json'; path.write_bytes(canonical_json(approved.to_dict()))
    report = tmp_path / 'report.json'
    assert cli.main(['verify', str(path), '--sources', str(root), '--dataset', str(destination), '--report', str(report)]) == 3
    assert not report.exists()
    assert 'LIMIT_EXCEEDED' in capsys.readouterr().out


@pytest.mark.parametrize('damage', ['part_bytes', 'verification_binding'])
def test_inspect_corrupt_complete_bundle_is_conflict(tmp_path, capsys, damage):
    destination, _ = bundle_case(tmp_path)
    if damage == 'part_bytes':
        path = destination / 'part-00000.parquet'
        path.write_bytes(path.read_bytes() + b'original benign identity change')
    else:
        path = destination / '_verification.json'
        record = json.loads(path.read_text()); record['source_digest'] = 'f' * 64
        path.write_bytes(canonical_json(record))
    assert cli.main(['inspect', str(destination)]) == 1
    text = capsys.readouterr().out
    assert text.startswith('CONFLICT:')
    assert 'INVALID_INPUT' not in text and 'Stored verification status: VERIFIED' not in text


@pytest.mark.parametrize('failure,expected,status', [
    ('limit', 3, 'LIMIT_EXCEEDED'), ('unsupported', 2, 'UNSUPPORTED'),
    ('missing', 3, 'INCOMPLETE'), ('ENOMEM', 3, 'LIMIT_EXCEEDED'),
    ('EIO', 4, 'ERROR'), ('wrapped_ENOMEM', 3, 'LIMIT_EXCEEDED'),
    ('wrapped_EIO', 4, 'ERROR'), ('wrapped_missing', 3, 'INCOMPLETE')])
def test_inspect_bundle_keeps_typed_operational_status(tmp_path, capsys, monkeypatch, failure, expected, status):
    import errno
    from parquet_decision.model import ModelError, LimitError, UnsupportedError
    destination = tmp_path / 'bundle'; destination.mkdir()
    def refuse(path):
        if failure == 'limit': raise LimitError('controlled known cap')
        if failure == 'unsupported': raise UnsupportedError('controlled unqualified runtime')
        if failure == 'missing': raise FileNotFoundError('controlled missing bundle metadata')
        if failure == 'wrapped_missing':
            raise ModelError('controlled missing reader wrapper') from FileNotFoundError('missing metadata')
        code = failure.removeprefix('wrapped_')
        error = OSError(getattr(errno, code), 'controlled bundle read error')
        if failure.startswith('wrapped_'):
            raise ModelError('controlled typed reader wrapper') from error
        raise error
    monkeypatch.setattr(cli, 'read_bundle', refuse)
    assert cli.main(['inspect', str(destination)]) == expected
    assert capsys.readouterr().out.startswith(status + ':')


def test_inspect_standalone_malformed_plan_remains_invalid_input(tmp_path, capsys):
    path = tmp_path / 'malformed.json'; path.write_bytes(b'ordinary malformed record')
    assert cli.main(['inspect', str(path)]) == 2
    assert capsys.readouterr().out.startswith('INVALID_INPUT:')


def test_example_rerun_is_concise_and_immutable(tmp_path):
    import hashlib
    env = dict(os.environ, PYTHONPATH=str(Path(cli.__file__).parents[1]))
    args = [sys.executable, '-m', 'parquet_decision.examples', 'sources']
    first = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    root = tmp_path / 'sources'
    original = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
    again = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True)
    assert again.returncode == 1
    text = again.stdout + again.stderr
    assert 'destination already exists' in text and 'preserved' in text and 'choose a new' in text
    assert 'Traceback' not in text and len(text) < 512
    assert original == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
