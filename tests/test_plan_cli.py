"""Newcomer-readable initial plan/inspect/decide commands, no apply yet."""
from importlib import import_module, util
import json
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.fixtures import model, plan
from tests.test_decisions import conflict_plan, choices, decisions


def cli():
    assert util.find_spec('parquet_decision.cli') is not None, 'development CLI is missing'
    return import_module('parquet_decision.cli')


def write_source(root, name, metadata=None):
    root.mkdir(exist_ok=True)
    pq.write_table(pa.Table.from_arrays([pa.array([1], type=pa.int64())],
        schema=pa.schema([pa.field('x', pa.int64())], metadata=metadata)), root / name)


def test_inspect_honest_copy():
    p, d = conflict_plan(), decisions()
    text = d.render_plan(p)
    disclaimer = 'Metadata provenance is preserved. Scientific semantic compatibility is not established.'
    assert disclaimer in text
    assert text.index(disclaimer) < text.index('Approval instructions')
    assert p.plan_digest in text
    for operation in p.proposal.operations:
        assert 'Operation ' + model().canonical_json(operation.to_dict()).decode() in text
    for conflict in p.proposal.conflicts:
        assert 'Conflict ' + model().canonical_json(conflict.to_dict()).decode() in text
        assert conflict.conflict_digest in text
    assert '["literal.dot"]' in text and '["obj","x"]' in text
    assert 'CHOOSE_archive_only_OR_abort' in text
    assert '"plan_digest": "' + p.plan_digest + '"' in text
    assert text.index('Target field ["literal.dot"]') < text.index('Target field ["obj"]')
    assert 'Preflight complete: true' in text


def test_plan_inspect_decide_cli(tmp_path, capsys):
    c, m = cli(), model()
    root = tmp_path / 'sources'
    write_source(root, 'a.parquet', {b'producer': b'old'})
    write_source(root, 'b.parquet', {b'producer': b'new'})
    original = {p.name: p.read_bytes() for p in root.iterdir()}
    output = tmp_path / 'plan.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--input', 'b.parquet', '--out', str(output)]) == 1
    assert 'NEEDS_DECISIONS' in capsys.readouterr().out
    p = m.load_plan(output, m.Limits())
    assert c.main(['inspect', str(output)]) == 0
    inspect = capsys.readouterr().out
    assert p.plan_digest in inspect and p.proposal.conflicts[0].conflict_digest in inspect
    assert 'Plan status: NEEDS_DECISIONS' in inspect and 'INSPECTED' in inspect
    decision_file = tmp_path / 'choices.json'
    decision_file.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': p.plan_digest,
        'resolutions': [r.to_dict() for r in choices(p)]}))
    approved = tmp_path / 'approved.json'
    assert c.main(['decide', str(output), '--decisions', str(decision_file), '--out', str(approved)]) == 0
    assert 'APPROVED' in capsys.readouterr().out
    assert m.load_approved_plan(approved, m.Limits()).plan == p
    assert original == {p.name: p.read_bytes() for p in root.iterdir()}
    assert c.main(['inspect', str(approved)]) == 0
    assert 'Stored approval: APPROVED' in capsys.readouterr().out


@pytest.mark.parametrize('raw', ['./a.parquet', 'sub//a.parquet', 'a.parquet/', '../a.parquet', '/a.parquet', 'a\\b.parquet'])
def test_cli_checks_raw_input_before_path_normalization(raw, tmp_path, capsys):
    c = cli()
    root = tmp_path / 'sources'; root.mkdir()
    output = tmp_path / 'plan.json'
    assert c.main(['plan', '--sources', str(root), '--input', raw, '--out', str(output)]) == 2
    assert 'INVALID_INPUT' in capsys.readouterr().out
    assert not output.exists()


def test_cli_output_cannot_overwrite_input_artifacts_or_sources(tmp_path, capsys):
    c, m = cli(), model()
    root = tmp_path / 'sources'
    write_source(root, 'a.parquet')
    source = root / 'a.parquet'; original = source.read_bytes()
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--out', str(source)]) == 1
    assert 'CONFLICT' in capsys.readouterr().out
    assert source.read_bytes() == original
    p = plan(); path = tmp_path / 'plan.json'; path.write_bytes(m.dump_canonical(p))
    decision_file = tmp_path / 'choices.json'; decision_file.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': p.plan_digest, 'resolutions': []}))
    for output in (path, decision_file):
        before = output.read_bytes()
        assert c.main(['decide', str(path), '--decisions', str(decision_file), '--out', str(output)]) == 1
        assert 'CONFLICT' in capsys.readouterr().out
        assert output.read_bytes() == before


def test_cli_source_root_protects_missing_sources_for_decide(tmp_path, capsys):
    c, m = cli(), model()
    root = tmp_path / 'sources'; root.mkdir()
    p = plan(); path = tmp_path / 'plan.json'; path.write_bytes(m.dump_canonical(p))
    decision_file = tmp_path / 'choices.json'; decision_file.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': p.plan_digest, 'resolutions': []}))
    output = root / p.sources[0].relative_path
    assert c.main(['decide', str(path), '--sources', str(root), '--decisions', str(decision_file), '--out', str(output)]) == 1
    assert not output.exists()
    assert 'CONFLICT' in capsys.readouterr().out


@pytest.mark.parametrize('status,expected', [('UNSUPPORTED', 2), ('LIMIT_EXCEEDED', 3), ('DATA_INCOMPATIBLE', 1), ('INCOMPLETE', 3), ('ERROR', 4), ('CANCELLED', 3)])
def test_cli_failed_preflight_cannot_be_approved(status, expected, tmp_path, capsys):
    c, m = cli(), model()
    p = m.Plan(status)
    path = tmp_path / 'failed.json'; path.write_bytes(m.dump_canonical(p))
    decision_file = tmp_path / 'choices.json'; decision_file.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': p.plan_digest, 'resolutions': []}))
    output = tmp_path / 'approved.json'
    assert c.main(['inspect', str(path)]) == 0
    assert status in capsys.readouterr().out
    assert c.main(['decide', str(path), '--decisions', str(decision_file), '--out', str(output)]) == expected
    assert status in capsys.readouterr().out
    assert not output.exists()


def test_cli_abort_and_stale_decisions_remain_conflicts(tmp_path, capsys):
    c, m = cli(), model()
    p = conflict_plan(); path = tmp_path / 'plan.json'; path.write_bytes(m.dump_canonical(p))
    decision_file = tmp_path / 'choices.json'
    for digest, resolutions in [(p.plan_digest, choices(p, 'abort')), ('0' * 64, choices(p))]:
        decision_file.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': digest, 'resolutions': [r.to_dict() for r in resolutions]}))
        assert c.main(['decide', str(path), '--decisions', str(decision_file), '--out', str(tmp_path / 'approved.json')]) == 1
        assert 'CONFLICT' in capsys.readouterr().out
        assert not (tmp_path / 'approved.json').exists()


def test_complete_cli_is_native_free():
    c = cli()
    result = subprocess.run([sys.executable, '-c', 'import sys; import parquet_decision.cli; assert "pyarrow" not in sys.modules'],
                            env={'PYTHONPATH': str(Path(c.__file__).parents[1])}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    result = subprocess.run([sys.executable, '-m', 'parquet_decision.cli', '--help'],
                            env={'PYTHONPATH': str(Path(c.__file__).parents[1])}, capture_output=True, text=True)
    assert result.returncode == 0
    assert '{plan,inspect,decide,apply,verify,origin}' in result.stdout


def test_cli_declarative_limits_can_only_lower_and_are_protected(tmp_path, capsys):
    c, m = cli(), model()
    root = tmp_path / 'sources'
    write_source(root, 'a.parquet'); write_source(root, 'b.parquet')
    limits_path = tmp_path / 'limits.json'
    limits_path.write_bytes(m.canonical_json(m.Limits(max_files=1).to_dict()))
    original = limits_path.read_bytes()
    output = tmp_path / 'limited.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--input', 'b.parquet', '--limits', str(limits_path), '--out', str(output)]) == 3
    assert 'LIMIT_EXCEEDED' in capsys.readouterr().out
    p = m.load_plan(output, m.Limits())
    assert p.limits.max_files == 1 and not p.preflight_complete
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--limits', str(limits_path), '--out', str(limits_path)]) == 1
    assert 'CONFLICT' in capsys.readouterr().out
    assert limits_path.read_bytes() == original
    invalid = m.Limits().to_dict(); invalid['max_files'] = 129
    limits_path.write_bytes(m.canonical_json(invalid))
    output = tmp_path / 'invalid.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--limits', str(limits_path), '--out', str(output)]) == 3
    assert 'LIMIT_EXCEEDED' in capsys.readouterr().out and not output.exists()


def test_cli_missing_source_returns_incomplete_review_artifact(tmp_path, capsys):
    c, m = cli(), model()
    root = tmp_path / 'sources'; root.mkdir()
    path = tmp_path / 'missing.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'missing.parquet', '--out', str(path)]) == 3
    assert 'INCOMPLETE' in capsys.readouterr().out
    assert m.load_plan(path, m.Limits()).status == 'INCOMPLETE'


def test_cli_duplicate_raw_identifiers_are_invalid_before_worker(tmp_path, capsys):
    c = cli()
    root = tmp_path / 'sources'; write_source(root, 'a.parquet')
    output = tmp_path / 'plan.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--input', 'a.parquet', '--out', str(output)]) == 2
    assert 'INVALID_INPUT' in capsys.readouterr().out and not output.exists()


def test_cli_plan_failure_displays_bounded_diagnostic(tmp_path, capsys):
    c = cli()
    root = tmp_path / 'missing-source-root'
    output = tmp_path / 'plan.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--out', str(output)]) == 3
    text = capsys.readouterr().out
    assert 'INCOMPLETE' in text and 'Diagnostic:' in text and 'missing-source-root' in text


def test_cli_unavailable_publication_capability_maps_to_unsupported(tmp_path, monkeypatch, capsys):
    c, m = cli(), model()
    a = import_module('parquet_decision.artifacts')
    root = tmp_path / 'sources'; write_source(root, 'a.parquet')
    def unavailable():
        raise m.UnsupportedError('renameat2 export unavailable')
    monkeypatch.setattr(a, '_renameat2_function', unavailable)
    output = tmp_path / 'plan.json'
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--out', str(output)]) == 2
    assert 'UNSUPPORTED' in capsys.readouterr().out and not output.exists()
    assert set(tmp_path.iterdir()) == {root}


@pytest.mark.parametrize('kind', ['symlink', 'hardlink'])
def test_cli_aliased_source_output_is_preserved(kind, tmp_path, capsys):
    import os
    c = cli()
    root = tmp_path / 'sources'; write_source(root, 'a.parquet')
    source = root / 'a.parquet'; before = source.read_bytes()
    output = tmp_path / 'alias'
    if kind == 'symlink':
        output.symlink_to(source)
    else:
        os.link(source, output)
    assert c.main(['plan', '--sources', str(root), '--input', 'a.parquet', '--out', str(output)]) == 1
    assert 'CONFLICT' in capsys.readouterr().out
    assert source.read_bytes() == before and output.read_bytes() == before


def test_inspect_explains_conflict_bytes_without_guessing_semantics():
    p = conflict_plan()
    text = decisions().render_plan(p)
    assert 'Metadata choice at ["literal.dot"]: key UTF-8 "unit"' in text
    assert 'source 0: UTF-8 "m"' in text and 'source 1: UTF-8 "cm"' in text
    assert 'Action: source 0 ["literal.dot"] identity int64 -> int64' in text
    assert 'Source 0: "source-0.parquet"; rows 1; bytes 10' in text
    assert 'Scientific semantic compatibility is not established.' in text


@pytest.mark.parametrize('missing', [False, True])
def test_inspect_invalid_or_missing_artifact_is_a_failure(missing, tmp_path, capsys):
    c = cli()
    path = tmp_path / 'input.json'
    if not missing:
        path.write_bytes(b'{"ordinary":"incomplete record"}')
    assert c.main(['inspect', str(path)]) == 2
    assert 'INVALID_INPUT' in capsys.readouterr().out


def test_inspect_opaque_and_absent_metadata_previews_remain_exact():
    from tests.fixtures import field, source, schema
    m = model()
    sources = (source(0, (field(metadata=((b'\xff', b'\xfe'), (b'empty', b''))),)),
               source(1, (field(metadata=((b'\xff', b'\x00'),)),)))
    p = m.Plan('NEEDS_DECISIONS', sources, schema().plan_schema(list(sources)), True,
               preflight_counters=m.PreflightCounters(2, 2, 20))
    text = decisions().render_plan(p)
    assert 'key base64 "/w=="' in text
    assert 'source 0: base64 "/g=="' in text
    assert 'source 1: UTF-8 "\\u0000"' in text
    assert 'source 0: UTF-8 ""; source 1: absent' in text
    for conflict in p.proposal.conflicts:
        assert 'Conflict ' + m.canonical_json(conflict.to_dict()).decode() in text


def approval_input_files(tmp_path):
    """Original strict declarative inputs, without a native fixture dependency."""
    m = model()
    p = plan()
    plan_path, decision_path = tmp_path / 'reviewed.json', tmp_path / 'explicit.json'
    plan_path.write_bytes(m.dump_canonical(p))
    decision_path.write_bytes(m.canonical_json({'format_version': 1, 'plan_digest': p.plan_digest, 'resolutions': []}))
    return plan_path, decision_path


@pytest.mark.parametrize('unbuffered', [False, True])
@pytest.mark.parametrize('failure', ['closed_pipe', 'dev_full'])
def test_real_post_commit_stdout_failure_keeps_exit_and_safe_retry(failure, unbuffered, tmp_path):
    import os
    c, m = cli(), model()
    plan_path, decision_path = approval_input_files(tmp_path)
    destination = tmp_path / 'committed-approval.json'
    command = [sys.executable, *(['-u'] if unbuffered else []), '-m', 'parquet_decision.cli',
               'decide', str(plan_path), '--decisions', str(decision_path), '--out', str(destination)]
    env = dict(os.environ, PYTHONPATH=str(Path(c.__file__).parents[1]))
    if failure == 'closed_pipe':
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.stdout.close()
        stderr = process.stderr.read().decode()
        actual = process.wait(timeout=10)
        expected, status = 4, 'ERROR'
    else:
        # Linux's standard test device never stores bytes or fills a filesystem.
        with open('/dev/full', 'wb') as full:
            process = subprocess.run(command, env=env, stdout=full, stderr=subprocess.PIPE, timeout=10)
        actual, stderr = process.returncode, process.stderr.decode()
        expected, status = 3, 'LIMIT_EXCEEDED'
    assert m.load_approved_plan(destination, m.Limits()).status == 'APPROVED'
    before = destination.read_bytes()
    assert actual == expected, (actual, stderr)
    assert status in stderr and 'artifact already published:' in stderr and str(destination) in stderr
    assert 'Traceback' not in stderr and 'Exception ignored' not in stderr
    retry = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
    assert retry.returncode == 1 and 'CONFLICT' in retry.stdout
    assert destination.read_bytes() == before


@pytest.mark.parametrize('route', ['plan_limits', 'inspect_plan', 'inspect_approval', 'decide_plan', 'decide_decisions'])
@pytest.mark.parametrize('failure,expected', [('ENOMEM', 3), ('ENOSPC', 3), ('EDQUOT', 3), ('EIO', 4), ('MemoryError', 3)])
def test_cli_json_read_preserves_typed_operational_failures(route, failure, expected, tmp_path, monkeypatch, capsys):
    import errno
    c, m = cli(), model()
    plan_path, decision_path = approval_input_files(tmp_path)
    limits_path = tmp_path / 'limits.json'; limits_path.write_bytes(m.canonical_json(m.Limits().to_dict()))
    approved_path = tmp_path / 'stored-approved.json'; approved_path.write_bytes(m.dump_canonical(m.ApprovedPlan(plan(), ())))
    destination = tmp_path / 'result.json'
    root = tmp_path / 'sources'; root.mkdir()
    if route == 'plan_limits':
        target = limits_path
        args = ['plan', '--sources', str(root), '--input', 'a.parquet', '--limits', str(target), '--out', str(destination)]
    elif route.startswith('inspect'):
        target = plan_path if route == 'inspect_plan' else approved_path
        args = ['inspect', str(target)]
    else:
        target = plan_path if route == 'decide_plan' else decision_path
        args = ['decide', str(plan_path), '--decisions', str(decision_path), '--out', str(destination)]
    originals = {path: path.read_bytes() for path in (plan_path, decision_path, limits_path, approved_path)}
    real_open = Path.open
    def read_fault(path, *args, **kwargs):
        if path == target:
            if failure == 'MemoryError':
                raise MemoryError('controlled ordinary allocation refusal')
            raise OSError(getattr(errno, failure), 'controlled ordinary JSON read failure')
        return real_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', read_fault)
    assert c.main(args) == expected
    text = capsys.readouterr().out
    assert ('LIMIT_EXCEEDED' if expected == 3 else 'ERROR') in text
    assert 'INVALID_INPUT' not in text and not destination.exists()
    monkeypatch.setattr(Path, 'open', real_open)
    assert originals == {path: path.read_bytes() for path in originals}


def test_inspect_stored_approval_is_labeled_before_embedded_plan(tmp_path, capsys):
    c, m = cli(), model()
    path = tmp_path / 'approved.json'
    path.write_bytes(m.dump_canonical(m.ApprovedPlan(plan(), ())))
    assert c.main(['inspect', str(path)]) == 0
    text = capsys.readouterr().out
    assert text.index('Stored approval: APPROVED') < text.index('Plan status:')
    assert text.index('Stored approval is not fresh source-bound verification.') < text.index('Approval instructions:')
