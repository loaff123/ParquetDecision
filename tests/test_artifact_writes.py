"""Real Linux no-replace publication on this destination filesystem."""
from importlib import import_module, util
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tests.fixtures import model, plan


def artifacts():
    assert util.find_spec('parquet_decision.artifacts') is not None, 'qualified no-replace artifact publication is missing'
    return import_module('parquet_decision.artifacts')


@pytest.mark.parametrize('record', ['plan', 'approval', 'report'])
def test_json_no_clobber_roundtrip(record, tmp_path):
    a, m = artifacts(), model()
    value = plan() if record == 'plan' else m.ApprovedPlan(plan(), ()) if record == 'approval' else m.VerificationReport('INCOMPLETE')
    destination = tmp_path / f'{record}.json'
    a.write_json_exclusive(value, destination, [])
    assert m.parse_json(destination.read_bytes()) == value.to_dict()
    assert set(tmp_path.iterdir()) == {destination}


@pytest.mark.parametrize('kind', ['file', 'directory', 'symlink', 'dangling_symlink', 'hardlink'])
def test_json_no_clobber_existing_targets_are_unchanged(kind, tmp_path):
    a = artifacts()
    protected = tmp_path / 'input'
    protected.write_bytes(b'original source bytes')
    output = tmp_path / 'output'
    if kind == 'file':
        output.write_bytes(b'old artifact')
    elif kind == 'directory':
        output.mkdir(); (output / 'owned-by-user').write_bytes(b'existing')
    elif kind == 'hardlink':
        os.link(protected, output)
    else:
        output.symlink_to(protected if kind == 'symlink' else tmp_path / 'absent')
    before = output.lstat()
    with pytest.raises(a.ArtifactConflictError):
        a.write_json_exclusive(plan(), output, [protected])
    assert output.lstat() == before
    assert protected.read_bytes() == b'original source bytes'
    if kind == 'file':
        assert output.read_bytes() == b'old artifact'
    if kind == 'directory':
        assert (output / 'owned-by-user').read_bytes() == b'existing'


def test_json_no_clobber_protects_missing_input_and_input_artifact_names(tmp_path):
    a = artifacts()
    protected = tmp_path / 'source.parquet'
    for destination in (protected, tmp_path / 'a' / '..' / 'source.parquet'):
        with pytest.raises(a.ArtifactConflictError, match='protected'):
            a.write_json_exclusive(plan(), destination, [protected])
    assert not protected.exists()
    artifact = tmp_path / 'plan.json'
    artifact.write_bytes(b'receipt bytes')
    with pytest.raises(a.ArtifactConflictError):
        a.write_json_exclusive(plan(), artifact, [artifact])
    assert artifact.read_bytes() == b'receipt bytes'


def test_json_no_clobber_rejects_symlink_parent_alias(tmp_path):
    a = artifacts()
    root = tmp_path / 'real'; root.mkdir()
    alias = tmp_path / 'alias'; alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(model().ModelError, match='symlink'):
        a.write_json_exclusive(plan(), alias / 'new.json', [])
    assert not (root / 'new.json').exists()


def test_publication_does_not_create_missing_parent(tmp_path):
    a = artifacts()
    with pytest.raises(FileNotFoundError):
        a.write_json_exclusive(plan(), tmp_path / 'new' / 'plan.json', [])
    assert not (tmp_path / 'new').exists()


def test_actual_destination_fs_qualifies_file_and_directory_noreplace(tmp_path):
    a = artifacts()
    a.qualify_noreplace(tmp_path)
    assert list(tmp_path.iterdir()) == []
    original = tmp_path / 'stage'; original.mkdir()
    (original / 'token').write_text('complete')
    destination = tmp_path / 'bundle'
    a.publish_directory_exclusive(original, destination, [])
    assert not original.exists()
    assert (destination / 'token').read_text() == 'complete'


@pytest.mark.parametrize('kind', ['file', 'directory', 'symlink'])
def test_directory_noreplace_preserves_existing_target(kind, tmp_path):
    a = artifacts()
    stage = tmp_path / 'stage'; stage.mkdir(); (stage / 'token').write_text('stage')
    destination = tmp_path / 'bundle'
    if kind == 'file':
        destination.write_text('user file')
    elif kind == 'directory':
        destination.mkdir(); (destination / 'token').write_text('user directory')
    else:
        destination.symlink_to(stage, target_is_directory=True)
    before = destination.lstat()
    with pytest.raises(a.ArtifactConflictError):
        a.publish_directory_exclusive(stage, destination, [])
    assert destination.lstat() == before
    assert (stage / 'token').read_text() == 'stage'


@pytest.mark.parametrize('directory', [False, True])
def test_real_competing_creator_cannot_be_overwritten(directory, tmp_path, monkeypatch):
    a = artifacts()
    destination = tmp_path / 'result'
    primitive = a._rename_noreplace
    raced = []
    def race(source, target):
        if Path(target) == destination:
            subprocess.run([sys.executable, '-c',
                'import pathlib,sys; p=pathlib.Path(sys.argv[1]); '
                + ('p.mkdir(); (p/"token").write_bytes(b"competitor")' if directory else 'p.write_bytes(b"competitor")'),
                str(destination)], check=True)
            raced.append(True)
        return primitive(source, target)
    monkeypatch.setattr(a, '_rename_noreplace', race)
    if directory:
        stage = tmp_path / 'stage'; stage.mkdir(); (stage / 'token').write_bytes(b'owned')
        with pytest.raises(a.ArtifactConflictError):
            a.publish_directory_exclusive(stage, destination, [])
        assert (stage / 'token').read_bytes() == b'owned'
        assert (destination / 'token').read_bytes() == b'competitor'
    else:
        with pytest.raises(a.ArtifactConflictError):
            a.write_json_exclusive(plan(), destination, [])
        assert destination.read_bytes() == b'competitor'
        assert set(tmp_path.iterdir()) == {destination}
    assert raced == [True]


def test_directory_output_cannot_enclose_a_protected_input(tmp_path):
    a = artifacts()
    stage = tmp_path / 'stage'; stage.mkdir()
    destination = tmp_path / 'future-source-root'
    with pytest.raises(a.ArtifactConflictError, match='protected'):
        a.publish_directory_exclusive(stage, destination, [destination / 'source.parquet'])
    assert stage.exists() and not destination.exists()


def test_unavailable_noreplace_has_no_unsafe_fallback(tmp_path, monkeypatch):
    a = artifacts()
    def unavailable():
        raise model().UnsupportedError('Linux renameat2(RENAME_NOREPLACE) unavailable')
    monkeypatch.setattr(a, '_renameat2_function', unavailable)
    with pytest.raises(model().UnsupportedError, match='renameat2'):
        a.write_json_exclusive(plan(), tmp_path / 'out.json', [])
    assert list(tmp_path.iterdir()) == []


def test_fsync_failure_never_publishes_and_cleans_only_owned_temp(tmp_path, monkeypatch):
    a = artifacts()
    receipt = tmp_path / 'user-receipt'; receipt.write_bytes(b'keep')
    def fail(fd):
        raise OSError(28, 'original controlled full-device observation')
    monkeypatch.setattr(a.os, 'fsync', fail)
    with pytest.raises(OSError):
        a.write_json_exclusive(plan(), tmp_path / 'output', [])
    assert set(tmp_path.iterdir()) == {receipt}
    assert receipt.read_bytes() == b'keep'


def test_directory_stage_must_be_same_filesystem_real_directory(tmp_path):
    a = artifacts()
    file = tmp_path / 'stage'; file.write_bytes(b'owned')
    with pytest.raises(model().ModelError, match='directory'):
        a.publish_directory_exclusive(file, tmp_path / 'bundle', [])
    assert file.read_bytes() == b'owned'


def test_destination_fs_without_noreplace_is_unsupported_and_cleans_owned_probe(tmp_path, monkeypatch):
    import ctypes
    import errno
    a = artifacts()
    def unsupported(*args):
        ctypes.set_errno(errno.EOPNOTSUPP)
        return -1
    monkeypatch.setattr(a, '_renameat2_function', lambda: unsupported)
    with pytest.raises(model().UnsupportedError, match='filesystem'):
        a.write_json_exclusive(plan(), tmp_path / 'out.json', [])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('directory', [False, True])
@pytest.mark.parametrize('alias', ['symlink', 'hardlink'])
def test_real_competing_input_alias_creator_preserves_source(alias, directory, tmp_path, monkeypatch):
    a = artifacts()
    protected = tmp_path / 'source.parquet'; protected.write_bytes(b'original trusted input')
    destination = tmp_path / 'result'
    primitive = a._rename_noreplace
    def race(source, target):
        if Path(target) == destination:
            action = 'os.symlink' if alias == 'symlink' else 'os.link'
            subprocess.run([sys.executable, '-c',
                f'import os,sys; {action}(sys.argv[1],sys.argv[2])', str(protected), str(destination)], check=True)
        return primitive(source, target)
    monkeypatch.setattr(a, '_rename_noreplace', race)
    if directory:
        stage = tmp_path / 'stage'; stage.mkdir(); (stage / 'token').write_text('owned staging')
        with pytest.raises(a.ArtifactConflictError):
            a.publish_directory_exclusive(stage, destination, [protected])
        assert (stage / 'token').read_text() == 'owned staging'
    else:
        with pytest.raises(a.ArtifactConflictError):
            a.write_json_exclusive(plan(), destination, [protected])
        assert set(tmp_path.iterdir()) == {protected, destination}
    assert protected.read_bytes() == b'original trusted input'
    assert destination.read_bytes() == b'original trusted input'
    assert destination.is_symlink() == (alias == 'symlink')


@pytest.mark.parametrize('directory', [False, True])
def test_static_symlink_parent_cannot_disappear_through_dotdot(directory, tmp_path):
    a = artifacts()
    actual = tmp_path / 'actual'; actual.mkdir(); (actual / 'child').mkdir()
    alias = tmp_path / 'alias'; alias.symlink_to(actual / 'child', target_is_directory=True)
    output = alias / '..' / 'output'
    stage = tmp_path / 'stage'; stage.mkdir(); (stage / 'token').write_bytes(b'owned original staging')
    with pytest.raises(model().ModelError, match='symlink'):
        if directory:
            a.publish_directory_exclusive(stage, output, [])
        else:
            a.write_json_exclusive(plan(), output, [])
    assert not (tmp_path / 'output').exists() and not (actual / 'output').exists()
    assert (stage / 'token').read_bytes() == b'owned original staging'
    assert set(tmp_path.iterdir()) == {actual, alias, stage}


@pytest.mark.parametrize('directory', [False, True])
def test_real_nonsymlink_parent_dotdot_remains_supported(directory, tmp_path):
    a = artifacts()
    child = tmp_path / 'child'; child.mkdir()
    output = child / '..' / 'output'
    if directory:
        stage = tmp_path / 'stage'; stage.mkdir(); (stage / 'token').write_bytes(b'owned complete directory')
        a.publish_directory_exclusive(stage, output, [])
        assert not stage.exists()
        assert (tmp_path / 'output' / 'token').read_bytes() == b'owned complete directory'
    else:
        a.write_json_exclusive(plan(), output, [])
        assert model().parse_json((tmp_path / 'output').read_bytes()) == plan().to_dict()
