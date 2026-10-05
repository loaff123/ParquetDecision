"""Original byte fixtures test source identity without native decoding."""
import hashlib
from dataclasses import replace
from importlib import import_module, util
from pathlib import Path

import pytest

from parquet_decision.model import ApprovedPlan, Limits, ModelError, LimitError, Plan, PreflightCounters, SourceSpec
from parquet_decision.schema import plan_schema


def sources_module():
    assert util.find_spec('parquet_decision.source') is not None, 'source snapshots are missing'
    return import_module('parquet_decision.source')


def inventory(root, names=('a.parquet', 'sub/b.parquet')):
    result = []
    for i, name in enumerate(names):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f'original benign bytes {i}'.encode())
        data = path.read_bytes()
        result.append(SourceSpec(i, name, hashlib.sha256(data).hexdigest(), len(data), i + 1, ()))
    return result


def test_snapshot_exact_identity(tmp_path):
    root = tmp_path / 'sources'
    sources = inventory(root)
    staging = tmp_path / 'staging'
    staging.mkdir()
    snapshots = sources_module().snapshot_inventory(sources, root, staging, Limits())
    assert [s.source for s in snapshots] == sources
    for snapshot in snapshots:
        assert snapshot.local_path != root / snapshot.source.relative_path
        assert snapshot.local_path.read_bytes() == (root / snapshot.source.relative_path).read_bytes()
        assert hashlib.sha256(snapshot.local_path.read_bytes()).hexdigest() == snapshot.source.sha256
        assert snapshot.local_path.stat().st_mode & 0o222 == 0


def test_snapshot_approved_plan_adapter(tmp_path):
    root = tmp_path / 'sources'
    sources = inventory(root)
    plan = Plan('READY_FOR_REVIEW', tuple(sources), plan_schema(sources), True,
                preflight_counters=PreflightCounters(2, 3, sum(s.size_bytes for s in sources)))
    staging = tmp_path / 'stage'; staging.mkdir()
    assert [s.source for s in sources_module().snapshot_sources(ApprovedPlan(plan, ()), root, staging)] == sources


@pytest.mark.parametrize('change', ['bytes', 'symlink', 'parent_symlink', 'hardlink', 'duplicate'])
def test_snapshot_refuses_changed_or_aliased_source(tmp_path, change):
    root = tmp_path / 'sources'
    sources = inventory(root)
    a, b = root / sources[0].relative_path, root / sources[1].relative_path
    if change == 'bytes':
        a.write_bytes(b'changed')
    elif change == 'symlink':
        a.unlink(); a.symlink_to(b)
    elif change == 'parent_symlink':
        (root / 'sub').rename(root / 'actual'); (root / 'sub').symlink_to(root / 'actual', target_is_directory=True)
    elif change == 'hardlink':
        b.unlink(); b.hardlink_to(a)
    else:
        sources[1] = replace(sources[0], source_id=1)
    staging = tmp_path / 'stage'; staging.mkdir()
    with pytest.raises(ModelError):
        sources_module().snapshot_inventory(sources, root, staging, Limits())
    assert not list(staging.iterdir())


@pytest.mark.parametrize('cap', ['max_input_bytes', 'max_staging_bytes', 'max_rows', 'max_files'])
def test_snapshot_limits_count_exactly(tmp_path, cap):
    root = tmp_path / 'sources'; sources = inventory(root)
    totals = {'max_input_bytes': sum(s.size_bytes for s in sources),
              'max_staging_bytes': sum(s.size_bytes for s in sources), 'max_rows': 3, 'max_files': 2}
    staging = tmp_path / 'stage'; staging.mkdir()
    assert len(sources_module().snapshot_inventory(sources, root, staging, replace(Limits(), **{cap: totals[cap]}))) == 2
    staging2 = tmp_path / 'stage2'; staging2.mkdir()
    with pytest.raises(LimitError):
        sources_module().snapshot_inventory(sources, root, staging2, replace(Limits(), **{cap: totals[cap] - 1}))
    assert not list(staging2.iterdir())


def test_snapshot_counts_existing_staging_and_preserves_it(tmp_path):
    root = tmp_path / 'sources'; sources = inventory(root)
    staging = tmp_path / 'stage'; staging.mkdir(); (staging / 'owned.txt').write_bytes(b'123')
    limit = replace(Limits(), max_staging_bytes=sum(s.size_bytes for s in sources) + 2)
    with pytest.raises(LimitError):
        sources_module().snapshot_inventory(sources, root, staging, limit)
    assert (staging / 'owned.txt').read_bytes() == b'123'
    assert list(staging.iterdir()) == [staging / 'owned.txt']


def test_source_identifiers_are_relative_and_native_iterators_require_workers(tmp_path):
    module = sources_module()
    root = tmp_path / 'sources'; sources = inventory(root)
    for name in ('/outside.parquet', '../outside.parquet', 'sub/../a.parquet'):
        with pytest.raises(ModelError):
            module._snapshot_inputs([Path(name)], root, tmp_path, Limits())
    staging = tmp_path / 'stage'; staging.mkdir()
    snapshot = module.snapshot_inventory(sources[:1], root, staging, Limits())[0]
    with pytest.raises(ModelError, match='worker'):
        list(module.iter_source_batches(snapshot, 2))


def test_staging_in_source_root_is_refused(tmp_path):
    root = tmp_path / 'sources'; sources = inventory(root)
    stage = root / 'stage'; stage.mkdir()
    with pytest.raises(ModelError):
        sources_module().snapshot_inventory(sources, root, stage, Limits())
    assert not list(stage.iterdir())


def test_snapshot_detects_quiescence_change_and_cleans_only_owned_files(tmp_path, monkeypatch):
    module = sources_module(); root = tmp_path / 'sources'; sources = inventory(root)
    stage = tmp_path / 'stage'; stage.mkdir(); keep = stage / 'keep.txt'; keep.write_bytes(b'keep')
    original_read = module.os.read
    changed = False
    def read_and_change(fd, size):
        nonlocal changed
        chunk = original_read(fd, size)
        if chunk and not changed:
            changed = True
            (root / 'a.parquet').write_bytes(b'changed during copy')
        return chunk
    monkeypatch.setattr(module.os, 'read', read_and_change)
    with pytest.raises(ModelError, match='quiescent'):
        module.snapshot_inventory(sources, root, stage, Limits())
    assert list(stage.iterdir()) == [keep]
    assert keep.read_bytes() == b'keep'


def test_snapshot_rejects_symlink_root_and_staging(tmp_path):
    module = sources_module(); root = tmp_path / 'sources'; sources = inventory(root)
    linked = tmp_path / 'linked'; linked.symlink_to(root, target_is_directory=True)
    stage = tmp_path / 'stage'; stage.mkdir()
    with pytest.raises(ModelError):
        module.snapshot_inventory(sources, linked, stage, Limits())
    linked_stage = tmp_path / 'linked-stage'; linked_stage.symlink_to(stage, target_is_directory=True)
    with pytest.raises(ModelError):
        module.snapshot_inventory(sources, root, linked_stage, Limits())


def test_snapshot_preserves_recognized_os_allocation_failure(tmp_path, monkeypatch):
    import errno
    module = sources_module(); root = tmp_path / 'sources'; sources = inventory(root)
    stage = tmp_path / 'stage'; stage.mkdir()
    def allocation_refusal(*args, **kwargs):
        raise OSError(errno.ENOMEM, 'original controlled allocation refusal')
    monkeypatch.setattr(module.os, 'open', allocation_refusal)
    with pytest.raises(OSError) as exception:
        with module._open_relative(root, sources[0].relative_path):
            pass
    assert exception.value.errno == errno.ENOMEM


def test_directory_canonicalization_keeps_staging_overlap_protection(tmp_path):
    module = sources_module(); root = tmp_path / 'sources'; sources = inventory(root)
    (root / 'inner').mkdir(); stage = root / 'stage'; stage.mkdir()
    with pytest.raises(ModelError, match='outside'):
        module.snapshot_inventory(sources, root / 'inner' / '..', stage, Limits())
