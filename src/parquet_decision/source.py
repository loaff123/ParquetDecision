"""Streamed, hash-bound snapshots of trusted quiescent local sources.

Copying is native-free. Native batch iteration is a qualified-worker primitive,
not an arbitrary caller Arrow-object input API or a hostile-file sanitizer.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import errno
import os
from pathlib import Path
import shutil
import stat
import tempfile
from typing import Iterator

from .model import (ApprovedPlan, Batch, Limits, LimitError, ModelError, Snapshot,
                    SourceSpec, _relative_path, validate_inventory)

_COPY_BYTES = 1024 * 1024


@dataclass(frozen=True)
class _SnapshotBytes:
    source_id: int
    relative_path: str
    sha256: str
    size_bytes: int
    local_path: Path


def _directory(path: Path) -> Path:
    if not isinstance(path, Path):
        raise ModelError('local directory must be a Path')
    absolute = path.absolute()
    # Reject symlinks in every supplied directory component, rather than resolve
    # them and silently authorize a different tree.
    cursor = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        cursor /= component
        mode = cursor.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise ModelError('directory components must be real directories, not symlinks')
    return absolute.resolve(strict=True)


@contextmanager
def _open_relative(root: Path, relative_path: str):
    _relative_path(relative_path)
    descriptors = []
    try:
        descriptors.append(os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        parts = relative_path.split('/')
        for component in parts[:-1]:
            descriptors.append(os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptors[-1])
        descriptors.append(fd)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ModelError('source must be a regular file')
        yield fd
    except FileNotFoundError:
        raise
    except OSError as exc:
        if exc.errno in (errno.ENOMEM, errno.ENOSPC, errno.EDQUOT):
            raise
        raise ModelError(f'source path refused: {exc.strerror}') from exc
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _staging_bytes(staging: Path, limits: Limits) -> int:
    total = 0
    for directory, directories, filenames in os.walk(staging, followlinks=False):
        for name in (*directories, *filenames):
            path = Path(directory) / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise ModelError('owned staging must contain only real directories and files')
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
                if total > limits.max_staging_bytes:
                    raise LimitError(f'owned staging bytes {total} exceed cap {limits.max_staging_bytes}')
    return total


def _snapshot_inputs(inputs: list[Path], source_root: Path, staging: Path, limits: Limits,
                     expected: list[SourceSpec] | None = None) -> list[_SnapshotBytes]:
    """Private byte inventory used before source schemas/row counts are known."""
    if not isinstance(limits, Limits) or type(inputs) is not list or not inputs:
        raise ModelError('snapshot inputs need a nonempty ordered Path list and Limits')
    if len(inputs) > limits.max_files:
        raise LimitError(f'input files {len(inputs)} exceed cap {limits.max_files}')
    names = []
    for path in inputs:
        if not isinstance(path, Path):
            raise ModelError('input identifier must be a relative Path')
        names.append(_relative_path(str(path)))
    if len(set(names)) != len(names):
        raise ModelError('duplicate input identifier')
    root, stage = _directory(source_root), _directory(staging)
    if stage == root or stage.is_relative_to(root):
        raise ModelError('owned staging must be outside the source root')
    if expected is not None:
        validate_inventory(tuple(expected), limits)
        if names != [s.relative_path for s in expected]:
            raise ModelError('snapshot inventory identity differs')
    baseline = _staging_bytes(stage, limits)
    owned = Path(tempfile.mkdtemp(prefix='snapshots-', dir=stage))
    total = 0
    seen = set()
    snapshots = []
    try:
        for source_id, name in enumerate(names):
            destination = owned / f'source-{source_id:05d}.parquet'
            with _open_relative(root, name) as fd:
                before = os.fstat(fd)
                if (before.st_dev, before.st_ino) in seen:
                    raise ModelError('duplicate input file identity (hard-link alias)')
                seen.add((before.st_dev, before.st_ino))
                if total + before.st_size > limits.max_input_bytes:
                    raise LimitError(f'input bytes {total + before.st_size} exceed cap {limits.max_input_bytes}')
                if baseline + total + before.st_size > limits.max_staging_bytes:
                    raise LimitError(f'owned staging bytes {baseline + total + before.st_size} exceed cap {limits.max_staging_bytes}')
                digest, size = hashlib.sha256(), 0
                with destination.open('xb') as output:
                    while chunk := os.read(fd, _COPY_BYTES):
                        measured = total + size + len(chunk)
                        if measured > limits.max_input_bytes:
                            raise LimitError(f'input bytes {measured} exceed cap {limits.max_input_bytes}')
                        if baseline + measured > limits.max_staging_bytes:
                            raise LimitError(f'owned staging bytes {baseline + measured} exceed cap {limits.max_staging_bytes}')
                        output.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                    output.flush()
                after = os.fstat(fd)
                if _identity(before) != _identity(after) or size != before.st_size:
                    raise ModelError('source is not quiescent while snapshotting')
                # A replacement at the selected path is also a changed source,
                # even when our original descriptor still references its bytes.
                with _open_relative(root, name) as current:
                    if _identity(os.fstat(current)) != _identity(before):
                        raise ModelError('source path changed while snapshotting')
            hexdigest = digest.hexdigest()
            if expected is not None and (size, hexdigest) != (expected[source_id].size_bytes, expected[source_id].sha256):
                raise ModelError('source bytes differ from the approved hash/size')
            destination.chmod(0o400)
            total += size
            snapshots.append(_SnapshotBytes(source_id, name, hexdigest, size, destination))
        return snapshots
    except BaseException:
        # Only the subdirectory created by this call is removed. Existing staging
        # and the originals are never edited or cleaned up here.
        shutil.rmtree(owned)
        raise


def snapshot_inventory(sources: list[SourceSpec], source_root: Path, staging: Path,
                       limits: Limits) -> list[Snapshot]:
    if type(sources) is not list or not sources:
        raise ModelError('snapshot inventory requires a nonempty SourceSpec list')
    validate_inventory(tuple(sources), limits)
    copied = _snapshot_inputs([Path(s.relative_path) for s in sources], source_root, staging, limits, sources)
    return [Snapshot(source, copy.local_path) for source, copy in zip(sources, copied, strict=True)]


def snapshot_sources(plan: ApprovedPlan, source_root: Path, staging: Path) -> list[Snapshot]:
    if not isinstance(plan, ApprovedPlan):
        raise ModelError('snapshots require an ApprovedPlan')
    return snapshot_inventory(list(plan.plan.sources), source_root, staging, plan.plan.limits)


@contextmanager
def _open_snapshot(snapshot: Snapshot):
    """Check snapshot bytes on the same descriptor subsequently given to Arrow."""
    if not isinstance(snapshot, Snapshot) or snapshot.local_path is None:
        raise ModelError('batch reading requires a local Snapshot')
    path = snapshot.local_path
    _directory(path.parent)
    with _open_relative(path.parent, path.name) as fd:
        before = os.fstat(fd)
        digest, size = hashlib.sha256(), 0
        while chunk := os.read(fd, _COPY_BYTES):
            size += len(chunk)
            if size > snapshot.source.size_bytes:
                raise ModelError('snapshot size changed')
            digest.update(chunk)
        if size != snapshot.source.size_bytes or digest.hexdigest() != snapshot.source.sha256:
            raise ModelError('snapshot hash/size mismatch')
        if _identity(before) != _identity(os.fstat(fd)):
            raise ModelError('snapshot changed during hash validation')
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(fd), 'rb') as stream:
            yield stream
        if _identity(before) != _identity(os.fstat(fd)):
            raise ModelError('snapshot is not quiescent during native reading')


def iter_source_batches(snapshot: Snapshot, batch_rows: int) -> Iterator[Batch]:
    """Stream and reconcile every row of a checked snapshot inside a worker."""
    from .supervise import _require_worker
    _require_worker()
    if type(batch_rows) is not int or not 1 <= batch_rows <= Limits().batch_rows:
        raise ModelError('batch_rows must be an integer in the supported range')
    import pyarrow.parquet as pq
    from .model import _fields_from_arrow
    with _open_snapshot(snapshot) as stream:
        reader = pq.ParquetFile(stream, arrow_extensions_enabled=False, pre_buffer=False)
        fields, metadata = _fields_from_arrow(reader.schema_arrow)
        if (fields, metadata, reader.metadata.num_rows) != (snapshot.source.fields, snapshot.source.schema_metadata, snapshot.source.num_rows):
            raise ModelError('snapshot schema/row identity differs from source inventory')
        first_row = 0
        for batch in reader.iter_batches(batch_size=batch_rows, use_threads=False):
            if batch.num_rows > batch_rows or first_row + batch.num_rows > snapshot.source.num_rows:
                raise ModelError('source batch counters exceed declared inventory')
            yield Batch(snapshot.source.source_id, first_row, batch)
            first_row += batch.num_rows
        if first_row != snapshot.source.num_rows:
            raise ModelError('source rows do not reconcile snapshot inventory')
