"""Qualified Linux no-replace publication for trusted quiescent local parents.

No check-then-overwriting rename fallback exists. Destination parents are real
existing directories; only operation-owned temporaries are cleaned. This is not
hostile-filesystem race isolation or a power-loss durability guarantee.
"""
from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import sys
import tempfile

from .model import (ApprovedPlan, Limits, LimitError, ModelError, Plan,
                    UnsupportedError, VerificationReport, canonical_json)
from .source import _directory


class ArtifactConflictError(ModelError):
    """The intended output exists or overlaps a protected input identity."""


def _renameat2_function():
    if sys.platform != 'linux':
        raise UnsupportedError('Linux renameat2(RENAME_NOREPLACE) is required')
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        function = libc.renameat2
    except AttributeError as exc:
        raise UnsupportedError('exported libc renameat2(RENAME_NOREPLACE) is unavailable') from exc
    function.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    function.restype = ctypes.c_int
    return function


def _rename_noreplace(source: Path, destination: Path) -> None:
    function = _renameat2_function()
    ctypes.set_errno(0)
    if function(-100, os.fsencode(source), -100, os.fsencode(destination), 1) == 0:
        return
    code = ctypes.get_errno()
    if code in (errno.EEXIST, errno.ENOTEMPTY):
        raise ArtifactConflictError(f'destination already exists; preserved: {destination}')
    if code in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP, errno.EXDEV):
        raise UnsupportedError('destination filesystem lacks qualified same-filesystem renameat2(RENAME_NOREPLACE)')
    raise OSError(code, os.strerror(code), str(destination))


def qualify_noreplace(parent: Path) -> None:
    """Prove actual file AND directory no-replace behavior on the target fs."""
    parent = _directory(parent)
    _renameat2_function()  # No temporary creation when the export is absent.
    with tempfile.TemporaryDirectory(prefix='.pdecision-qualify-', dir=parent) as temporary:
        owned = Path(temporary)
        for directory in (False, True):
            source, target = owned / 'source', owned / 'target'
            if directory:
                source.mkdir(); target.mkdir()
                (source / 'token').write_bytes(b'source')
                (target / 'token').write_bytes(b'target')
            else:
                source.write_bytes(b'source'); target.write_bytes(b'target')
            source_identity, target_identity = source.stat(), target.stat()
            try:
                _rename_noreplace(source, target)
            except ArtifactConflictError:
                pass
            else:
                raise UnsupportedError('renameat2 failed actual no-replace capability qualification')
            read = lambda path: (path / 'token').read_bytes() if directory else path.read_bytes()
            if (source.stat() != source_identity or target.stat() != target_identity
                    or read(source) != b'source' or read(target) != b'target'):
                raise UnsupportedError('renameat2 did not preserve existing qualification artifacts')
            if directory:
                (target / 'token').unlink(); target.rmdir()
            else:
                target.unlink()
            _rename_noreplace(source, target)
            if source.exists() or read(target) != b'source':
                raise UnsupportedError('renameat2 failed actual publication capability qualification')
            if directory:
                (target / 'token').unlink(); target.rmdir()
            else:
                target.unlink()


def _destination(destination: Path, protected_paths: list[Path]) -> Path:
    if not isinstance(destination, Path) or type(protected_paths) is not list or any(not isinstance(p, Path) for p in protected_paths):
        raise ModelError('artifact publication requires a destination Path and a list of protected Paths')
    # Lexical normalization is only an early protected-name refusal. Preserve
    # destination itself: its supplied parent traversal must be checked before
    # canonicalization can erase a symlink followed by '..'.
    normalized = Path(os.path.abspath(destination))
    for protected in protected_paths:
        identity = protected.resolve(strict=False)
        if normalized == identity or identity.is_relative_to(normalized) or normalized.is_relative_to(identity):
            raise ArtifactConflictError(f'destination overlaps protected input: {protected}')
    parent = _directory(destination.parent)
    normalized = parent / destination.name
    if os.path.lexists(normalized):
        raise ArtifactConflictError(f'destination already exists; preserved: {normalized}')
    return normalized


def write_json_exclusive(value: Plan | ApprovedPlan | VerificationReport, destination: Path,
                         protected_paths: list[Path], *, limits: Limits | None = None) -> None:
    """Publish complete canonical JSON through qualified RENAME_NOREPLACE."""
    if not isinstance(value, (Plan, ApprovedPlan, VerificationReport)):
        raise ModelError('only Plan, ApprovedPlan or VerificationReport can be published')
    if limits is not None and (not isinstance(value, VerificationReport) or not isinstance(limits, Limits)):
        raise ModelError('explicit artifact limits apply only to VerificationReport and must be Limits')
    limits = limits or (value.plan.limits if isinstance(value, ApprovedPlan) else value.limits if isinstance(value, Plan) else Limits())
    data = canonical_json(value.to_dict())
    if len(data) > limits.max_plan_bytes:
        raise LimitError('artifact JSON bytes exceed plan cap')
    if len(data) > limits.max_staging_bytes:
        raise LimitError('owned JSON staging bytes exceed cap')
    destination = _destination(destination, protected_paths)
    qualify_noreplace(destination.parent)
    owned = None
    try:
        descriptor, name = tempfile.mkstemp(prefix='.pdecision-json-', dir=destination.parent)
        owned = Path(name)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        _rename_noreplace(owned, destination)
    finally:
        if owned is not None:
            owned.unlink(missing_ok=True)


def publish_directory_exclusive(staging: Path, destination: Path, protected_paths: list[Path]) -> None:
    """Transfer a caller-owned real directory without overwriting any input.

    Failure keeps the staging directory for its owner; this helper never removes
    it. Complete-bundle validation is a separate caller responsibility.
    """
    staging = _directory(staging)
    destination = _destination(destination, [*protected_paths, staging])
    if staging.stat().st_dev != destination.parent.stat().st_dev:
        raise UnsupportedError('directory publication requires the same destination filesystem')
    qualify_noreplace(destination.parent)
    _rename_noreplace(staging, destination)
