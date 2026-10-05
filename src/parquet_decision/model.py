"""Immutable records and strict version-1 JSON for the trusted-local alpha.

This module has no native imports. Metadata is exact bytes; logical paths are
segment tuples; filesystem paths are explicitly internal, never source identity.
"""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
from typing import Any, ClassVar


FORMAT_VERSION = 1
PROTOCOL_VERSION = 1
RESERVED_PREFIX = "__pc_"
MAX_DIAGNOSTICS = 32
MAX_DIAGNOSTIC_BYTES = 4096
MAX_WORKER_PATHS = 256
MAX_WORKER_ARGS = 32
MAX_TEXT_BYTES = 8192
# Independent record-count ceiling, not a conversion from raw metadata bytes.
# Empty byte keys/values can consume zero raw metadata bytes. The plan's JSON
# byte envelope generally binds much earlier than this fixed count ceiling.
MAX_CONFLICTS = 1_048_576
MAX_RESOLUTIONS = MAX_CONFLICTS
_HEX = re.compile(r"[0-9a-f]{64}\Z")


class ModelError(ValueError):
    """A strict record, source identity, supported type, or cap was invalid."""


class UnsupportedError(ModelError):
    """An actual valid logical type or type mixture is outside the alpha."""


class LimitError(ModelError):
    """An explicit byte, count, depth, or process-profile cap was exceeded."""


def _cap(value: Any, label: str, maximum: int, minimum: int = 0) -> int:
    _int(value, label, minimum)
    if value > maximum:
        raise LimitError(f"{label} exceeds cap {maximum}")
    return value


def _int(value: Any, label: str, minimum: int = 0, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ModelError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ModelError(f"{label} must be a boolean")
    return value


def _str(value: Any, label: str, *, nonempty: bool = False, bounded: bool = False) -> str:
    if type(value) is not str or (nonempty and not value) or "\x00" in value:
        raise ModelError(f"{label} must be a valid string")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ModelError(f"{label} must be valid UTF-8") from exc
    if bounded and len(encoded) > MAX_TEXT_BYTES:
        raise LimitError(f"{label} exceeds text-byte cap")
    return value


def _digest(value: Any, label: str, *, optional: bool = False) -> str:
    if optional and type(value) is str and value == "":
        return value
    if type(value) is not str or _HEX.fullmatch(value) is None:
        raise ModelError(f"{label} must be lowercase SHA256 hex")
    return value


def _enum(value: Any, label: str, choices: tuple[str, ...]) -> str:
    if type(value) is not str or value not in choices:
        raise ModelError(f"unknown {label}: {value!r}")
    return value


def _tuple(value: Any, label: str) -> tuple:
    if type(value) is not tuple:
        raise ModelError(f"{label} must be an immutable tuple")
    return value


def _array(value: Any, label: str) -> list:
    if type(value) is not list:
        raise ModelError(f"{label} must be a JSON array")
    return value


def _keys(value: Any, expected: tuple[str, ...]) -> dict:
    if type(value) is not dict or set(value) != set(expected):
        actual = set(value) if type(value) is dict else set()
        raise ModelError(f"record keys differ: missing={set(expected) - actual}, unknown={actual - set(expected)}")
    return value


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: Any) -> bytes:
    if type(value) is not str:
        raise ModelError("metadata encoding must be base64 text")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ModelError("invalid base64 metadata") from exc
    if _b64(raw) != value:
        raise ModelError("noncanonical base64 metadata")
    return raw


Metadata = tuple[tuple[bytes, bytes], ...]
Counters = tuple[tuple[str, int], ...]
PathSegments = tuple[str, ...]


def _metadata(value: Any) -> Metadata:
    _tuple(value, "metadata")
    seen = set()
    total = 0
    for pair in value:
        if type(pair) is not tuple or len(pair) != 2 or any(type(p) is not bytes for p in pair):
            raise ModelError("metadata must contain exact byte pairs")
        if pair[0] in seen:
            raise ModelError("duplicate metadata key")
        seen.add(pair[0])
        total += len(pair[0]) + len(pair[1])
    if total > 1024**2:
        raise LimitError("metadata exceeds alpha byte cap")
    return tuple(sorted(value))


def _metadata_from(value: Any) -> Metadata:
    result = []
    for pair in _array(value, "metadata"):
        if type(pair) is not list or len(pair) != 2:
            raise ModelError("metadata must contain JSON byte pairs")
        result.append((_unb64(pair[0]), _unb64(pair[1])))
    return _metadata(tuple(result))


def _path(value: Any) -> PathSegments:
    _tuple(value, "path")
    if len(value) > 9:
        raise LimitError("path exceeds alpha depth")
    for segment in value:
        _str(segment, "path segment")
        if segment.startswith(RESERVED_PREFIX):
            raise ModelError("reserved field name")
    return value


def _relative_path(value: Any) -> str:
    _str(value, "relative_path", nonempty=True)
    path = PurePosixPath(value)
    if path.is_absolute() or "\\" in value or any(p in ("", ".", "..") for p in value.split("/")) or str(path) != value:
        raise ModelError("source path must be normalized relative POSIX path")
    return value


def _diagnostics(value: Any) -> tuple[str, ...]:
    _tuple(value, "diagnostics")
    if len(value) > MAX_DIAGNOSTICS:
        raise LimitError("too many diagnostics")
    for diagnostic in value:
        _str(diagnostic, "diagnostic")
        if len(diagnostic.encode("utf-8")) > MAX_DIAGNOSTIC_BYTES:
            raise LimitError("diagnostic exceeds byte cap")
    return value


def _counters(value: Any) -> Counters:
    _tuple(value, "counters")
    if len(value) > 32:
        raise LimitError("too many counters")
    names = set()
    for pair in value:
        if type(pair) is not tuple or len(pair) != 2:
            raise ModelError("invalid counter pair")
        _str(pair[0], "counter name", nonempty=True, bounded=True)
        _int(pair[1], "counter", maximum=2**63 - 1)
        if pair[0] in names:
            raise ModelError("duplicate counter")
        names.add(pair[0])
    return tuple(sorted(value))


def _pairs_from(value: Any, label: str) -> tuple:
    result = []
    for pair in _array(value, label):
        if type(pair) is not list or len(pair) != 2:
            raise ModelError(f"invalid {label} pair")
        result.append(tuple(pair))
    return tuple(result)


def _encoded(value: Any) -> Any:
    if isinstance(value, Record):
        return value.to_dict()
    if type(value) is bytes:
        return _b64(value)
    if type(value) is tuple:
        return [_encoded(v) for v in value]
    if value is None or type(value) in (str, int, bool):
        return value
    raise ModelError("unsupported JSON value; Python object serialization is forbidden")


def canonical_json(value: Any) -> bytes:
    """Canonical UTF-8 JSON: sorted object keys, compact, no floats or NaN.

    Arrays retain order. Metadata byte pairs are sorted by raw key before base64
    encoding. No Unicode normalization is applied. Digests omit only their own
    named self-digest field, not nested digests.
    """
    def validate(v: Any) -> None:
        if v is None or type(v) in (bool, int):
            return
        if type(v) is str:
            _str(v, "JSON text")
            return
        if type(v) is list:
            for item in v:
                validate(item)
            return
        if type(v) is dict:
            for key, item in v.items():
                _str(key, "JSON key")
                validate(item)
            return
        raise ModelError("only strict JSON values are permitted; floats are forbidden")
    validate(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


class Record:
    """Base for explicit safe JSON record adapters."""
    _internal: ClassVar[tuple[str, ...]] = ()

    def to_dict(self) -> dict[str, Any]:
        return {f.name: _encoded(getattr(self, f.name)) for f in dataclass_fields(self) if f.name not in self._internal}


@dataclass(frozen=True)
class Limits(Record):
    max_files: int = 128
    max_input_bytes: int = 512 * 1024**2
    max_rows: int = 1_000_000
    max_fields: int = 512
    max_depth: int = 8
    max_metadata_bytes: int = 1024**2
    max_plan_bytes: int = 8 * 1024**2
    batch_rows: int = 8192
    max_output_bytes: int = 1024**3
    max_staging_bytes: int = 2 * 1024**3
    wall_seconds: int = 300
    cpu_seconds: int = 120
    address_space_bytes: int = 4 * 1024**3

    def __post_init__(self) -> None:
        for f in dataclass_fields(self):
            _cap(getattr(self, f.name), f.name, f.default, 1)

    @classmethod
    def from_dict(cls, value: Any) -> Limits:
        return cls(**_keys(value, tuple(f.name for f in dataclass_fields(cls))))


@dataclass(frozen=True)
class PreflightCounters(Record):
    files: int = 0
    rows: int = 0
    input_bytes: int = 0

    def __post_init__(self) -> None:
        limits = Limits()
        _cap(self.files, "preflight files", limits.max_files)
        _cap(self.rows, "preflight rows", limits.max_rows)
        _cap(self.input_bytes, "preflight input bytes", limits.max_input_bytes)

    @classmethod
    def from_dict(cls, value: Any) -> PreflightCounters:
        return cls(**_keys(value, ("files", "rows", "input_bytes")))


@dataclass(frozen=True)
class RuntimeInfo(Record):
    python: str = "3.12.14"
    pyarrow: str = "25.0.0"
    platform: str = "linux"
    package_version: str = "0.1.0a1"

    def __post_init__(self) -> None:
        for f in dataclass_fields(self):
            _str(getattr(self, f.name), f.name, nonempty=True, bounded=True)

    @classmethod
    def from_dict(cls, value: Any) -> RuntimeInfo:
        return cls(**_keys(value, ("python", "pyarrow", "platform", "package_version")))


@dataclass(frozen=True)
class TypeSpec(Record):
    kind: str
    bit_width: int | None = None
    precision: int | None = None
    scale: int | None = None
    unit: str | None = None
    timezone: str | None = None
    fields: tuple[FieldSpec, ...] | None = None

    def __post_init__(self) -> None:
        _enum(self.kind, "type kind", ("null", "bool", "int", "uint", "float", "string", "binary", "decimal128", "decimal256", "timestamp", "struct"))
        applicable = {"int": ("bit_width",), "uint": ("bit_width",), "float": ("bit_width",),
                      "decimal128": ("precision", "scale"), "decimal256": ("precision", "scale"),
                      "timestamp": ("unit", "timezone"), "struct": ("fields",)}.get(self.kind, ())
        for name in ("bit_width", "precision", "scale", "unit", "timezone", "fields"):
            if name not in applicable and getattr(self, name) is not None:
                raise ModelError(f"{name} is inapplicable to {self.kind}")
        if self.kind in ("int", "uint", "float"):
            _int(self.bit_width, "bit_width")
            if self.bit_width not in ((32, 64) if self.kind == "float" else (8, 16, 32, 64)):
                raise ModelError("unsupported bit width")
        elif self.kind in ("decimal128", "decimal256"):
            _int(self.precision, "precision", 1, 38 if self.kind == "decimal128" else 76)
            _int(self.scale, "scale", 0, self.precision)
        elif self.kind == "timestamp":
            _enum(self.unit, "timestamp unit", ("s", "ms", "us", "ns"))
            if self.timezone is not None:
                _str(self.timezone, "timezone", nonempty=True)
        elif self.kind == "struct":
            _fields(self.fields)
            count, depth, metadata = _schema_stats(self.fields)
            if count > 512 or depth + 1 > 8 or metadata > 1024**2:
                raise LimitError("struct exceeds field/depth/metadata cap")

    def to_dict(self) -> dict[str, Any]:
        names = {"int": ("bit_width",), "uint": ("bit_width",), "float": ("bit_width",),
                 "decimal128": ("precision", "scale"), "decimal256": ("precision", "scale"),
                 "timestamp": ("unit", "timezone"), "struct": ("fields",)}.get(self.kind, ())
        return {"kind": self.kind, **{name: _encoded(getattr(self, name)) for name in names}}

    @classmethod
    def from_dict(cls, value: Any) -> TypeSpec:
        if type(value) is not dict:
            raise ModelError("type must be a JSON object")
        kind = value.get("kind")
        _enum(kind, "type kind", ("null", "bool", "int", "uint", "float", "string", "binary", "decimal128", "decimal256", "timestamp", "struct"))
        names = {"int": ("bit_width",), "uint": ("bit_width",), "float": ("bit_width",),
                 "decimal128": ("precision", "scale"), "decimal256": ("precision", "scale"),
                 "timestamp": ("unit", "timezone"), "struct": ("fields",)}.get(kind, ())
        data = dict(_keys(value, ("kind", *names)))
        if kind == "struct":
            data["fields"] = tuple(FieldSpec.from_dict(v) for v in _array(data["fields"], "struct fields"))
        return cls(**data)


@dataclass(frozen=True)
class FieldSpec(Record):
    name: str
    type: TypeSpec
    nullable: bool = True
    metadata: Metadata = ()

    def __post_init__(self) -> None:
        _str(self.name, "field name")
        if self.name.startswith(RESERVED_PREFIX):
            raise ModelError("reserved field name")
        if not isinstance(self.type, TypeSpec):
            raise ModelError("field type must be TypeSpec")
        _bool(self.nullable, "nullable")
        object.__setattr__(self, "metadata", _metadata(self.metadata))

    @classmethod
    def from_dict(cls, value: Any) -> FieldSpec:
        d = _keys(value, ("name", "type", "nullable", "metadata"))
        return cls(d["name"], TypeSpec.from_dict(d["type"]), d["nullable"], _metadata_from(d["metadata"]))


def _fields(value: Any) -> tuple[FieldSpec, ...]:
    _tuple(value, "fields")
    names = set()
    for f in value:
        if not isinstance(f, FieldSpec):
            raise ModelError("fields must contain FieldSpec records")
        if f.name in names:
            raise ModelError("duplicate field name")
        names.add(f.name)
    return value


def _schema_stats(fields: tuple[FieldSpec, ...]) -> tuple[int, int, int]:
    count, depth, metadata = 0, 0, 0
    for f in fields:
        count += 1
        metadata += sum(len(k) + len(v) for k, v in f.metadata)
        if f.type.kind == "struct":
            child_count, child_depth, child_metadata = _schema_stats(f.type.fields)
            count += child_count
            depth = max(depth, 1 + child_depth)
            metadata += child_metadata
    return count, depth, metadata


@dataclass(frozen=True)
class SourceSpec(Record):
    source_id: int
    relative_path: str
    sha256: str
    size_bytes: int
    num_rows: int
    fields: tuple[FieldSpec, ...]
    schema_metadata: Metadata = ()

    def __post_init__(self) -> None:
        _int(self.source_id, "source_id", maximum=127)
        _relative_path(self.relative_path)
        _digest(self.sha256, "source sha256")
        _cap(self.size_bytes, "size_bytes", 512 * 1024**2)
        _cap(self.num_rows, "num_rows", 1_000_000)
        _fields(self.fields)
        object.__setattr__(self, "schema_metadata", _metadata(self.schema_metadata))
        count, depth, metadata = _schema_stats(self.fields)
        metadata += sum(len(k) + len(v) for k, v in self.schema_metadata)
        if count > 512 or depth > 8 or metadata > 1024**2:
            raise LimitError("source schema exceeds field/depth/metadata cap")

    @classmethod
    def from_dict(cls, value: Any) -> SourceSpec:
        d = _keys(value, ("source_id", "relative_path", "sha256", "size_bytes", "num_rows", "fields", "schema_metadata"))
        return cls(d["source_id"], d["relative_path"], d["sha256"], d["size_bytes"], d["num_rows"],
                   tuple(FieldSpec.from_dict(f) for f in _array(d["fields"], "fields")), _metadata_from(d["schema_metadata"]))


@dataclass(frozen=True)
class Operation(Record):
    source_id: int
    path: PathSegments
    source_type: TypeSpec | None
    target_type: TypeSpec
    kind: str

    def __post_init__(self) -> None:
        _int(self.source_id, "source_id", maximum=127)
        _path(self.path)
        if not self.path:
            raise ModelError("field operation needs path segments")
        if self.source_type is not None and not isinstance(self.source_type, TypeSpec):
            raise ModelError("operation source type is invalid")
        if not isinstance(self.target_type, TypeSpec):
            raise ModelError("operation target type is invalid")
        _enum(self.kind, "operation kind", ("identity", "widen", "null_insert", "struct_union"))
        if self.kind == "identity" and self.source_type != self.target_type:
            raise ModelError("identity operation changes type")
        if self.kind == "null_insert" and self.source_type is not None and self.source_type.kind != "null":
            raise ModelError("null_insert needs absent or null source")
        if self.kind == "struct_union" and (self.source_type is None or self.source_type.kind != "struct" or self.target_type.kind != "struct"):
            raise ModelError("struct_union requires two struct types")
        if self.kind == "widen" and (self.source_type is None or self.source_type.kind in ("null", "struct") or self.target_type.kind in ("null", "struct") or self.source_type == self.target_type):
            raise ModelError("invalid widening operation")

    @classmethod
    def from_dict(cls, value: Any) -> Operation:
        d = _keys(value, ("source_id", "path", "source_type", "target_type", "kind"))
        return cls(d["source_id"], tuple(_array(d["path"], "path")),
                   None if d["source_type"] is None else TypeSpec.from_dict(d["source_type"]),
                   TypeSpec.from_dict(d["target_type"]), d["kind"])


@dataclass(frozen=True)
class Conflict(Record):
    path: PathSegments
    key: bytes
    values: tuple[tuple[int, bytes | None], ...]
    conflict_digest: str = ""

    def __post_init__(self) -> None:
        _digest(self.conflict_digest, "conflict_digest", optional=True)
        _path(self.path)
        if type(self.key) is not bytes or len(self.key) > 1024**2:
            raise ModelError("conflict key must be bounded bytes")
        _tuple(self.values, "conflict values")
        ids = []
        total = len(self.key)
        for pair in self.values:
            if type(pair) is not tuple or len(pair) != 2:
                raise ModelError("conflict value must contain source-id and value-or-absent")
            _int(pair[0], "source_id", maximum=127)
            if pair[1] is not None and type(pair[1]) is not bytes:
                raise ModelError("conflict value must be exact bytes or absence")
            ids.append(pair[0])
            total += 0 if pair[1] is None else len(pair[1])
        if len(ids) < 2 or len(set(ids)) != len(ids) or len({v for _, v in self.values}) < 2 or total > 1024**2:
            raise ModelError("conflict requires distinct applicable sources and variants within metadata cap")
        object.__setattr__(self, "values", tuple(sorted(self.values)))
        expected = digest_json({"path": _encoded(self.path), "key": _b64(self.key), "values": _encoded(self.values)})
        if self.conflict_digest and self.conflict_digest != expected:
            raise ModelError("conflict digest mismatch")
        object.__setattr__(self, "conflict_digest", expected)

    @classmethod
    def from_dict(cls, value: Any) -> Conflict:
        d = _keys(value, ("path", "key", "values", "conflict_digest"))
        _digest(d["conflict_digest"], "conflict_digest")
        pairs = []
        for pair in _array(d["values"], "values"):
            if type(pair) is not list or len(pair) != 2:
                raise ModelError("invalid conflict values")
            pairs.append((pair[0], None if pair[1] is None else _unb64(pair[1])))
        return cls(tuple(_array(d["path"], "path")), _unb64(d["key"]), tuple(pairs), d["conflict_digest"])


@dataclass(frozen=True)
class SchemaProposal(Record):
    fields: tuple[FieldSpec, ...]
    schema_metadata: Metadata = ()
    operations: tuple[Operation, ...] = ()
    conflicts: tuple[Conflict, ...] = ()

    def __post_init__(self) -> None:
        _fields(self.fields)
        _tuple(self.operations, "operations")
        _tuple(self.conflicts, "conflicts")
        object.__setattr__(self, "schema_metadata", _metadata(self.schema_metadata))
        count, depth, metadata = _schema_stats(self.fields)
        metadata += sum(len(k) + len(v) for k, v in self.schema_metadata)
        if count > 512 or depth > 8 or metadata > 1024**2:
            raise LimitError("target schema exceeds field/depth/metadata cap")
        if tuple(f.name for f in self.fields) != tuple(sorted(f.name for f in self.fields)):
            raise ModelError("target fields must have canonical codepoint ordering")
        _canonical_nested(self.fields)
        if any(not isinstance(o, Operation) for o in self.operations) or len(self.operations) > 128 * 512:
            raise ModelError("invalid or excessive operations")
        if len({(o.source_id, o.path) for o in self.operations}) != len(self.operations):
            raise ModelError("duplicate operation identity")
        if tuple(sorted(self.operations, key=lambda o: (o.source_id, o.path))) != self.operations:
            raise ModelError("operations must be source/path ordered")
        if any(not isinstance(c, Conflict) for c in self.conflicts):
            raise ModelError("invalid conflict records")
        _cap(len(self.conflicts), "conflict count", MAX_CONFLICTS)
        if len({(c.path, c.key) for c in self.conflicts}) != len(self.conflicts):
            raise ModelError("duplicate conflict identity")
        if tuple(sorted(self.conflicts, key=lambda c: (c.path, c.key))) != self.conflicts:
            raise ModelError("conflicts must be path/raw-key ordered")

    @classmethod
    def from_dict(cls, value: Any) -> SchemaProposal:
        d = _keys(value, ("fields", "schema_metadata", "operations", "conflicts"))
        return cls(tuple(FieldSpec.from_dict(f) for f in _array(d["fields"], "fields")), _metadata_from(d["schema_metadata"]),
                   tuple(Operation.from_dict(o) for o in _array(d["operations"], "operations")),
                   tuple(Conflict.from_dict(c) for c in _array(d["conflicts"], "conflicts")))


def _canonical_nested(fields: tuple[FieldSpec, ...]) -> None:
    for f in fields:
        if f.type.kind == "struct":
            child = f.type.fields
            if tuple(c.name for c in child) != tuple(sorted(c.name for c in child)):
                raise ModelError("nested target fields must have canonical codepoint ordering")
            _canonical_nested(child)


PLAN_STATUSES = ("READY_FOR_REVIEW", "NEEDS_DECISIONS", "UNSUPPORTED", "LIMIT_EXCEEDED", "DATA_INCOMPATIBLE", "INCOMPLETE", "ERROR", "CANCELLED")
REVIEW_STATUSES = ("READY_FOR_REVIEW", "NEEDS_DECISIONS")


def validate_inventory(sources: tuple[SourceSpec, ...], limits: Limits) -> None:
    """Validate complete source identities and cumulative declared schema caps."""
    _tuple(sources, "sources")
    if any(not isinstance(s, SourceSpec) for s in sources):
        raise ModelError("sources must contain SourceSpec records")
    if len(sources) > limits.max_files:
        raise LimitError("source file cap exceeded")
    if tuple(s.source_id for s in sources) != tuple(range(len(sources))):
        raise ModelError("source IDs must match explicit ordered inventory")
    if len({s.relative_path for s in sources}) != len(sources):
        raise ModelError("duplicate input identity")
    if sum(s.size_bytes for s in sources) > limits.max_input_bytes:
        raise LimitError("input-byte cap exceeded")
    if sum(s.num_rows for s in sources) > limits.max_rows:
        raise LimitError("row cap exceeded")
    count, metadata = 0, 0
    for s in sources:
        c, depth, size = _schema_stats(s.fields)
        count += c
        metadata += size + sum(len(k) + len(v) for k, v in s.schema_metadata)
        if depth > limits.max_depth:
            raise LimitError("struct-depth cap exceeded")
    if count > limits.max_fields:
        raise LimitError("cumulative source-field cap exceeded")
    if metadata > limits.max_metadata_bytes:
        raise LimitError("cumulative metadata-byte cap exceeded")


@dataclass(frozen=True)
class Plan(Record):
    status: str
    sources: tuple[SourceSpec, ...] = ()
    proposal: SchemaProposal | None = None
    preflight_complete: bool = False
    limits: Limits = field(default_factory=Limits)
    runtime: RuntimeInfo = field(default_factory=RuntimeInfo)
    diagnostics: tuple[str, ...] = ()
    preflight_counters: PreflightCounters = field(default_factory=PreflightCounters)
    format_version: int = FORMAT_VERSION
    plan_digest: str = ""

    def __post_init__(self) -> None:
        _digest(self.plan_digest, "plan_digest", optional=True)
        _int(self.format_version, "format_version", FORMAT_VERSION, FORMAT_VERSION)
        _enum(self.status, "plan status", PLAN_STATUSES)
        _bool(self.preflight_complete, "preflight_complete")
        if not isinstance(self.limits, Limits) or not isinstance(self.runtime, RuntimeInfo) or not isinstance(self.preflight_counters, PreflightCounters):
            raise ModelError("invalid limits/runtime/preflight counters")
        _diagnostics(self.diagnostics)
        _cap(self.preflight_counters.files, "preflight files", self.limits.max_files)
        _cap(self.preflight_counters.rows, "preflight rows", self.limits.max_rows)
        _cap(self.preflight_counters.input_bytes, "preflight input bytes", self.limits.max_input_bytes)
        validate_inventory(self.sources, self.limits)
        if self.proposal is not None and not isinstance(self.proposal, SchemaProposal):
            raise ModelError("invalid schema proposal")
        if self.status in REVIEW_STATUSES:
            if not self.preflight_complete or self.proposal is None or not self.sources:
                raise ModelError("review status needs complete successful preflight and proposal")
            expected = PreflightCounters(len(self.sources), sum(s.num_rows for s in self.sources), sum(s.size_bytes for s in self.sources))
            if self.preflight_counters != expected:
                raise ModelError("preflight counters do not reconcile source inventory")
            if (self.status == "NEEDS_DECISIONS") != bool(self.proposal.conflicts):
                raise ModelError("review status does not match remaining metadata decisions")
        elif self.preflight_complete:
            raise ModelError("failure/incomplete status cannot have complete successful preflight")
        if self.proposal:
            count, depth, metadata = _schema_stats(self.proposal.fields)
            metadata += sum(len(k) + len(v) for k, v in self.proposal.schema_metadata)
            if count > self.limits.max_fields or depth > self.limits.max_depth or metadata > self.limits.max_metadata_bytes:
                raise LimitError("proposal exceeds plan limits")
            ids = {s.source_id for s in self.sources}
            if any(o.source_id not in ids for o in self.proposal.operations) or any(i not in ids for c in self.proposal.conflicts for i, _ in c.values):
                raise ModelError("proposal references unknown source")
        body = self.to_dict()
        body.pop("plan_digest")
        expected_digest = digest_json(body)
        if self.plan_digest and self.plan_digest != expected_digest:
            raise ModelError("plan digest mismatch")
        object.__setattr__(self, "plan_digest", expected_digest)
        if len(canonical_json(self.to_dict())) > self.limits.max_plan_bytes:
            raise LimitError("plan JSON bytes exceed cap")

    @classmethod
    def from_dict(cls, value: Any) -> Plan:
        d = _keys(value, ("format_version", "status", "preflight_complete", "sources", "proposal", "limits", "runtime", "diagnostics", "preflight_counters", "plan_digest"))
        _digest(d["plan_digest"], "plan_digest")
        return cls(status=d["status"], sources=tuple(SourceSpec.from_dict(s) for s in _array(d["sources"], "sources")),
                   proposal=None if d["proposal"] is None else SchemaProposal.from_dict(d["proposal"]),
                   preflight_complete=d["preflight_complete"], limits=Limits.from_dict(d["limits"]), runtime=RuntimeInfo.from_dict(d["runtime"]),
                   diagnostics=tuple(_array(d["diagnostics"], "diagnostics")), preflight_counters=PreflightCounters.from_dict(d["preflight_counters"]),
                   format_version=d["format_version"], plan_digest=d["plan_digest"])


@dataclass(frozen=True)
class Resolution(Record):
    conflict_digest: str
    action: str

    def __post_init__(self) -> None:
        _digest(self.conflict_digest, "conflict_digest")
        _enum(self.action, "resolution action", ("archive_only", "abort"))

    @classmethod
    def from_dict(cls, value: Any) -> Resolution:
        return cls(**_keys(value, ("conflict_digest", "action")))


@dataclass(frozen=True)
class ApprovedPlan(Record):
    plan: Plan
    resolutions: tuple[Resolution, ...]
    approval_digest: str = ""
    status: str = "APPROVED"
    format_version: int = FORMAT_VERSION

    def __post_init__(self) -> None:
        _digest(self.approval_digest, "approval_digest", optional=True)
        _int(self.format_version, "format_version", FORMAT_VERSION, FORMAT_VERSION)
        _enum(self.status, "approval status", ("APPROVED",))
        if not isinstance(self.plan, Plan) or self.plan.status not in REVIEW_STATUSES or not self.plan.preflight_complete:
            raise ModelError("only complete successful review plans can be approved")
        _tuple(self.resolutions, "resolutions")
        _cap(len(self.resolutions), "resolution count", MAX_RESOLUTIONS)
        if any(not isinstance(r, Resolution) for r in self.resolutions):
            raise ModelError("invalid resolution record")
        if any(r.action != "archive_only" for r in self.resolutions):
            raise ModelError("abort resolution cannot produce approval")
        expected = sorted(c.conflict_digest for c in self.plan.proposal.conflicts)
        actual = [r.conflict_digest for r in self.resolutions]
        if actual != expected:
            raise ModelError("approval requires sorted exact complete resolution coverage")
        body = self.to_dict()
        body.pop("approval_digest")
        expected_digest = digest_json(body)
        if self.approval_digest and self.approval_digest != expected_digest:
            raise ModelError("approval digest mismatch")
        object.__setattr__(self, "approval_digest", expected_digest)
        if len(canonical_json(self.to_dict())) > self.plan.limits.max_plan_bytes:
            raise LimitError("approved plan JSON bytes exceed cap")

    @classmethod
    def from_dict(cls, value: Any) -> ApprovedPlan:
        d = _keys(value, ("plan", "resolutions", "approval_digest", "status", "format_version"))
        _digest(d["approval_digest"], "approval_digest")
        return cls(Plan.from_dict(d["plan"]), tuple(Resolution.from_dict(r) for r in _array(d["resolutions"], "resolutions")),
                   d["approval_digest"], d["status"], d["format_version"])


@dataclass(frozen=True)
class Snapshot(Record):
    source: SourceSpec
    local_path: Path | None = field(default=None, compare=False, repr=False)
    _internal: ClassVar[tuple[str, ...]] = ("local_path",)

    def __post_init__(self) -> None:
        if not isinstance(self.source, SourceSpec) or (self.local_path is not None and not isinstance(self.local_path, Path)):
            raise ModelError("invalid snapshot source/local path")

    @classmethod
    def from_dict(cls, value: Any) -> Snapshot:
        d = _keys(value, ("source",))
        return cls(SourceSpec.from_dict(d["source"]))


@dataclass(frozen=True)
class Batch(Record):
    source_id: int
    first_row: int
    record_batch: Any = field(compare=False, repr=False)

    def __post_init__(self) -> None:
        _int(self.source_id, "source_id", maximum=127)
        _int(self.first_row, "first_row", maximum=1_000_000)
        # Batch is an internal worker adapter, the one record that carries native
        # state. Never permit JSON reconstruction of arbitrary Python/native data.
        import pyarrow as pa
        if not isinstance(self.record_batch, pa.RecordBatch):
            raise ModelError("Batch requires an Arrow RecordBatch")
        if self.record_batch.num_rows > 8192 or self.first_row + self.record_batch.num_rows > 1_000_000:
            raise LimitError("batch row cap exceeded")

    @property
    def rows(self) -> int:
        return self.record_batch.num_rows

    def to_dict(self) -> dict:
        raise ModelError("Batch is worker-local native state, never a JSON object")

    @classmethod
    def from_dict(cls, value: Any) -> Batch:
        raise ModelError("Batch cannot deserialize Python/native objects")


@dataclass(frozen=True)
class PartRecord(Record):
    filename: str
    source_id: int
    rows: int
    bytes: int
    sha256: str
    target_schema_digest: str
    local_path: Path | None = field(default=None, compare=False, repr=False)
    _internal: ClassVar[tuple[str, ...]] = ("local_path",)

    def __post_init__(self) -> None:
        _int(self.source_id, "source_id", maximum=127)
        _str(self.filename, "part filename", nonempty=True)
        if self.filename != f"part-{self.source_id:05d}.parquet":
            raise ModelError("part filename must match source identity")
        _cap(self.rows, "part rows", 1_000_000)
        _cap(self.bytes, "part bytes", 1024**3)
        _digest(self.sha256, "part sha256")
        _digest(self.target_schema_digest, "target schema digest")
        if self.local_path is not None and not isinstance(self.local_path, Path):
            raise ModelError("part local path must be a Path")

    @classmethod
    def from_dict(cls, value: Any) -> PartRecord:
        return cls(**_keys(value, ("filename", "source_id", "rows", "bytes", "sha256", "target_schema_digest")))


def _utc(value: Any, *, optional: bool = False) -> str:
    if optional and value == "":
        return value
    _str(value, "checked_at_utc", nonempty=True, bounded=True)
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", value):
        raise ModelError("check time must be an explicit UTC ISO timestamp")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ModelError("invalid UTC check time") from exc
    return value


@dataclass(frozen=True)
class VerificationReport(Record):
    status: str
    plan_digest: str = ""
    approval_digest: str = ""
    source_digest: str = ""
    output_digest: str = ""
    counters: Counters = ()
    diagnostics: tuple[str, ...] = ()
    checked_at_utc: str = ""
    format_version: int = FORMAT_VERSION

    def __post_init__(self) -> None:
        _int(self.format_version, "format_version", FORMAT_VERSION, FORMAT_VERSION)
        _enum(self.status, "verification status", ("VERIFIED", "CONFLICT", "UNSUPPORTED", "LIMIT_EXCEEDED", "INCOMPLETE", "ERROR", "CANCELLED"))
        for name in ("plan_digest", "approval_digest", "source_digest", "output_digest"):
            _digest(getattr(self, name), name, optional=self.status != "VERIFIED")
        _utc(self.checked_at_utc, optional=self.status != "VERIFIED")
        object.__setattr__(self, "counters", _counters(self.counters))
        _diagnostics(self.diagnostics)
        if self.status == "VERIFIED" and (not self.counters or self.diagnostics):
            raise ModelError("fresh complete verification requires counters and no diagnostics")

    @property
    def verified(self) -> bool:
        return self.status == "VERIFIED"

    @classmethod
    def from_dict(cls, value: Any) -> VerificationReport:
        d = _keys(value, ("status", "plan_digest", "approval_digest", "source_digest", "output_digest", "counters", "diagnostics", "checked_at_utc", "format_version"))
        return cls(d["status"], d["plan_digest"], d["approval_digest"], d["source_digest"], d["output_digest"],
                   _pairs_from(d["counters"], "counters"), tuple(_array(d["diagnostics"], "diagnostics")), d["checked_at_utc"], d["format_version"])


WORKER_OPERATIONS = ("preflight", "rewrite", "verify")
WORKER_ARG_KEYS = ("source_root", "staging", "destination", "plan", "approved_plan", "dataset", "batch_rows")


@dataclass(frozen=True)
class WorkerRequest(Record):
    protocol_version: int
    operation: str
    paths: tuple[str, ...] = ()
    args: tuple[tuple[str, str | int], ...] = ()

    def __post_init__(self) -> None:
        _int(self.protocol_version, "protocol_version", PROTOCOL_VERSION, PROTOCOL_VERSION)
        _enum(self.operation, "worker operation", WORKER_OPERATIONS)
        _tuple(self.paths, "worker paths")
        if len(self.paths) > MAX_WORKER_PATHS:
            raise LimitError("worker path cap exceeded")
        for path in self.paths:
            _str(path, "worker path", nonempty=True, bounded=True)
        _tuple(self.args, "worker args")
        if len(self.args) > MAX_WORKER_ARGS:
            raise LimitError("worker arg cap exceeded")
        names = set()
        for pair in self.args:
            if type(pair) is not tuple or len(pair) != 2:
                raise ModelError("invalid worker arg")
            _enum(pair[0], "worker arg", WORKER_ARG_KEYS)
            if pair[0] in names:
                raise ModelError("duplicate worker arg")
            names.add(pair[0])
            if pair[0] == "batch_rows":
                _int(pair[1], "batch_rows", 1, 8192)
            else:
                _str(pair[1], "worker path arg", nonempty=True, bounded=True)
        object.__setattr__(self, "args", tuple(sorted(self.args)))

    @classmethod
    def from_dict(cls, value: Any) -> WorkerRequest:
        d = _keys(value, ("protocol_version", "operation", "paths", "args"))
        return cls(d["protocol_version"], d["operation"], tuple(_array(d["paths"], "paths")), _pairs_from(d["args"], "args"))


@dataclass(frozen=True)
class WorkerResult(Record):
    protocol_version: int
    operation: str
    status: str
    counters: Counters = ()
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _int(self.protocol_version, "protocol_version", PROTOCOL_VERSION, PROTOCOL_VERSION)
        _enum(self.operation, "worker operation", WORKER_OPERATIONS)
        _enum(self.status, "worker terminal status", ("COMPLETE", "UNSUPPORTED", "DATA_INCOMPATIBLE", "CONFLICT", "LIMIT_EXCEEDED", "INCOMPLETE", "ERROR", "CANCELLED"))
        object.__setattr__(self, "counters", _counters(self.counters))
        _diagnostics(self.diagnostics)

    @classmethod
    def from_dict(cls, value: Any) -> WorkerResult:
        d = _keys(value, ("protocol_version", "operation", "status", "counters", "diagnostics"))
        return cls(d["protocol_version"], d["operation"], d["status"], _pairs_from(d["counters"], "counters"), tuple(_array(d["diagnostics"], "diagnostics")))


@dataclass(frozen=True)
class OriginRecord(Record):
    integrity_level: str
    source: SourceSpec
    row: int
    original_fields: tuple[FieldSpec, ...]
    original_schema_metadata: Metadata
    decisions: tuple[Resolution, ...]
    verification_label: str
    format_version: int = FORMAT_VERSION

    def __post_init__(self) -> None:
        _int(self.format_version, "format_version", FORMAT_VERSION, FORMAT_VERSION)
        _enum(self.integrity_level, "origin integrity", ("OUTPUT_HASHES_MATCH", "VERIFIED"))
        if not isinstance(self.source, SourceSpec):
            raise ModelError("invalid origin source")
        _int(self.row, "origin row", maximum=self.source.num_rows - 1)
        _fields(self.original_fields)
        object.__setattr__(self, "original_schema_metadata", _metadata(self.original_schema_metadata))
        if self.original_fields != self.source.fields or self.original_schema_metadata != self.source.schema_metadata:
            raise ModelError("origin field/metadata record differs from source identity")
        _tuple(self.decisions, "origin decisions")
        if any(not isinstance(r, Resolution) for r in self.decisions):
            raise ModelError("invalid origin decision")
        if tuple(sorted(self.decisions, key=lambda r: r.conflict_digest)) != self.decisions or len({r.conflict_digest for r in self.decisions}) != len(self.decisions):
            raise ModelError("origin decisions require sorted unique coverage")
        _enum(self.verification_label, "origin verification label", ("HISTORICAL", "FRESH_SOURCE_BOUND", "NOT_CHECKED"))
        if (self.integrity_level == "VERIFIED") != (self.verification_label == "FRESH_SOURCE_BOUND"):
            raise ModelError("origin VERIFIED requires fresh source-bound verification")

    @classmethod
    def from_dict(cls, value: Any) -> OriginRecord:
        d = _keys(value, ("integrity_level", "source", "row", "original_fields", "original_schema_metadata", "decisions", "verification_label", "format_version"))
        return cls(d["integrity_level"], SourceSpec.from_dict(d["source"]), d["row"],
                   tuple(FieldSpec.from_dict(f) for f in _array(d["original_fields"], "original_fields")), _metadata_from(d["original_schema_metadata"]),
                   tuple(Resolution.from_dict(r) for r in _array(d["decisions"], "decisions")), d["verification_label"], d["format_version"])


def dump_canonical(value: Plan | ApprovedPlan) -> bytes:
    if not isinstance(value, (Plan, ApprovedPlan)):
        raise ModelError("only Plan or ApprovedPlan can be dumped by this interface")
    return canonical_json(value.to_dict())


def _reject_float(value: str) -> Any:
    raise ModelError("JSON floats and non-finite numbers are forbidden")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ModelError("duplicate JSON key")
        result[key] = value
    return result


def parse_json(data: bytes, *, max_bytes: int = 8 * 1024**2) -> Any:
    """Bound byte input before JSON decoding; reject duplicate keys and floats."""
    _int(max_bytes, "JSON byte cap", 1, 8 * 1024**2)
    if type(data) is not bytes:
        raise ModelError("strict JSON input must be bytes")
    if len(data) > max_bytes:
        raise LimitError("plan JSON bytes exceed cap")
    try:
        value = json.loads(data.decode("utf-8", errors="strict"), object_pairs_hook=_unique_object,
                           parse_float=_reject_float, parse_constant=_reject_float)
        # Reject invalid Unicode scalar strings everywhere, even in unknown keys.
        canonical_json(value)
        return value
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        if isinstance(exc, ModelError):
            raise
        raise ModelError("invalid strict JSON") from exc


def read_json(path: Path, limits: Limits) -> Any:
    if not isinstance(path, Path) or not isinstance(limits, Limits):
        raise ModelError("read_json requires Path and Limits")
    try:
        with path.open("rb") as stream:
            data = stream.read(limits.max_plan_bytes + 1)
    except OSError as exc:
        raise ModelError(f"cannot read JSON artifact: {exc}") from exc
    return parse_json(data, max_bytes=limits.max_plan_bytes)


def _raw_schema_stats(value: Any, limits: Limits) -> tuple[int, int]:
    """Count raw source/target schema data before recursively making records."""
    pending = [(f, 0) for f in _array(value, "fields")]
    count, metadata = 0, 0
    while pending:
        raw, parent_depth = pending.pop()
        count += 1
        if count > limits.max_fields:
            raise LimitError("raw schema field cap exceeded")
        d = _keys(raw, ("name", "type", "nullable", "metadata"))
        metadata += sum(len(k) + len(v) for k, v in _metadata_from(d["metadata"]))
        if metadata > limits.max_metadata_bytes:
            raise LimitError("raw schema metadata-byte cap exceeded")
        typ = d["type"]
        if type(typ) is not dict:
            raise ModelError("type must be a JSON object")
        if typ.get("kind") == "struct":
            depth = parent_depth + 1
            if depth > limits.max_depth:
                raise LimitError("raw schema struct-depth cap exceeded")
            pending.extend((f, depth) for f in _array(typ.get("fields"), "struct fields"))
    return count, metadata


def _precheck_plan(value: Any, caller_limits: Limits) -> None:
    """Enforce declared/caller bounds before the recursive record adapters."""
    d = _keys(value, ("format_version", "status", "preflight_complete", "sources", "proposal", "limits", "runtime", "diagnostics", "preflight_counters", "plan_digest"))
    declared = Limits.from_dict(d["limits"])
    limits = Limits(**{f.name: min(getattr(declared, f.name), getattr(caller_limits, f.name)) for f in dataclass_fields(Limits)})
    _int(d["format_version"], "format_version", FORMAT_VERSION, FORMAT_VERSION)
    counters = _keys(d["preflight_counters"], ("files", "rows", "input_bytes"))
    for counter, limit in (("files", "max_files"), ("rows", "max_rows"), ("input_bytes", "max_input_bytes")):
        _cap(counters[counter], f"raw preflight {counter}", getattr(limits, limit))
    sources = _array(d["sources"], "sources")
    _cap(len(sources), "raw source files", limits.max_files)
    field_count, metadata, input_bytes, rows = 0, 0, 0, 0
    for source in sources:
        raw = _keys(source, ("source_id", "relative_path", "sha256", "size_bytes", "num_rows", "fields", "schema_metadata"))
        _int(raw["size_bytes"], "size_bytes")
        _int(raw["num_rows"], "num_rows")
        input_bytes += raw["size_bytes"]
        rows += raw["num_rows"]
        count, size = _raw_schema_stats(raw["fields"], limits)
        field_count += count
        metadata += size + sum(len(k) + len(v) for k, v in _metadata_from(raw["schema_metadata"]))
        _cap(field_count, "raw cumulative source fields", limits.max_fields)
        _cap(metadata, "raw cumulative metadata bytes", limits.max_metadata_bytes)
        _cap(input_bytes, "raw input bytes", limits.max_input_bytes)
        _cap(rows, "raw rows", limits.max_rows)
    if d["proposal"] is not None:
        proposal = _keys(d["proposal"], ("fields", "schema_metadata", "operations", "conflicts"))
        _, size = _raw_schema_stats(proposal["fields"], limits)
        size += sum(len(k) + len(v) for k, v in _metadata_from(proposal["schema_metadata"]))
        _cap(size, "raw target metadata bytes", limits.max_metadata_bytes)
        operations = _array(proposal["operations"], "operations")
        _cap(len(operations), "raw operations", limits.max_files * limits.max_fields)
        for operation in operations:
            raw = _keys(operation, ("source_id", "path", "source_type", "target_type", "kind"))
            _cap(len(_array(raw["path"], "path")), "raw operation path", limits.max_depth + 1)
            for name in ("source_type", "target_type"):
                typ = raw[name]
                if typ is not None and type(typ) is dict and typ.get("kind") == "struct":
                    # A synthetic wrapper makes the struct itself count as one
                    # nesting level, as it will in the eventual TypeSpec.
                    _raw_schema_stats([{"name": "", "type": typ, "nullable": True, "metadata": []}], limits)
        conflicts = _array(proposal["conflicts"], "conflicts")
        _cap(len(conflicts), "raw conflict count", MAX_CONFLICTS)
        for conflict in conflicts:
            raw = _keys(conflict, ("path", "key", "values", "conflict_digest"))
            _cap(len(_array(raw["path"], "path")), "raw conflict path", limits.max_depth + 1)
            _cap(len(_array(raw["values"], "values")), "raw conflict variants", limits.max_files)
    _diagnostics(tuple(_array(d["diagnostics"], "diagnostics")))


def load_plan(path: Path, limits: Limits) -> Plan:
    value = read_json(path, limits)
    _precheck_plan(value, limits)
    try:
        plan = Plan.from_dict(value)
    except RecursionError as exc:
        raise ModelError("record nesting exceeds alpha profile") from exc
    validate_inventory(plan.sources, limits)
    if plan.proposal is not None:
        count, depth, metadata = _schema_stats(plan.proposal.fields)
        if count > limits.max_fields or depth > limits.max_depth or metadata > limits.max_metadata_bytes:
            raise LimitError("target schema exceeds caller limits")
    return plan


def _precheck_approved_plan(value: Any, caller_limits: Limits) -> None:
    """Shared raw approval envelope/caps gate, before recursive record adapters."""
    raw = _keys(value, ("plan", "resolutions", "approval_digest", "status", "format_version"))
    _precheck_plan(raw["plan"], caller_limits)
    _cap(len(_array(raw["resolutions"], "resolutions")), "resolution count", MAX_RESOLUTIONS)


def load_approved_plan(path: Path, limits: Limits) -> ApprovedPlan:
    value = read_json(path, limits)
    _precheck_approved_plan(value, limits)
    try:
        approved = ApprovedPlan.from_dict(value)
    except RecursionError as exc:
        raise ModelError("record nesting exceeds alpha profile") from exc
    validate_inventory(approved.plan.sources, limits)
    return approved


def _type_from_arrow(arrow_type: Any) -> TypeSpec:
    """Worker-only Arrow input adapter, not a public caller-object API.

    Use only in fresh qualified workers without caller application extensions or
    plugins. Arbitrary already-registered caller ExtensionType objects are outside
    the alpha profile; exposing their nested children can invoke Arrow hooks.
    """
    import pyarrow as pa
    if not isinstance(arrow_type, pa.DataType):
        raise ModelError("expected actual Arrow DataType")
    _check_arrow_profile([arrow_type])
    return _convert_arrow_type(arrow_type)


def _check_arrow_profile(roots: list[Any], *, field_count: int = 0, metadata_bytes: int = 0) -> None:
    """Iteratively bound actual Arrow shape before making recursive records.

    This worker-internal pre-walk inspects existing in-memory schema/type objects.
    Direct extension objects are refused. In arbitrary caller environments even
    exposing nested children can invoke registered Arrow extension hooks, so those
    objects are explicitly unsupported. Subsequent conversion recurses only within
    eight struct levels. This is not a native initialization or sandbox guarantee.
    """
    import pyarrow as pa
    _cap(field_count, "Arrow cumulative fields", 512)
    _cap(metadata_bytes, "Arrow cumulative metadata bytes", 1024**2)
    pending = [(typ, 0) for typ in roots]
    while pending:
        typ, parent_depth = pending.pop()
        if isinstance(typ, pa.BaseExtensionType):
            raise UnsupportedError("extension types are unsupported")
        if not pa.types.is_struct(typ):
            continue
        depth = parent_depth + 1
        _cap(depth, "Arrow struct depth", 8)
        field_count += len(typ)
        _cap(field_count, "Arrow cumulative fields", 512)
        for child in typ:
            metadata_bytes += _arrow_metadata_size(child.metadata, field_metadata=True)
            _cap(metadata_bytes, "Arrow cumulative metadata bytes", 1024**2)
            pending.append((child.type, depth))


def _arrow_metadata_size(metadata: Any, *, field_metadata: bool = False) -> int:
    pairs = _metadata(tuple((metadata or {}).items()))
    if field_metadata and any(key in (b"ARROW:extension:name", b"ARROW:extension:metadata") for key, _ in pairs):
        raise UnsupportedError("Arrow extension field metadata is unsupported")
    return sum(len(k) + len(v) for k, v in pairs)


def _convert_arrow_type(arrow_type: Any) -> TypeSpec:
    """Recursive worker conversion after the bounded private pre-walk."""
    import pyarrow as pa
    if isinstance(arrow_type, pa.BaseExtensionType):
        raise UnsupportedError("extension types are unsupported")
    t = pa.types
    if t.is_null(arrow_type):
        return TypeSpec("null")
    if t.is_boolean(arrow_type):
        return TypeSpec("bool")
    if t.is_signed_integer(arrow_type):
        return TypeSpec("int", bit_width=arrow_type.bit_width)
    if t.is_unsigned_integer(arrow_type):
        return TypeSpec("uint", bit_width=arrow_type.bit_width)
    if t.is_floating(arrow_type):
        if arrow_type.bit_width not in (32, 64):
            raise UnsupportedError("only float32 and float64 are supported")
        return TypeSpec("float", bit_width=arrow_type.bit_width)
    if t.is_string(arrow_type):
        return TypeSpec("string")
    if t.is_binary(arrow_type):
        return TypeSpec("binary")
    if t.is_decimal128(arrow_type):
        if arrow_type.scale < 0 or arrow_type.scale > arrow_type.precision:
            raise UnsupportedError("decimal scale is outside the supported alpha profile")
        return TypeSpec("decimal128", precision=arrow_type.precision, scale=arrow_type.scale)
    if t.is_decimal256(arrow_type):
        if arrow_type.scale < 0 or arrow_type.scale > arrow_type.precision:
            raise UnsupportedError("decimal scale is outside the supported alpha profile")
        return TypeSpec("decimal256", precision=arrow_type.precision, scale=arrow_type.scale)
    if t.is_timestamp(arrow_type):
        return TypeSpec("timestamp", unit=arrow_type.unit, timezone=arrow_type.tz)
    if t.is_struct(arrow_type):
        return TypeSpec("struct", fields=tuple(_convert_arrow_field(f) for f in arrow_type))
    raise UnsupportedError("unsupported Arrow logical type")


def _field_from_arrow(arrow_field: Any) -> FieldSpec:
    """Private fresh-worker field input; arbitrary caller extensions unsupported."""
    import pyarrow as pa
    if not isinstance(arrow_field, pa.Field):
        raise ModelError("expected actual Arrow field")
    # Refuse ordinary extension-tagged field metadata before accessing field.type.
    metadata_bytes = _arrow_metadata_size(arrow_field.metadata, field_metadata=True)
    _check_arrow_profile([arrow_field.type], field_count=1, metadata_bytes=metadata_bytes)
    return _convert_arrow_field(arrow_field)


def _convert_arrow_field(arrow_field: Any) -> FieldSpec:
    return FieldSpec(arrow_field.name, _convert_arrow_type(arrow_field.type), arrow_field.nullable,
                     tuple((arrow_field.metadata or {}).items()))


def _fields_from_arrow(arrow_schema: Any) -> tuple[tuple[FieldSpec, ...], Metadata]:
    """Private fresh-worker schema input; not an arbitrary caller-object API."""
    import pyarrow as pa
    if not isinstance(arrow_schema, pa.Schema):
        raise ModelError("expected actual Arrow schema")
    _cap(len(arrow_schema), "Arrow schema fields", 512)
    metadata_bytes = _arrow_metadata_size(arrow_schema.metadata)
    for arrow_field in arrow_schema:
        metadata_bytes += _arrow_metadata_size(arrow_field.metadata, field_metadata=True)
        _cap(metadata_bytes, "Arrow cumulative metadata bytes", 1024**2)
    _check_arrow_profile([f.type for f in arrow_schema], field_count=len(arrow_schema), metadata_bytes=metadata_bytes)
    converted = tuple(_convert_arrow_field(f) for f in arrow_schema)
    _fields(converted)
    metadata = _metadata(tuple((arrow_schema.metadata or {}).items()))
    count, depth, size = _schema_stats(converted)
    size += sum(len(k) + len(v) for k, v in metadata)
    _cap(count, "Arrow schema cumulative fields", 512)
    _cap(depth, "Arrow schema depth", 8)
    _cap(size, "Arrow schema cumulative metadata bytes", 1024**2)
    return converted, metadata


def type_to_arrow(spec: TypeSpec) -> Any:
    import pyarrow as pa
    if not isinstance(spec, TypeSpec):
        raise ModelError("expected TypeSpec")
    if spec.kind == "struct":
        return pa.struct([field_to_arrow(f) for f in spec.fields])
    if spec.kind in ("decimal128", "decimal256"):
        return getattr(pa, spec.kind)(spec.precision, spec.scale)
    if spec.kind == "timestamp":
        return pa.timestamp(spec.unit, tz=spec.timezone)
    if spec.kind in ("int", "uint"):
        return getattr(pa, f"{spec.kind}{spec.bit_width}")()
    if spec.kind == "float":
        return getattr(pa, f"float{spec.bit_width}")()
    return {"null": pa.null, "bool": pa.bool_, "string": pa.string, "binary": pa.binary}[spec.kind]()


def field_to_arrow(spec: FieldSpec) -> Any:
    import pyarrow as pa
    return pa.field(spec.name, type_to_arrow(spec.type), nullable=spec.nullable, metadata=dict(spec.metadata) or None)


def schema_to_arrow(fields: tuple[FieldSpec, ...], metadata: Metadata = ()) -> Any:
    import pyarrow as pa
    _fields(fields)
    return pa.schema([field_to_arrow(f) for f in fields], metadata=dict(_metadata(metadata)) or None)


def schema_digest(fields: tuple[FieldSpec, ...], metadata: Metadata = ()) -> str:
    _fields(fields)
    return digest_json({"fields": _encoded(fields), "schema_metadata": _encoded(_metadata(metadata))})
