"""Pure supported-schema planner; no Arrow/native import or value feasibility.

Every conclusion here is a proposal. Timestamp multiplication and casts still
need complete supervised source-value preflight. The verifier must independently
derive its conclusions and must not import this module.
"""
from __future__ import annotations

from .model import (Conflict, FieldSpec, Limits, Metadata, ModelError, Operation,
                    SchemaProposal, SourceSpec, TypeSpec, UnsupportedError, validate_inventory)


def _metadata(variants: list[tuple[int, Metadata]], path: tuple[str, ...], conflicts: list[Conflict]) -> Metadata:
    keys = sorted({key for _, metadata in variants for key, _ in metadata})
    mappings = [(source_id, dict(metadata)) for source_id, metadata in variants]
    common = []
    for key in keys:
        values = tuple((source_id, metadata.get(key)) for source_id, metadata in mappings)
        if len({value for _, value in values}) == 1:
            # key belongs to the union, so an equal variant cannot be all absent.
            common.append((key, values[0][1]))
        else:
            conflicts.append(Conflict(path, key, values))
    return tuple(common)


def _integer_range(spec: TypeSpec) -> tuple[int, int]:
    if spec.kind == "uint":
        return 0, 2**spec.bit_width - 1
    return -(2**(spec.bit_width - 1)), 2**(spec.bit_width - 1) - 1


def _numeric(types: list[TypeSpec]) -> TypeSpec:
    if all(t.kind in ("int", "uint") for t in types):
        ranges = [_integer_range(t) for t in types]
        lower, upper = min(r[0] for r in ranges), max(r[1] for r in ranges)
        kind = "uint" if lower >= 0 else "int"
        for width in (8, 16, 32, 64):
            candidate = TypeSpec(kind, bit_width=width)
            lo, hi = _integer_range(candidate)
            if lo <= lower and upper <= hi:
                return candidate
        return TypeSpec("decimal128", precision=20, scale=0)
    integer_digits, scale = 0, 0
    for t in types:
        if t.kind in ("int", "uint"):
            lo, hi = _integer_range(t)
            integer_digits = max(integer_digits, len(str(max(abs(lo), abs(hi)))))
        else:
            integer_digits = max(integer_digits, t.precision - t.scale)
            scale = max(scale, t.scale)
    precision = max(1, integer_digits + scale)
    if precision > 76:
        raise UnsupportedError("decimal common precision exceeds 76")
    kind = "decimal256" if precision > 38 or any(t.kind == "decimal256" for t in types) else "decimal128"
    return TypeSpec(kind, precision=precision, scale=scale)


def _merge_type(variants: list[tuple[int, TypeSpec]], path: tuple[str, ...], conflicts: list[Conflict]) -> TypeSpec:
    concrete = [(source_id, spec) for source_id, spec in variants if spec.kind != "null"]
    if not concrete:
        return TypeSpec("null")
    types = [t for _, t in concrete]
    kinds = {t.kind for t in types}
    if kinds == {"struct"}:
        return TypeSpec("struct", fields=_merge_fields([(source_id, t.fields) for source_id, t in concrete], path, conflicts))
    if kinds <= {"int", "uint", "decimal128", "decimal256"}:
        return _numeric(types)
    if kinds == {"float"}:
        return TypeSpec("float", bit_width=max(t.bit_width for t in types))
    if kinds == {"timestamp"}:
        if len({t.timezone for t in types}) != 1:
            raise UnsupportedError(f"timestamp timezone labels differ at {path!r}")
        units = ("s", "ms", "us", "ns")
        return TypeSpec("timestamp", unit=max((t.unit for t in types), key=units.index), timezone=types[0].timezone)
    if len(kinds) == 1 and all(t == types[0] for t in types):
        return types[0]
    raise UnsupportedError(f"unsupported type mixture at path {path!r}: {sorted(kinds)!r}")


def _merge_fields(variants: list[tuple[int, tuple[FieldSpec, ...]]], path: tuple[str, ...], conflicts: list[Conflict]) -> tuple[FieldSpec, ...]:
    maps = [(source_id, {f.name: f for f in fields}) for source_id, fields in variants]
    names = sorted({name for _, mapping in maps for name in mapping})
    result = []
    for name in names:
        applicable = [(source_id, mapping[name]) for source_id, mapping in maps if name in mapping]
        field_path = (*path, name)
        typ = _merge_type([(source_id, f.type) for source_id, f in applicable], field_path, conflicts)
        metadata = _metadata([(source_id, f.metadata) for source_id, f in applicable], field_path, conflicts)
        nullable = len(applicable) != len(variants) or any(f.nullable or f.type.kind == "null" for _, f in applicable)
        result.append(FieldSpec(name, typ, nullable, metadata))
    return tuple(result)


def _operations(source_id: int, source_fields: tuple[FieldSpec, ...], target_fields: tuple[FieldSpec, ...], path: tuple[str, ...]) -> list[Operation]:
    mapping = {f.name: f for f in source_fields}
    result = []
    for target in target_fields:
        target_path = (*path, target.name)
        original = mapping.get(target.name)
        source_type = None if original is None else original.type
        if source_type is None or source_type.kind == "null":
            kind = "identity" if source_type == target.type else "null_insert"
        elif source_type.kind == "struct":
            kind = "identity" if source_type == target.type else "struct_union"
        else:
            kind = "identity" if source_type == target.type else "widen"
        result.append(Operation(source_id, target_path, source_type, target.type, kind))
        # A missing/null parent is one absence operation. Its children are not
        # invented applicable fields or metadata variants.
        if source_type is not None and source_type.kind == "struct":
            result.extend(_operations(source_id, source_type.fields, target.type.fields, target_path))
    return result


def plan_schema(sources: list[SourceSpec]) -> SchemaProposal:
    """Deterministically union bounded supported schemas in explicit source order."""
    if type(sources) is not list or not sources:
        raise ModelError("planning needs a nonempty ordered list of sources")
    validate_inventory(tuple(sources), Limits())
    conflicts: list[Conflict] = []
    fields = _merge_fields([(s.source_id, s.fields) for s in sources], (), conflicts)
    metadata = _metadata([(s.source_id, s.schema_metadata) for s in sources], (), conflicts)
    operations = [o for s in sources for o in _operations(s.source_id, s.fields, fields, ())]
    return SchemaProposal(fields, metadata,
                          tuple(sorted(operations, key=lambda o: (o.source_id, o.path))),
                          tuple(sorted(conflicts, key=lambda c: (c.path, c.key))))
