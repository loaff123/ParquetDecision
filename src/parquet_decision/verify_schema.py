"""Independent schema conclusions from actual, hash-bound snapshot bytes.

This module deliberately does not depend on the planner, decisions, preflight
alignment, or transformation. Only the strict grammar and primitive byte/Arrow
adapters are shared. Public calls enter a fresh qualified worker.
"""
from __future__ import annotations

from .model import (Conflict, FieldSpec, Limits, ModelError, Operation, SchemaProposal,
                    Snapshot, SourceSpec, TypeSpec, UnsupportedError, validate_inventory)


def derive_expected_schema(snapshots: list[Snapshot]) -> SchemaProposal:
    """Reopen actual snapshots in a fresh worker; never trust recorded schemas."""
    from .verify_values import _invoke
    if type(snapshots) is not list or not snapshots:
        raise ModelError('derivation requires a nonempty Snapshot list')
    result, artifact = _invoke('derive', snapshots, [], Limits())
    if result.status != 'COMPLETE':
        from .model import LimitError
        exception = {'UNSUPPORTED': UnsupportedError, 'LIMIT_EXCEEDED': LimitError}.get(result.status, ModelError)
        raise exception('; '.join(result.diagnostics) or result.status)
    return SchemaProposal.from_dict(artifact)


def _derive_expected_schema(snapshots: list[Snapshot], limits: Limits) -> SchemaProposal:
    """Worker-local entry point for the single supervised apply operation."""
    from .supervise import _require_worker
    from .source import _open_snapshot
    from .model import _fields_from_arrow
    _require_worker()
    import pyarrow as pa
    import pyarrow.parquet as pq
    if pa.__version__ != '25.0.0':
        raise UnsupportedError('qualified verifier requires PyArrow 25.0.0')
    validate_inventory(tuple(s.source for s in snapshots), limits)
    actual = []
    for snapshot in snapshots:
        source = snapshot.source
        try:
            with _open_snapshot(snapshot) as stream:
                reader = pq.ParquetFile(stream, arrow_extensions_enabled=False)
                fields, metadata = _fields_from_arrow(reader.schema_arrow)
                observed = SourceSpec(source.source_id, source.relative_path, source.sha256,
                                      source.size_bytes, reader.metadata.num_rows, fields, metadata)
                if observed != source:
                    raise ModelError('actual source schema/row identity differs from recorded inventory')
                actual.append(observed)
        except (UnsupportedError,):
            raise
        except ModelError as exc:
            from .model import LimitError
            if isinstance(exc, LimitError):
                raise
            raise ModelError(f'source={source.source_id} {exc}') from exc
    validate_inventory(tuple(actual), limits)
    conflicts = []
    fields = _field_union([(s.source_id, s.fields) for s in actual], (), conflicts)
    metadata = _common_metadata([(s.source_id, s.schema_metadata) for s in actual], (), conflicts)
    operations = []
    for source in actual:
        _describe_operations(source.source_id, source.fields, fields, (), operations)
    result = SchemaProposal(fields, metadata, tuple(sorted(operations, key=lambda o: (o.source_id, o.path))),
                            tuple(sorted(conflicts, key=lambda c: (c.path, c.key))))
    # The target is separately bounded. Grammar validation remains shared, not
    # schema conclusions. SourceStats includes exactly the target logical fields.
    from .model import _schema_stats, LimitError
    count, depth, metadata_bytes = _schema_stats(result.fields)
    metadata_bytes += sum(len(k) + len(v) for k, v in result.schema_metadata)
    if count > limits.max_fields or depth > limits.max_depth or metadata_bytes > limits.max_metadata_bytes:
        raise LimitError('independently derived target exceeds limits')
    return result


def _common_metadata(records, path, conflicts):
    maps = [(source_id, dict(metadata)) for source_id, metadata in records]
    keys = sorted({key for _, metadata in maps for key in metadata})
    common = []
    for key in keys:
        variants = tuple((source_id, metadata.get(key)) for source_id, metadata in maps)
        values = {value for _, value in variants}
        if len(values) == 1:
            common.append((key, variants[0][1]))
        else:
            conflicts.append(Conflict(path, key, variants))
    return tuple(common)


def _field_union(records, parent_path, conflicts):
    maps = [(source_id, {f.name: f for f in fields}) for source_id, fields in records]
    names = sorted({name for _, fields in maps for name in fields})
    answer = []
    for name in names:
        present = [(source_id, fields[name]) for source_id, fields in maps if name in fields]
        path = (*parent_path, name)
        typed = [(source_id, field.type) for source_id, field in present if field.type.kind != 'null']
        if not typed:
            target = TypeSpec('null')
        elif all(typ.kind == 'struct' for _, typ in typed):
            target = TypeSpec('struct', fields=_field_union([(i, t.fields) for i, t in typed], path, conflicts))
        else:
            target = _scalar_union([t for _, t in typed])
        nullable = len(present) != len(records) or any(field.nullable or field.type.kind == 'null' for _, field in present)
        metadata = _common_metadata([(i, field.metadata) for i, field in present], path, conflicts)
        answer.append(FieldSpec(name, target, nullable, metadata))
    return tuple(answer)


def _integer_domain(typ):
    if typ.kind == 'uint':
        return 0, (1 << typ.bit_width) - 1
    return -(1 << (typ.bit_width - 1)), (1 << (typ.bit_width - 1)) - 1


def _scalar_union(types):
    kinds = {t.kind for t in types}
    if kinds <= {'int', 'uint'}:
        domains = [_integer_domain(t) for t in types]
        low, high = min(lo for lo, _ in domains), max(hi for _, hi in domains)
        # Preserve all-unsigned typing. Mixed signed/unsigned requires a signed
        # whole-domain type, regardless of observed values or the empty shards.
        kind = 'uint' if kinds == {'uint'} else 'int'
        for bits in (8, 16, 32, 64):
            candidate = TypeSpec(kind, bit_width=bits)
            lower, upper = _integer_domain(candidate)
            if lower <= low and high <= upper:
                return candidate
        return TypeSpec('decimal128', precision=20, scale=0)
    if kinds <= {'int', 'uint', 'decimal128', 'decimal256'}:
        scale = max((t.scale for t in types if t.kind.startswith('decimal')), default=0)
        integer_digits = max(t.precision - t.scale if t.kind.startswith('decimal')
                             else max(len(str(abs(x))) for x in _integer_domain(t)) for t in types)
        precision = integer_digits + scale
        if precision > 76:
            raise UnsupportedError('independently derived decimal precision exceeds 76')
        kind = 'decimal256' if precision > 38 or 'decimal256' in kinds else 'decimal128'
        return TypeSpec(kind, precision=precision, scale=scale)
    if kinds == {'float'}:
        return TypeSpec('float', bit_width=max(t.bit_width for t in types))
    if kinds == {'timestamp'}:
        if len({t.timezone for t in types}) != 1:
            raise UnsupportedError('timestamp timezone labels differ')
        ranks = ('s', 'ms', 'us', 'ns')
        return TypeSpec('timestamp', unit=max((t.unit for t in types), key=ranks.index), timezone=types[0].timezone)
    if len(kinds) == 1 and types[0].kind in ('bool', 'string', 'binary'):
        return types[0]
    raise UnsupportedError('independent source type union is unsupported')


def _describe_operations(source_id, source_fields, target_fields, parent_path, operations):
    by_name = {f.name: f for f in source_fields}
    for field in target_fields:
        original = by_name.get(field.name)
        source_type = None if original is None else original.type
        target_type = field.type
        path = (*parent_path, field.name)
        if source_type == target_type:
            kind = 'identity'
        elif source_type is None or source_type.kind == 'null':
            kind = 'null_insert'
        elif source_type.kind == 'struct':
            kind = 'struct_union'
        else:
            kind = 'widen'
        operations.append(Operation(source_id, path, source_type, target_type, kind))
        if source_type is not None and source_type.kind == 'struct':
            _describe_operations(source_id, source_type.fields, target_type.fields, path, operations)
