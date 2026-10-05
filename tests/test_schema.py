from dataclasses import replace
import pytest
from tests.fixtures import field, model, schema, source


def proposal_for(*types):
    m, s = model(), schema()
    return s.plan_schema([source(i, (m.FieldSpec("x", typ, False),)) for i, typ in enumerate(types)])


@pytest.mark.parametrize("types,expected", [
    ((("int", 64), ("uint", 64)), ("decimal128", 20, 0)),
    ((("int", 64), ("uint", 32)), ("int", 64)),
    ((("int", 8), ("uint", 8)), ("int", 16)),
    ((("uint", 8), ("uint", 16)), ("uint", 16)),
    ((("int", 16), ("int", 32)), ("int", 32)),
    ((("float", 32), ("float", 64)), ("float", 64)),
])
def test_supported_type_lattice_domains(types, expected):
    m = model()
    p = proposal_for(*(m.TypeSpec(k, bit_width=w) for k, w in types))
    actual = p.fields[0].type
    if expected[0].startswith("decimal"):
        assert actual == m.TypeSpec(expected[0], precision=expected[1], scale=expected[2])
    else:
        assert actual == m.TypeSpec(expected[0], bit_width=expected[1])


def test_supported_type_lattice_decimal_integer_digits():
    m = model()
    p = proposal_for(m.TypeSpec("decimal128", precision=10, scale=8), m.TypeSpec("decimal128", precision=10, scale=2))
    assert p.fields[0].type == m.TypeSpec("decimal128", precision=16, scale=8)
    p = proposal_for(m.TypeSpec("uint", bit_width=64), m.TypeSpec("decimal128", precision=38, scale=20))
    assert p.fields[0].type == m.TypeSpec("decimal256", precision=40, scale=20)
    with pytest.raises(m.ModelError):
        proposal_for(m.TypeSpec("decimal256", precision=76, scale=0), m.TypeSpec("decimal256", precision=76, scale=76))


@pytest.mark.parametrize("a,b", [
    (("int", {"bit_width": 8}), ("float", {"bit_width": 64})),
    (("decimal128", {"precision": 8, "scale": 2}), ("float", {"bit_width": 32})),
    (("timestamp", {"unit": "ns", "timezone": "UTC"}), ("timestamp", {"unit": "ms", "timezone": "+00:00"})),
    (("timestamp", {"unit": "ns", "timezone": "UTC"}), ("timestamp", {"unit": "ns"})),
    (("bool", {}), ("int", {"bit_width": 8})),
    (("string", {}), ("binary", {})),
])
def test_supported_type_lattice_refuses_unsupported_mixes(a, b):
    m = model()
    with pytest.raises(m.ModelError):
        proposal_for(m.TypeSpec(a[0], **a[1]), m.TypeSpec(b[0], **b[1]))


def test_supported_type_lattice_timestamp_finest_unit_is_only_proposal():
    m = model()
    p = proposal_for(m.TypeSpec("timestamp", unit="s", timezone="UTC"), m.TypeSpec("timestamp", unit="ns", timezone="UTC"))
    assert p.fields[0].type == m.TypeSpec("timestamp", unit="ns", timezone="UTC")
    assert p.operations[0].kind == "widen"


def test_supported_type_lattice_null_and_struct_union():
    m, s = model(), schema()
    a = m.TypeSpec("struct", fields=(field("a", "int", bit_width=8),))
    b = m.TypeSpec("struct", fields=(field("b", "string"),))
    p = s.plan_schema([source(0, (m.FieldSpec("record", a, False),)), source(1, (m.FieldSpec("record", b, False),)), source(2, (m.FieldSpec("record", m.TypeSpec("null"), False),))])
    result = p.fields[0]
    assert result.nullable
    assert [(f.name, f.nullable) for f in result.type.fields] == [("a", True), ("b", True)]
    assert any(o.source_id == 0 and o.path == ("record", "b") and o.source_type is None and o.kind == "null_insert" for o in p.operations)
    assert any(o.source_id == 2 and o.path == ("record",) and o.kind == "null_insert" for o in p.operations)
    assert not any(o.source_id == 2 and o.path == ("record", "a") for o in p.operations)


def test_metadata_conflict_identity_exact_paths_presence_and_common_bytes():
    m, s = model(), schema()
    nested_a = m.TypeSpec("struct", fields=(field("b", metadata=((b"unit", b"m"),)),))
    nested_b = m.TypeSpec("struct", fields=(field("b", metadata=((b"unit", b"cm"),)),))
    a = source(0, (field("a.b", metadata=((b"unit", b"m"), (b"same", b"\xff"))), m.FieldSpec("a", nested_a, False)), ((b"producer", b"old"),))
    b = source(1, (field("a.b", metadata=((b"same", b"\xff"),)), m.FieldSpec("a", nested_b, False)), ((b"producer", b"new"),))
    p = s.plan_schema([a, b])
    assert {(c.path, c.key) for c in p.conflicts} == {((), b"producer"), (("a.b",), b"unit"), (("a", "b"), b"unit")}
    assert len({c.conflict_digest for c in p.conflicts}) == 3
    literal = next(c for c in p.conflicts if c.path == ("a.b",))
    assert literal.values == ((0, b"m"), (1, None))
    assert next(f for f in p.fields if f.name == "a.b").metadata == ((b"same", b"\xff"),)
    assert p.schema_metadata == ()


def test_metadata_conflict_identity_missing_whole_field_is_only_absence():
    m, s = model(), schema()
    p = s.plan_schema([source(0, (field("a", metadata=((b"unit", b"m"),)),)), source(1, (field("z"),))])
    assert not p.conflicts
    assert p.fields[0].metadata == ((b"unit", b"m"),)
    assert p.fields[0].nullable
    assert any(o.source_id == 1 and o.path == ("a",) and o.source_type is None for o in p.operations)


def test_schema_canonical_order_preserves_source_order_and_is_deterministic():
    m, s = model(), schema()
    a = source(0, (field("z"), field("A"), field("é"), field("a")))
    p = s.plan_schema([a])
    assert [f.name for f in p.fields] == ["A", "a", "z", "é"]
    assert [o.path for o in p.operations] == [("A",), ("a",), ("z",), ("é",)]
    assert p == s.plan_schema([a])
    assert [f.name for f in a.fields] == ["z", "A", "é", "a"]


def test_arrow_type_conversion_is_lazy_and_exact():
    m = model()
    import pyarrow as pa
    arrow = pa.schema([pa.field("literal.dot", pa.struct([pa.field("child", pa.decimal256(76, 3), nullable=False)]), metadata={b"\xff": b"\x00"})], metadata={b"schema": b"raw"})
    fields, metadata = m._fields_from_arrow(arrow)
    assert fields[0].type.fields[0].type == m.TypeSpec("decimal256", precision=76, scale=3)
    assert metadata == ((b"schema", b"raw"),)
    assert m.schema_to_arrow(fields, metadata).equals(arrow, check_metadata=True)


@pytest.mark.parametrize("factory", [
    lambda pa: pa.list_(pa.int8()), lambda pa: pa.map_(pa.string(), pa.int32()),
    lambda pa: pa.dictionary(pa.int8(), pa.string()), lambda pa: pa.date32(),
    lambda pa: pa.duration("ns"), lambda pa: pa.binary(4), lambda pa: pa.decimal128(3, -1),
    lambda pa: pa.large_string(), lambda pa: pa.large_binary(),
    lambda pa: pa.union([pa.field("a", pa.int8())], mode="sparse"),
])
def test_arrow_type_conversion_rejects_every_excluded_family(factory):
    m = model()
    import pyarrow as pa
    with pytest.raises(m.ModelError):
        m._type_from_arrow(factory(pa))


def test_worker_type_adapter_refuses_direct_unregistered_extension():
    """Narrow direct-object refusal; registered nested caller objects are excluded."""
    m = model()
    import pyarrow as pa
    class OriginalExtension(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int8(), "pdecision.original-fixture")
        def __arrow_ext_serialize__(self):
            return b"benign"
        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            raise AssertionError("direct unregistered fixture unexpectedly reached deserializer")
    with pytest.raises(m.ModelError):
        m._type_from_arrow(OriginalExtension())


def test_supported_type_lattice_field_and_depth_caps():
    m = model()
    assert len(source(fields=tuple(field(f"x{i}") for i in range(512))).fields) == 512
    with pytest.raises(m.ModelError):
        source(fields=tuple(field(f"x{i}") for i in range(513)))
    typ = m.TypeSpec("null")
    for _ in range(8):
        typ = m.TypeSpec("struct", fields=(m.FieldSpec("child", typ, True),))
    assert source(fields=(m.FieldSpec("root", typ, True),))
    with pytest.raises(m.ModelError):
        m.TypeSpec("struct", fields=(m.FieldSpec("child", typ, True),))


@pytest.mark.parametrize("factory,expected", [
    (lambda pa: pa.null(), ("null", {})), (lambda pa: pa.bool_(), ("bool", {})),
    (lambda pa: pa.int8(), ("int", {"bit_width": 8})), (lambda pa: pa.int16(), ("int", {"bit_width": 16})),
    (lambda pa: pa.int32(), ("int", {"bit_width": 32})), (lambda pa: pa.int64(), ("int", {"bit_width": 64})),
    (lambda pa: pa.uint8(), ("uint", {"bit_width": 8})), (lambda pa: pa.uint16(), ("uint", {"bit_width": 16})),
    (lambda pa: pa.uint32(), ("uint", {"bit_width": 32})), (lambda pa: pa.uint64(), ("uint", {"bit_width": 64})),
    (lambda pa: pa.float32(), ("float", {"bit_width": 32})), (lambda pa: pa.float64(), ("float", {"bit_width": 64})),
    (lambda pa: pa.string(), ("string", {})), (lambda pa: pa.binary(), ("binary", {})),
    (lambda pa: pa.decimal128(38, 38), ("decimal128", {"precision": 38, "scale": 38})),
    (lambda pa: pa.decimal256(76, 0), ("decimal256", {"precision": 76, "scale": 0})),
    (lambda pa: pa.timestamp("s"), ("timestamp", {"unit": "s"})),
    (lambda pa: pa.timestamp("ms", "UTC"), ("timestamp", {"unit": "ms", "timezone": "UTC"})),
    (lambda pa: pa.timestamp("us", "America/New_York"), ("timestamp", {"unit": "us", "timezone": "America/New_York"})),
    (lambda pa: pa.timestamp("ns", "+00:00"), ("timestamp", {"unit": "ns", "timezone": "+00:00"})),
    (lambda pa: pa.struct([]), ("struct", {"fields": ()})),
])
def test_arrow_supported_types_roundtrip(factory, expected):
    m = model()
    import pyarrow as pa
    arrow = factory(pa)
    spec = m.TypeSpec(expected[0], **expected[1])
    assert m._type_from_arrow(arrow) == spec
    assert m.type_to_arrow(spec) == arrow


def test_schema_all_null_empty_and_same_type_are_deterministic():
    m, s = model(), schema()
    p = proposal_for(m.TypeSpec("null"), m.TypeSpec("null"))
    assert p.fields[0].type == m.TypeSpec("null")
    assert p.fields[0].nullable
    assert [o.kind for o in p.operations] == ["identity", "identity"]
    assert s.plan_schema([source(0, ()), source(1, ())]).fields == ()
    with pytest.raises(m.ModelError):
        s.plan_schema([])


def test_schema_rejects_duplicate_and_reserved_arrow_names():
    m = model()
    import pyarrow as pa
    for arrow in (pa.schema([("x", pa.int8()), ("x", pa.int8())]), pa.schema([("__pc_row", pa.int8())]), pa.schema([("parent", pa.struct([("__pc_nested", pa.int8())]))])):
        with pytest.raises(m.ModelError):
            m._fields_from_arrow(arrow)


def test_schema_digest_preserves_path_segments_and_raw_metadata():
    m, s = model(), schema()
    a = m.Conflict(("a.b",), b"key", ((0, b"v"), (1, None)))
    b = m.Conflict(("a", "b"), b"key", ((0, b"v"), (1, None)))
    c = m.Conflict(("a.b",), b"key.dot", ((0, b"v"), (1, None)))
    assert len({a.conflict_digest, b.conflict_digest, c.conflict_digest}) == 3
    assert m.schema_digest((field(),), ()) != m.schema_digest((field(nullable=True),), ())


def test_schema_unsupported_errors_have_dedicated_status_mapping():
    m = model()
    import pyarrow as pa
    assert issubclass(m.UnsupportedError, m.ModelError)
    with pytest.raises(m.UnsupportedError):
        m._type_from_arrow(pa.list_(pa.int8()))
    with pytest.raises(m.UnsupportedError):
        m._type_from_arrow(pa.decimal128(3, -1))
    with pytest.raises(m.UnsupportedError):
        m._type_from_arrow(pa.float16())
    with pytest.raises(m.UnsupportedError):
        proposal_for(m.TypeSpec("int", bit_width=8), m.TypeSpec("float", bit_width=32))
    with pytest.raises(m.ModelError) as error:
        m._type_from_arrow("not an actual Arrow type")
    assert not isinstance(error.value, m.UnsupportedError)


def test_arrow_schema_adapter_bounds_complete_original_schema():
    m = model()
    import pyarrow as pa
    with pytest.raises(m.LimitError):
        m._fields_from_arrow(pa.schema([(f"x{i}", pa.int8()) for i in range(513)]))
    with pytest.raises(m.LimitError):
        m._fields_from_arrow(pa.schema([("x", pa.int8())], metadata={b"k": b"v" * 1024**2}))


def test_arrow_nested_reserved_name_is_invalid_model_input():
    m = model()
    import pyarrow as pa
    with pytest.raises(m.ModelError) as error:
        m._fields_from_arrow(pa.schema([("parent", pa.struct([("__pc_nested", pa.int8())]))]))
    assert not isinstance(error.value, m.UnsupportedError)


def original_nested_arrow(depth):
    import pyarrow as pa
    result = pa.int8()
    for _ in range(depth):
        result = pa.struct([pa.field("child", result, nullable=False)])
    return result


@pytest.mark.parametrize("adapter", ["type", "field", "schema"])
@pytest.mark.parametrize("depth", [8, 9, 300])
def test_review_r3_arrow_depth_is_bounded_before_conversion(adapter, depth, monkeypatch):
    m = model()
    import pyarrow as pa
    typ = original_nested_arrow(depth)
    if depth > 8:
        def forbidden(value):
            raise AssertionError("Arrow bounds must precede recursive TypeSpec conversion")
        monkeypatch.setattr(m, "_convert_arrow_type", forbidden)
    if adapter == "type":
        action = lambda: m._type_from_arrow(typ)
    elif adapter == "field":
        action = lambda: m._field_from_arrow(pa.field("root", typ))
    else:
        action = lambda: m._fields_from_arrow(pa.schema([pa.field("root", typ)]))
    if depth <= 8:
        assert action()
    else:
        with pytest.raises(m.LimitError):
            action()


@pytest.mark.parametrize("bound", ["fields", "metadata"])
def test_review_r3_arrow_cumulative_bounds_precede_conversion(bound, monkeypatch):
    m = model()
    import pyarrow as pa
    if bound == "fields":
        typ = pa.struct([pa.field(f"x{i}", pa.int8()) for i in range(513)])
    else:
        half = b"x" * (1024**2 // 2)
        typ = pa.struct([pa.field("x", pa.int8(), metadata={b"a": half}), pa.field("y", pa.int8(), metadata={b"b": half})])
    def forbidden(value):
        raise AssertionError("Arrow aggregate bounds must precede recursive TypeSpec conversion")
    monkeypatch.setattr(m, "_convert_arrow_type", forbidden)
    with pytest.raises(m.LimitError):
        m._type_from_arrow(typ)


@pytest.mark.parametrize("key", [b"ARROW:extension:name", b"ARROW:extension:metadata"])
@pytest.mark.parametrize("adapter", ["type", "field", "schema"])
def test_worker_adapter_refuses_reserved_extension_field_metadata(key, adapter):
    """Ordinary well-formed fields only; no registered hook or file decoder."""
    m = model()
    import pyarrow as pa
    original = pa.field("child", pa.int8(), metadata={key: b"original-benign-fixture"})
    if adapter == "type":
        action = lambda: m._type_from_arrow(pa.struct([original]))
    elif adapter == "field":
        action = lambda: m._field_from_arrow(original)
    else:
        action = lambda: m._fields_from_arrow(pa.schema([original]))
    with pytest.raises(m.UnsupportedError):
        action()
