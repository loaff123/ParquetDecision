import base64
from dataclasses import FrozenInstanceError, replace
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

from tests.fixtures import field, model, plan, source


def test_model_contract_canonical_roundtrip(tmp_path):
    m = model()
    p = plan()
    data = m.dump_canonical(p)
    assert data == m.dump_canonical(p)
    assert data == json.dumps(json.loads(data), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    path = tmp_path / "plan.json"
    path.write_bytes(data)
    assert m.load_plan(path, m.Limits()) == p
    assert len(p.plan_digest) == 64
    with pytest.raises(FrozenInstanceError):
        p.status = "INCOMPLETE"


def test_model_contract_original_order_and_metadata_are_exact(tmp_path):
    m = model()
    f = field("literal.dot", metadata=((b"\xff.dot", b"\x00\xfe"),))
    p = m.Plan(status="INCOMPLETE", sources=(source(fields=(f, field("a"))),))
    raw = json.loads(m.dump_canonical(p))
    assert raw["sources"][0]["fields"][0]["name"] == "literal.dot"
    assert raw["sources"][0]["fields"][0]["metadata"] == [[base64.b64encode(b"\xff.dot").decode(), base64.b64encode(b"\x00\xfe").decode()]]
    path = tmp_path / "exact.json"
    path.write_bytes(m.dump_canonical(p))
    assert m.load_plan(path, m.Limits()) == p


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(format_version=2),
    lambda d: d.update(unknown="no"),
    lambda d: d.update(preflight_complete=1),
    lambda d: d["sources"][0].update(size_bytes=10.0),
    lambda d: d["sources"][0].update(source_id=True),
    lambda d: d["sources"][0]["fields"][0].update(name="__pc_source"),
    lambda d: d["sources"][0]["fields"][0]["type"].update(unit="ns"),
    lambda d: d["sources"][0]["fields"][0]["type"].update(bit_width=128),
    lambda d: d["sources"][0].update(relative_path="../escape.parquet"),
    lambda d: d["sources"][0].update(relative_path="/absolute.parquet"),
    lambda d: d["sources"][0].update(relative_path="a//b.parquet"),
    lambda d: d["sources"][0].update(relative_path="a/./b.parquet"),
    lambda d: d["sources"][0].update(relative_path="a\\b.parquet"),
    lambda d: d.update(plan_digest="0" * 64),
])
def test_model_contract_rejects_invalid_json(tmp_path, mutation):
    m = model()
    data = json.loads(m.dump_canonical(plan()))
    mutation(data)
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(data))
    with pytest.raises(m.ModelError):
        m.load_plan(path, m.Limits())


@pytest.mark.parametrize("payload", ['{"format_version":1,"format_version":1}', '{"x":NaN}', '{"x":Infinity}', '{"x":1.0}'])
def test_model_contract_rejects_duplicate_keys_and_floats(tmp_path, payload):
    m = model()
    path = tmp_path / "bad.json"
    path.write_text(payload)
    with pytest.raises(m.ModelError):
        m.load_plan(path, m.Limits())


@pytest.mark.parametrize("name,value", [
    ("max_files", 128), ("max_input_bytes", 512 * 1024**2), ("max_rows", 1_000_000),
    ("max_fields", 512), ("max_depth", 8), ("max_metadata_bytes", 1024**2),
    ("max_plan_bytes", 8 * 1024**2), ("batch_rows", 8192), ("max_output_bytes", 1024**3),
    ("max_staging_bytes", 2 * 1024**3), ("wall_seconds", 300), ("cpu_seconds", 120),
    ("address_space_bytes", 4 * 1024**3),
])
def test_model_contract_limits_boundary(name, value):
    m = model()
    assert getattr(m.Limits(**{name: value}), name) == value
    with pytest.raises(m.ModelError):
        m.Limits(**{name: value + 1})
    with pytest.raises(m.ModelError):
        m.Limits(**{name: 0})
    with pytest.raises(m.ModelError):
        m.Limits(**{name: True})


@pytest.mark.parametrize("kind,kwargs", [
    ("decimal128", {"precision": 39, "scale": 0}),
    ("decimal256", {"precision": 77, "scale": 0}),
    ("decimal128", {"precision": 0, "scale": 0}),
    ("decimal128", {"precision": 5, "scale": -1}),
    ("decimal256", {"precision": 5, "scale": 6}),
    ("timestamp", {"unit": "day"}),
    ("timestamp", {"unit": "ns", "timezone": 0}),
    ("string", {"bit_width": 8}),
    ("list", {}),
])
def test_model_contract_strict_tagged_types(kind, kwargs):
    m = model()
    with pytest.raises(m.ModelError):
        m.TypeSpec(kind, **kwargs)


def test_model_contract_refuses_duplicate_names_identities_and_metadata():
    m = model()
    with pytest.raises(m.ModelError):
        source(fields=(field(), field()))
    with pytest.raises(m.ModelError):
        field(metadata=((b"unit", b"m"), (b"unit", b"cm")))
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", sources=(source(), source()))
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", sources=(source(), replace(source(1), relative_path="source-0.parquet")))


def test_model_contract_external_lower_caps_apply_before_deserialization(tmp_path):
    m = model()
    path = tmp_path / "plan.json"
    data = m.dump_canonical(plan())
    path.write_bytes(data)
    with pytest.raises(m.ModelError, match="plan.*bytes|bytes.*plan"):
        m.load_plan(path, replace(m.Limits(), max_plan_bytes=len(data) - 1))
    with pytest.raises(m.ModelError):
        m.load_plan(path, replace(m.Limits(), max_input_bytes=9))


def test_model_contract_rejects_noncanonical_base64(tmp_path):
    m = model()
    p = m.Plan(status="INCOMPLETE", sources=(source(fields=(field(metadata=((b"a", b"b"),)),)),))
    d = json.loads(m.dump_canonical(p))
    d["sources"][0]["fields"][0]["metadata"] = [["YQ", "Yg=="]]
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(d))
    with pytest.raises(m.ModelError):
        m.load_plan(path, m.Limits())


def test_model_contract_every_shared_record_has_strict_json_adapter():
    m = model()
    p = plan()
    records = [
        m.Limits(), m.PreflightCounters(), m.RuntimeInfo(), m.TypeSpec("null"), field(), source(), p.proposal,
        p.proposal.operations[0],
        m.Conflict(("literal.dot",), b"unit", ((0, b"m"), (1, None))),
        p, m.Resolution("1" * 64, "archive_only"),
        m.ApprovedPlan(p, ()),
        m.PartRecord("part-00000.parquet", 0, 1, 10, "2" * 64, "3" * 64),
        m.VerificationReport("VERIFIED", p.plan_digest, "4" * 64, "5" * 64, "6" * 64,
                             (("rows", 1),), (), "2026-10-05T16:00:00Z"),
        m.WorkerRequest(1, "preflight", ("/tmp/source.parquet",), (("source_root", "/tmp"),)),
        m.WorkerResult(1, "preflight", "COMPLETE", (("rows", 1),), ()),
        m.OriginRecord("OUTPUT_HASHES_MATCH", source(), 0, (field(),), (), (), "HISTORICAL"),
    ]
    for record in records:
        assert type(record).from_dict(record.to_dict()) == record
        bad = record.to_dict() | {"unknown": 1}
        with pytest.raises(m.ModelError):
            type(record).from_dict(bad)
    snapshot = m.Snapshot(source(), Path("/tmp/snapshot.parquet"))
    assert snapshot.to_dict() == {"source": source().to_dict()}
    assert m.Snapshot.from_dict(snapshot.to_dict()).local_path is None
    part = replace(next(r for r in records if isinstance(r, m.PartRecord)), local_path=Path("/tmp/output.parquet"))
    assert "local_path" not in part.to_dict()
    assert m.PartRecord.from_dict(part.to_dict()).local_path is None


def test_model_contract_batch_is_internal_and_not_arbitrary_python_json():
    m = model()
    import pyarrow as pa
    batch = m.Batch(0, 0, pa.record_batch([[1]], names=["x"]))
    assert batch.rows == 1
    with pytest.raises(m.ModelError):
        batch.to_dict()
    with pytest.raises(m.ModelError):
        m.Batch.from_dict({"source_id": 0, "first_row": 0, "record_batch": "execute"})


def test_model_contract_verification_flag_and_worker_protocol():
    m = model()
    report = m.VerificationReport(status="INCOMPLETE")
    assert not report.verified
    with pytest.raises(m.ModelError):
        m.VerificationReport(status="VERIFIED")
    with pytest.raises(m.ModelError):
        m.WorkerRequest(1, "run_module", (), (("module", "os"),))
    with pytest.raises(m.ModelError):
        m.WorkerRequest(2, "preflight", (), ())


def test_model_contract_import_does_not_load_arrow():
    model()
    code = "import sys; sys.path.insert(0, sys.argv[1]); import parquet_decision; import parquet_decision.model; import parquet_decision.schema; assert 'pyarrow' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", code, str(Path(__import__('parquet_decision').__file__).resolve().parents[1])], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("kind", ["files", "fields", "metadata", "depth"])
def test_model_contract_raw_caps_precede_record_deserialization(tmp_path, monkeypatch, kind):
    m = model()
    data = json.loads(m.dump_canonical(m.Plan(status="INCOMPLETE", sources=(source(),))))
    if kind == "files":
        data["sources"] *= 129
    elif kind == "fields":
        data["sources"][0]["fields"] *= 513
    elif kind == "metadata":
        data["sources"][0]["schema_metadata"] = [["YQ==", base64.b64encode(b"x" * 1024**2).decode()]]
    else:
        typ = {"kind": "null"}
        for _ in range(9):
            typ = {"kind": "struct", "fields": [{"name": "x", "type": typ, "nullable": True, "metadata": []}]}
        data["sources"][0]["fields"][0]["type"] = typ
    path = tmp_path / "bounded.json"
    path.write_text(json.dumps(data))
    def forbidden(cls, value):
        raise AssertionError("raw structural cap must be checked before record deserialization")
    monkeypatch.setattr(m.Plan, "from_dict", classmethod(forbidden))
    with pytest.raises(m.ModelError):
        m.load_plan(path, m.Limits())


@pytest.mark.parametrize("counter,value", [("files", True), ("rows", 1.0), ("input_bytes", -1)])
def test_model_contract_preflight_counters_are_fixed(counter, value):
    m = model()
    with pytest.raises(m.ModelError):
        m.PreflightCounters(**{counter: value})
    with pytest.raises(m.ModelError):
        m.PreflightCounters.from_dict({"files": 0, "rows": 0, "input_bytes": 0, "unknown": 0})


def test_model_contract_review_admissibility_is_structural():
    m = model()
    s = source()
    with pytest.raises(m.ModelError):
        m.Plan(status="READY_FOR_REVIEW", sources=(s,))
    with pytest.raises(m.ModelError):
        replace(plan(), preflight_counters=m.PreflightCounters(rows=0), plan_digest="")
    with pytest.raises(m.ModelError):
        m.ApprovedPlan(m.Plan(status="INCOMPLETE", sources=(s,)), ())
    with pytest.raises(m.ModelError):
        replace(plan(), status="UNSUPPORTED", plan_digest="")


def test_model_contract_approval_roundtrip_and_exact_resolutions(tmp_path):
    m = model()
    from tests.fixtures import schema
    sources = (source(0, metadata=((b"producer", b"old"),)), source(1, metadata=((b"producer", b"new"),)))
    p = m.Plan(status="NEEDS_DECISIONS", sources=sources, proposal=schema().plan_schema(list(sources)),
               preflight_complete=True, preflight_counters=m.PreflightCounters(files=2, rows=2, input_bytes=20))
    resolution = m.Resolution(p.proposal.conflicts[0].conflict_digest, "archive_only")
    approved = m.ApprovedPlan(p, (resolution,))
    path = tmp_path / "approved.json"
    path.write_bytes(m.dump_canonical(approved))
    assert m.load_approved_plan(path, m.Limits()) == approved
    for resolutions in ((), (resolution, resolution), (replace(resolution, action="abort"),), (m.Resolution("1"*64, "archive_only"),)):
        with pytest.raises(m.ModelError):
            m.ApprovedPlan(p, resolutions)
    bad = approved.to_dict()
    bad["approval_digest"] = "0" * 64
    with pytest.raises(m.ModelError):
        m.ApprovedPlan.from_dict(bad)


def test_model_contract_cumulative_field_and_metadata_caps():
    m = model()
    fields = tuple(field(f"x{i}") for i in range(256))
    assert m.Plan(status="INCOMPLETE", sources=(source(0, fields), source(1, fields)))
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", sources=(source(0, fields), source(1, fields + (field("extra"),))))
    half = ((b"", b"x" * (1024**2 // 2)),)
    assert m.Plan(status="INCOMPLETE", sources=(source(0, metadata=half), source(1, metadata=half)))
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", sources=(source(0, metadata=half), source(1, metadata=((b"", b"x" * (1024**2 // 2 + 1)),))))


def test_model_contract_source_byte_and_row_cap_boundaries():
    m = model()
    maximum = replace(source(), size_bytes=512*1024**2, num_rows=1_000_000)
    assert m.Plan(status="INCOMPLETE", sources=(maximum,))
    with pytest.raises(m.ModelError):
        replace(maximum, size_bytes=maximum.size_bytes+1)
    with pytest.raises(m.ModelError):
        replace(maximum, num_rows=maximum.num_rows+1)
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", sources=(maximum, source(1)))


@pytest.mark.parametrize("record", ["operation", "conflict", "part", "origin", "worker", "report"])
def test_model_contract_rejects_bad_shared_record_values(record):
    m = model()
    with pytest.raises(m.ModelError):
        if record == "operation":
            m.Operation(True, ("x",), m.TypeSpec("null"), m.TypeSpec("null"), "identity")
        elif record == "conflict":
            m.Conflict(("x",), b"k", ((0, b"a"), (0, b"b")))
        elif record == "part":
            m.PartRecord("../source.parquet", 0, 1, 10, "1"*64, "2"*64)
        elif record == "origin":
            m.OriginRecord("VERIFIED", source(), 0, (field(),), (), (), "HISTORICAL")
        elif record == "worker":
            m.WorkerResult(1, "verify", "RUNNING")
        else:
            m.VerificationReport("INCOMPLETE", counters=(("rows", True),))


def test_model_contract_bounded_diagnostics_and_paths():
    m = model()
    assert m.Plan(status="INCOMPLETE", diagnostics=("x"*4096,)*32)
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", diagnostics=("x"*4097,))
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", diagnostics=("x",)*33)
    with pytest.raises(m.ModelError):
        m.WorkerRequest(1, "preflight", ("x"*8193,), ())


def test_model_contract_metadata_order_is_raw_byte_canonical():
    m = model()
    f = field(metadata=((b"\xff", b"last"), (b"a", b"first")))
    assert f.metadata == ((b"a", b"first"), (b"\xff", b"last"))
    assert m.FieldSpec.from_dict(f.to_dict()) == f


def test_model_contract_limit_errors_have_a_dedicated_status_mapping(tmp_path):
    m = model()
    assert issubclass(m.LimitError, m.ModelError)
    with pytest.raises(m.LimitError):
        m.Limits(max_files=129)
    with pytest.raises(m.LimitError):
        replace(source(), size_bytes=512*1024**2+1)
    path = tmp_path / "large.json"
    path.write_bytes(b" " * 11)
    with pytest.raises(m.LimitError):
        m.load_plan(path, replace(m.Limits(), max_plan_bytes=10))
    with pytest.raises(m.ModelError) as error:
        m.PreflightCounters(rows=True)
    assert not isinstance(error.value, m.LimitError)


@pytest.mark.parametrize("digest", [None, False, 0, [], "INVALID"])
def test_model_contract_direct_digest_fields_are_strict(digest):
    m = model()
    with pytest.raises(m.ModelError):
        m.Conflict(("x",), b"k", ((0, b"a"), (1, b"b")), digest)
    with pytest.raises(m.ModelError):
        m.Plan(status="INCOMPLETE", plan_digest=digest)
    with pytest.raises(m.ModelError):
        m.ApprovedPlan(plan(), (), approval_digest=digest)


def test_model_contract_failure_counters_cannot_exceed_declared_lower_limits():
    m = model()
    with pytest.raises(m.LimitError):
        m.Plan(status="INCOMPLETE", limits=m.Limits(max_files=1), preflight_counters=m.PreflightCounters(files=2))


@pytest.mark.parametrize("counter,limit", [("files", "max_files"), ("rows", "max_rows"), ("input_bytes", "max_input_bytes")])
def test_review_r1_caller_lower_caps_cover_failure_preflight_counters(tmp_path, counter, limit):
    m = model()
    p = m.Plan("INCOMPLETE", preflight_counters=m.PreflightCounters(**{counter: 2}))
    path = tmp_path / "partial.json"
    path.write_bytes(m.dump_canonical(p))
    with pytest.raises(m.LimitError):
        m.load_plan(path, m.Limits(**{limit: 1}))
    boundary = m.Plan("INCOMPLETE", preflight_counters=m.PreflightCounters(**{counter: 1}))
    path.write_bytes(m.dump_canonical(boundary))
    assert m.load_plan(path, m.Limits(**{limit: 1})) == boundary


@pytest.mark.parametrize("counter,limit", [("files", "max_files"), ("rows", "max_rows"), ("input_bytes", "max_input_bytes")])
@pytest.mark.parametrize("limit_source", ["declared", "caller"])
def test_review_r1_raw_counter_caps_precede_record_adapters(tmp_path, monkeypatch, counter, limit, limit_source):
    m = model()
    raw = m.Plan("INCOMPLETE", preflight_counters=m.PreflightCounters(**{counter: 2})).to_dict()
    caller = m.Limits()
    if limit_source == "declared":
        raw["limits"][limit] = 1
    else:
        caller = m.Limits(**{limit: 1})
    path = tmp_path / "partial.json"
    path.write_bytes(m.canonical_json(raw))
    def forbidden(cls, value):
        raise AssertionError("raw preflight-counter bounds must precede Plan.from_dict")
    monkeypatch.setattr(m.Plan, "from_dict", classmethod(forbidden))
    with pytest.raises(m.LimitError):
        m.load_plan(path, caller)


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(unknown=0), lambda d: d.update(files=True),
    lambda d: d.update(rows=1.0), lambda d: d.update(input_bytes=-1),
])
def test_review_r1_raw_counter_shape_precedes_record_adapters(tmp_path, monkeypatch, mutation):
    m = model()
    raw = m.Plan("INCOMPLETE").to_dict()
    mutation(raw["preflight_counters"])
    path = tmp_path / "partial.json"
    # json.dumps intentionally permits a malformed float for the loader refusal.
    path.write_text(json.dumps(raw))
    def forbidden(cls, value):
        raise AssertionError("raw preflight-counter shape must precede Plan.from_dict")
    monkeypatch.setattr(m.Plan, "from_dict", classmethod(forbidden))
    with pytest.raises(m.ModelError):
        m.load_plan(path, m.Limits())


@pytest.mark.parametrize("approved", [False, True])
def test_review_r2_zero_byte_conflicts_and_resolutions_roundtrip(tmp_path, approved):
    m = model()
    from tests.fixtures import schema
    empty = ((b"", b""),)
    left = (field("x", metadata=empty), field("y", metadata=empty))
    right = (field("x"), field("y"))
    sources = (source(0, left), source(1, right))
    p = m.Plan("NEEDS_DECISIONS", sources, schema().plan_schema(list(sources)), True,
               limits=m.Limits(max_metadata_bytes=1),
               preflight_counters=m.PreflightCounters(files=2, rows=2, input_bytes=20))
    assert len(p.proposal.conflicts) == 2
    assert all(c.key == b"" and c.values == ((0, b""), (1, None)) for c in p.proposal.conflicts)
    value, loader = p, m.load_plan
    if approved:
        resolutions = tuple(sorted((m.Resolution(c.conflict_digest, "archive_only") for c in p.proposal.conflicts), key=lambda r: r.conflict_digest))
        value, loader = m.ApprovedPlan(p, resolutions), m.load_approved_plan
    assert type(value).from_dict(value.to_dict()) == value
    path = tmp_path / "record.json"
    path.write_bytes(m.dump_canonical(value))
    assert loader(path, m.Limits(max_metadata_bytes=1)) == value


def test_review_ruling_arrow_object_input_adapters_are_worker_private():
    m = model()
    for public_name in ("type_from_arrow", "field_from_arrow", "fields_from_arrow"):
        assert not hasattr(m, public_name), "arbitrary caller Arrow-object input is not an alpha public API"
        assert callable(getattr(m, "_" + public_name))


def test_review_ruling_private_worker_adapter_import_boundary_is_native_free():
    model()
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); import parquet_decision.model as m; "
        "assert 'pyarrow' not in sys.modules; "
        "assert callable(m._type_from_arrow); assert callable(m._field_from_arrow); "
        "assert callable(m._fields_from_arrow); "
        "assert not hasattr(m, 'type_from_arrow'); assert not hasattr(m, 'field_from_arrow'); "
        "assert not hasattr(m, 'fields_from_arrow')"
    )
    result = subprocess.run([sys.executable, "-c", code, str(Path(__import__('parquet_decision').__file__).resolve().parents[1])], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
