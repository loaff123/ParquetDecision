"""Fresh fixtures; no prototype runtime is imported or copied."""
from importlib import import_module, util


def model():
    assert util.find_spec("parquet_decision") is not None, "strict product model is missing"
    return import_module("parquet_decision.model")


def schema():
    assert util.find_spec("parquet_decision") is not None, "schema planner is missing"
    return import_module("parquet_decision.schema")


def field(name="x", kind="int", *, nullable=False, metadata=(), **kwargs):
    m = model()
    if kind in ("int", "uint", "float"):
        kwargs.setdefault("bit_width", 64)
    return m.FieldSpec(name, m.TypeSpec(kind, **kwargs), nullable, metadata)


def source(source_id=0, fields=None, metadata=()):
    m = model()
    if fields is None:
        fields = (field(),)
    return m.SourceSpec(source_id, f"source-{source_id}.parquet", str(source_id % 10) * 64, 10, 1, tuple(fields), metadata)


def plan():
    m, s = model(), schema()
    sources = (source(),)
    return m.Plan(status="READY_FOR_REVIEW", preflight_complete=True, sources=sources,
                  proposal=s.plan_schema(list(sources)), preflight_counters=m.PreflightCounters(files=1, input_bytes=10, rows=1))
