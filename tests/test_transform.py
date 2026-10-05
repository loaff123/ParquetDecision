"""Original workload integration and explicitly hand-written value assertions."""
from dataclasses import replace
from importlib import import_module, util
from pathlib import Path
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parquet_decision.decisions import approve
from parquet_decision.model import Limits, ModelError, Resolution
from parquet_decision.preflight import preflight
from tests.verify_fixtures import union_case, simple_case

FROZEN = Path(__import__('parquet_decision').__file__).resolve().parent / 'data' / 'original-fixtures'
CASES = ('ordinary', 'signed_unsigned', 'decimal_scale', 'nested', 'metadata_conflict',
         'nanoseconds', 'empty', 'larger', 'integer_float', 'timestamp_overflow')


def module(name):
    assert util.find_spec('parquet_decision.' + name), f'{name} implementation missing'
    return import_module('parquet_decision.' + name)


def approved_case(root, limits=None):
    inputs = [p.relative_to(root) for p in sorted(root.glob('*.parquet'))]
    plan = preflight(inputs, root, limits or Limits())
    if not plan.preflight_complete:
        return plan, None
    return plan, approve(plan, [Resolution(c.conflict_digest, 'archive_only') for c in plan.proposal.conflicts])


@pytest.mark.parametrize('name', CASES)
def test_rewrite_original_workloads(tmp_path, name):
    root = FROZEN / name
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('*.parquet')}
    plan, approved = approved_case(root)
    if name in ('integer_float', 'timestamp_overflow'):
        assert approved is None
        assert plan.status == ('UNSUPPORTED' if name == 'integer_float' else 'DATA_INCOMPATIBLE')
        with pytest.raises(ModelError): approve(plan, [])
    else:
        report = module('bundle').apply(approved, root, tmp_path / 'result')
        assert report.verified, report
        table = pq.read_table(tmp_path / 'result')
        assert table.num_rows == sum(s.num_rows for s in plan.sources)
        assert len(list((tmp_path / 'result').glob('*.parquet'))) == len(plan.sources)
        for source in plan.sources:
            part = pq.read_table(tmp_path / 'result' / f'part-{source.source_id:05d}.parquet')
            assert part['__pc_source'].to_pylist() == [source.source_id] * source.num_rows
            assert part['__pc_row'].to_pylist() == list(range(source.num_rows))
            original = pq.read_table(root / source.relative_path)
            for field in original.schema:
                left, right = original[field.name], part[field.name]
                if pa.types.is_timestamp(field.type):
                    target = right.type
                    exponent = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
                    factor = 10 ** (exponent[target.unit] - exponent[field.type.unit])
                    assert right.cast(pa.int64()).to_pylist() == [None if v is None else v * factor for v in left.cast(pa.int64()).to_pylist()]
                elif not pa.types.is_struct(field.type):
                    assert left.to_pylist() == right.to_pylist()
        # Existing documented compatibility only; no timestamp precision promise.
        import duckdb
        assert duckdb.connect().execute('select count(*) from read_parquet(?)', [str(tmp_path / 'result' / '*.parquet')]).fetchone()[0] == table.num_rows
    assert before == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('*.parquet')}


def test_rewrite_combined_reviewer_and_irregular_batches(tmp_path):
    approved, snapshots, _, data = union_case(tmp_path)
    approved = replace(approved, plan=replace(approved.plan, limits=replace(Limits(), batch_rows=2), plan_digest=''), approval_digest='')
    stage = tmp_path / 'rewrite'; stage.mkdir()
    parts = module('transform').rewrite(approved, snapshots, stage)
    assert [p.rows for p in parts] == [3, 1]
    for part, expected in zip(parts, data, strict=True):
        actual = pq.read_table(part.local_path).to_pylist()
        assert actual == [dict(row, __pc_source=part.source_id, __pc_row=i) for i, row in enumerate(expected)]
    from parquet_decision.verify_values import verify_streams
    assert verify_streams(approved, snapshots, parts, source_batch_rows=1, output_batch_rows=2).verified


def test_rewrite_guard_and_empty_source(tmp_path):
    approved, snapshots, _, _ = simple_case(tmp_path, count=0)
    stage = tmp_path / 'rewrite'; stage.mkdir()
    parts = module('transform').rewrite(approved, snapshots, stage)
    assert len(parts) == 1 and parts[0].rows == 0 and parts[0].bytes > 0
    assert pq.read_table(stage).num_rows == 0
    with pytest.raises(ModelError, match='fresh qualified worker'):
        module('transform')._rewrite(approved, snapshots, tmp_path)


def test_apply_combined_reviewer_exact_values(tmp_path):
    from decimal import Decimal
    root = tmp_path / 'original-combined'; root.mkdir()
    timestamps = [-1, 1700000000000000001]
    for i, ids, ticks, payload in ((0, [-2**63, 9], timestamps, [{'distance': 1.5}, None]),
                                  (1, [2**64-1], [1700000000000000002], [{'distance': 2.5, 'quality': 7}])):
        fields = [pa.field('device', pa.int64() if i == 0 else pa.uint64(), nullable=False),
                  pa.field('time', pa.timestamp('ns', tz='UTC'), nullable=False),
                  pa.field('payload', pa.struct([pa.field('distance', pa.float64(), metadata={b'unit': b'm' if i == 0 else b'cm'})] + ([] if i == 0 else [pa.field('quality', pa.int32())])))]
        schema = pa.schema(fields, metadata={b'producer': b'old' if i == 0 else b'new'})
        table = pa.Table.from_arrays([pa.array(ids, type=fields[0].type), pa.array(ticks, type=pa.int64()).cast(fields[1].type), pa.array(payload, type=fields[2].type)], schema=schema)
        pq.write_table(table, root / f'{i}.parquet', version='2.6', compression='NONE', use_dictionary=False)
    before = {p.name: p.read_bytes() for p in root.iterdir()}
    plan, approved = approved_case(root, replace(Limits(), batch_rows=1))
    assert plan.status == 'NEEDS_DECISIONS' and len(approved.resolutions) == 2
    report = module('bundle').apply(approved, root, tmp_path / 'combined')
    assert report.verified, report
    table = pq.read_table(tmp_path / 'combined')
    assert table['device'].to_pylist() == [Decimal(-2**63), Decimal(9), Decimal(2**64-1)]
    assert table['time'].cast(pa.int64()).to_pylist() == [-1, 1700000000000000001, 1700000000000000002]
    assert table['payload'].to_pylist() == [{'distance': 1.5, 'quality': None}, None, {'distance': 2.5, 'quality': 7}]
    assert table['__pc_source'].to_pylist() == [0, 0, 1] and table['__pc_row'].to_pylist() == [0, 1, 0]
    assert table.schema.metadata is None and table.schema.field('payload').type.field('distance').metadata is None
    assert before == {p.name: p.read_bytes() for p in root.iterdir()}


def test_public_imports_remain_native_free(tmp_path):
    import subprocess
    import sys
    code = ('import sys; sys.path.insert(0, ' + repr(str(Path(__import__('parquet_decision').__file__).resolve().parents[1])) + '); '
            'import parquet_decision.transform, parquet_decision.bundle; assert "pyarrow" not in sys.modules')
    result = subprocess.run([sys.executable, '-I', '-S', '-c', code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('source_type,other_type,values', [
    (pa.uint64(), pa.int64(), [0, 2**64-1, None]),
    (pa.decimal128(38, 19), pa.decimal256(39, 19), ['9999999999999999999.1234567890123456789', '0.0000000000000000001', None]),
    (pa.float32(), pa.float64(), [0.1, -0.0, 0.0, float('nan'), float('inf'), -float('inf'), None]),
    (pa.timestamp('ms', tz='UTC'), pa.timestamp('ns', tz='UTC'), [-9223372036854, 0, 9223372036854, None]),
    (pa.timestamp('us'), pa.timestamp('ns'), [-1, 1700000000000000, None]),
    (pa.binary(), pa.binary(), [b'', b'\x00\xff', None]),
    (pa.string(), pa.string(), ['', 'µ\x00z', None]),
    (pa.bool_(), pa.bool_(), [False, True, None]),
    (pa.null(), pa.int32(), [None, None])])
def test_transform_supported_logical_values(tmp_path, source_type, other_type, values):
    from decimal import Decimal, localcontext
    import math
    root = tmp_path / 'sources'; root.mkdir()
    if pa.types.is_decimal(source_type): values = [None if v is None else Decimal(v) for v in values]
    if pa.types.is_timestamp(source_type): source = pa.array(values, type=pa.int64()).cast(source_type)
    else: source = pa.array(values, type=source_type)
    pq.write_table(pa.table({'x': source}), root / '0.parquet', version='2.6', compression='NONE', use_dictionary=False)
    pq.write_table(pa.table({'x': pa.array([], type=other_type)}), root / '1.parquet', version='2.6', compression='NONE', use_dictionary=False)
    _, approved = approved_case(root, replace(Limits(), batch_rows=2))
    with localcontext() as context:
        context.prec = 3
        assert module('bundle').apply(approved, root, tmp_path / 'result').verified
    actual = pq.read_table(tmp_path / 'result' / 'part-00000.parquet')['x']
    if pa.types.is_timestamp(source_type):
        powers = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
        factor = 10 ** (powers[actual.type.unit] - powers[source_type.unit])
        assert actual.cast(pa.int64()).to_pylist() == [None if v is None else v * factor for v in values]
    elif pa.types.is_floating(source_type):
        for left, right in zip(source.to_pylist(), actual.to_pylist(), strict=True):
            if left is None: assert right is None
            elif math.isnan(left): assert math.isnan(right)
            else:
                assert left == right
                if left == 0: assert math.copysign(1, left) == math.copysign(1, right)
    else: assert source.to_pylist() == actual.to_pylist()
