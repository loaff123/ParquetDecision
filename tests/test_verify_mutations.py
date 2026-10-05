from copy import deepcopy
from dataclasses import replace
import pytest
import pyarrow as pa
from tests.verify_fixtures import output_schema, union_case, verifier, write_part


@pytest.mark.parametrize('mutation,row,path', [
    ('wrong_value', 0, '["z"]'), ('absent_top', 0, '["added"]'), ('absent_child', 0, '["rec","y"]'), ('typed_null', 0, '["n"]'),
    ('null_parent_collapsed', 1, '["rec"]'), ('valid_all_null_collapsed', 2, '["rec"]'), ('source_id', 0, '["__pc_source"]'), ('row_index', 1, '["__pc_row"]')])
def test_complete_target_mutations_fail_at_coordinate(tmp_path, mutation, row, path):
    approved, snapshots, parts, rows = union_case(tmp_path); changed = deepcopy(rows[0])
    if mutation == 'wrong_value': changed[0]['z'] = 99
    if mutation == 'absent_top': changed[0]['added'] = 999
    if mutation == 'absent_child': changed[0]['rec']['y'] = 999
    if mutation == 'typed_null': changed[0]['n'] = 999
    if mutation == 'null_parent_collapsed': changed[1]['rec'] = {'x': None, 'y': None}
    if mutation == 'valid_all_null_collapsed': changed[2]['rec'] = None
    if mutation == 'source_id': changed[0]['__pc_source'] = 1
    if mutation == 'row_index': changed[1]['__pc_row'] = 0
    parts[0] = write_part(tmp_path / 'parts', 0, approved.plan.proposal, changed)
    result = verifier().verify_streams(approved, snapshots, parts)
    assert result.status == 'CONFLICT', result
    assert f'source=0 row={row} path={path}' in ' '.join(result.diagnostics)


@pytest.mark.parametrize('mutation', ['missing_field', 'extra_field', 'type', 'nullability', 'field_metadata', 'schema_metadata', 'reordered_fields', 'provenance_type', 'provenance_nullable', 'provenance_metadata', 'missing_provenance', 'reordered_children', 'child_metadata'])
def test_output_schema_mutations(tmp_path, mutation):
    approved, snapshots, parts, rows = union_case(tmp_path); schema = output_schema(approved.plan.proposal)
    if mutation == 'missing_field': schema = schema.remove(0)
    elif mutation == 'extra_field': schema = schema.append(pa.field('extra', pa.int64()))
    elif mutation == 'type': schema = schema.set(0, pa.field('added', pa.int64()))
    elif mutation == 'nullability': schema = schema.set(3, pa.field('z', pa.int16(), nullable=True))
    elif mutation == 'field_metadata': schema = schema.set(0, schema.field(0).with_metadata({b'k': b'v'}))
    elif mutation == 'schema_metadata': schema = schema.with_metadata({b'k': b'v'})
    elif mutation == 'reordered_fields': schema = pa.schema([schema.field(1), schema.field(0), *list(schema)[2:]])
    elif mutation == 'provenance_type': schema = schema.set(4, pa.field('__pc_source', pa.uint64(), nullable=False))
    elif mutation == 'provenance_nullable': schema = schema.set(4, pa.field('__pc_source', pa.uint32()))
    elif mutation == 'provenance_metadata': schema = schema.set(4, schema.field(4).with_metadata({b'k': b'v'}))
    elif mutation == 'missing_provenance': schema = schema.remove(5)
    elif mutation == 'reordered_children': schema = schema.set(2, pa.field('rec', pa.struct(list(schema.field(2).type)[::-1])))
    elif mutation == 'child_metadata': schema = schema.set(2, pa.field('rec', pa.struct([schema.field(2).type[0].with_metadata({b'unit': b'wrong'}), schema.field(2).type[1]])))
    parts[0] = write_part(tmp_path / 'parts', 0, approved.plan.proposal, rows[0], schema=schema)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'CONFLICT', report
    assert 'source=0' in ' '.join(report.diagnostics) and 'schema' in ' '.join(report.diagnostics)


@pytest.mark.parametrize('mutation', ['drop_row', 'duplicate_row', 'reorder_rows', 'missing_part', 'extra_part', 'part_order', 'row_count', 'part_hash', 'schema_digest', 'extra_file', 'source_hash', 'missing_source', 'source_order', 'output_only', 'truncated_output'])
def test_inventory_and_provenance(tmp_path, mutation):
    approved, snapshots, parts, rows = union_case(tmp_path)
    if mutation == 'drop_row': parts[0] = write_part(tmp_path / 'parts', 0, approved.plan.proposal, rows[0][:-1])
    if mutation == 'duplicate_row': parts[0] = write_part(tmp_path / 'parts', 0, approved.plan.proposal, [rows[0][0], rows[0][0], rows[0][2]])
    if mutation == 'reorder_rows': parts[0] = write_part(tmp_path / 'parts', 0, approved.plan.proposal, rows[0][::-1])
    if mutation == 'missing_part': parts.pop()
    if mutation == 'extra_part': parts.append(parts[1])
    if mutation == 'part_order': parts.reverse()
    if mutation == 'row_count': parts[0] = replace(parts[0], rows=1)
    if mutation == 'part_hash': parts[0] = replace(parts[0], sha256='0' * 64)
    if mutation == 'schema_digest': parts[0] = replace(parts[0], target_schema_digest='0' * 64)
    if mutation == 'extra_file': (tmp_path / 'parts' / 'extra.parquet').write_bytes(parts[0].local_path.read_bytes())
    if mutation == 'source_hash': snapshots[0].local_path.write_bytes(snapshots[1].local_path.read_bytes())
    if mutation == 'missing_source': snapshots[0].local_path.unlink()
    if mutation == 'source_order': snapshots.reverse()
    if mutation == 'output_only': snapshots = []
    if mutation == 'truncated_output':
        path = parts[0].local_path
        path.write_bytes(path.read_bytes()[:-1])  # Hash mismatch, refused before native parsing
    result = verifier().verify_streams(approved, snapshots, parts)
    assert not result.verified
    assert result.status == ('INCOMPLETE' if mutation in ('missing_source', 'output_only') else 'CONFLICT'), result


@pytest.mark.parametrize('kind', ['visible_file', 'subdirectory', 'part_symlink'])
def test_exact_dataset_inventory_excludes_unrecorded_entries(tmp_path, kind):
    approved, snapshots, parts, _ = union_case(tmp_path)
    root = tmp_path / 'parts'
    if kind == 'visible_file': (root / 'surprise.txt').write_text('ordinary extra dataset file')
    elif kind == 'subdirectory': (root / 'other-data').mkdir()
    else:
        path = parts[0].local_path
        copy = tmp_path / 'copy.parquet'; copy.write_bytes(path.read_bytes())
        path.unlink(); path.symlink_to(copy)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'CONFLICT', report


@pytest.mark.parametrize('change', ['integer', 'decimal_tick', 'timestamp_tick', 'float32_rounding', 'negative_zero', 'nan_to_number', 'number_to_nan', 'binary', 'string', 'bool'])
def test_exact_numeric_and_primitive_mutations(tmp_path, change):
    from decimal import Decimal
    from parquet_decision.model import Operation, SchemaProposal, TypeSpec
    from tests.verify_fixtures import approve, f, integer, write_source
    cases = {
        'integer': (integer(64, True), integer(64, True), 2**64-1, 2**64-2),
        'decimal_tick': (TypeSpec('decimal256', precision=76, scale=38), TypeSpec('decimal256', precision=76, scale=38),
                         Decimal('12345678901234567890123456789012345678.12345678901234567890123456789012345678'),
                         Decimal('12345678901234567890123456789012345678.12345678901234567890123456789012345679')),
        'timestamp_tick': (TypeSpec('timestamp', unit='ns', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='UTC'), 1700000000000000001, 1700000000000000002),
        'float32_rounding': (TypeSpec('float', bit_width=32), TypeSpec('float', bit_width=64), 0.1, 0.1),
        'negative_zero': (TypeSpec('float', bit_width=64), TypeSpec('float', bit_width=64), -0.0, 0.0),
        'nan_to_number': (TypeSpec('float', bit_width=64), TypeSpec('float', bit_width=64), float('nan'), 1.0),
        'number_to_nan': (TypeSpec('float', bit_width=64), TypeSpec('float', bit_width=64), 1.0, float('nan')),
        'binary': (TypeSpec('binary'), TypeSpec('binary'), b'\xff\x00', b'\xff'),
        'string': (TypeSpec('string'), TypeSpec('string'), 'é', 'e\u0301'),
        'bool': (TypeSpec('bool'), TypeSpec('bool'), True, False)}
    source_type, target_type, value, wrong = cases[change]
    snapshots = [write_source(tmp_path / 'sources', 0, (f('x', source_type),), [{'x': value}])]
    if source_type != target_type:
        snapshots.append(write_source(tmp_path / 'sources', 1, (f('x', target_type),), []))
    proposal = SchemaProposal((f('x', target_type),), operations=tuple(
        Operation(i, ('x',), s.source.fields[0].type, target_type, 'identity' if s.source.fields[0].type == target_type else 'widen')
        for i, s in enumerate(snapshots)))
    parts = [write_part(tmp_path / 'parts', 0, proposal, [{'x': wrong}])]
    if len(snapshots) == 2: parts.append(write_part(tmp_path / 'parts', 1, proposal, []))
    report = verifier().verify_streams(approve(snapshots, proposal), snapshots, parts)
    assert report.status == 'CONFLICT', report
    assert 'source=0 row=0 path=["x"]' in ' '.join(report.diagnostics)


def test_inventory_stops_on_first_extra_entry(tmp_path, monkeypatch):
    from pathlib import Path
    from parquet_decision.model import ModelError
    approved, snapshots, parts, _ = union_case(tmp_path)
    root = parts[0].local_path.parent
    extra = root / 'extra.txt'; extra.write_text('ordinary extra file')
    original = Path.iterdir
    def controlled_entries(path):
        if path == root:
            yield extra
            raise AssertionError('inventory consumed entries after already finding an extra file')
        else:
            yield from original(path)
    monkeypatch.setattr(Path, 'iterdir', controlled_entries)
    with pytest.raises(ModelError, match='inventory'):
        verifier()._check_part_inventory(parts)


def test_preserved_prototype_false_pass_is_rejected(tmp_path):
    """Exact benign A{x:1}, B{x:2,y:3}; A's invented y=999 must fail."""
    from parquet_decision.model import Operation, SchemaProposal
    from tests.verify_fixtures import approve, f, integer, write_source
    x, y = f('x'), f('y')
    snapshots = [write_source(tmp_path / 'sources', 0, (x,), [{'x': 1}]),
                 write_source(tmp_path / 'sources', 1, (x, y), [{'x': 2, 'y': 3}])]
    proposal = SchemaProposal((x, y), operations=(Operation(0, ('x',), integer(), integer(), 'identity'),
        Operation(0, ('y',), None, integer(), 'null_insert'), Operation(1, ('x',), integer(), integer(), 'identity'),
        Operation(1, ('y',), integer(), integer(), 'identity')))
    parts = [write_part(tmp_path / 'parts', 0, proposal, [{'x': 1, 'y': 999}]),
             write_part(tmp_path / 'parts', 1, proposal, [{'x': 2, 'y': 3}])]
    report = verifier().verify_streams(approve(snapshots, proposal), snapshots, parts)
    assert report.status == 'CONFLICT'
    assert 'source=0 row=0 path=["y"] expected null observation' in report.diagnostics
