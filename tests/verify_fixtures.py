"""Original hand-specified fixtures; no planner/transform oracle."""
from dataclasses import replace
import hashlib
from importlib import import_module, util
import pyarrow as pa
import pyarrow.parquet as pq
from parquet_decision.model import (ApprovedPlan, Conflict, FieldSpec, Limits, Operation,
    PartRecord, Plan, PreflightCounters, Resolution, SchemaProposal, Snapshot,
    SourceSpec, TypeSpec, schema_digest, schema_to_arrow)


def verifier():
    assert util.find_spec('parquet_decision.verify_values'), 'independent stream verifier missing'
    return import_module('parquet_decision.verify_values')


def derivation():
    assert util.find_spec('parquet_decision.verify_schema'), 'independent actual-source derivation missing'
    return import_module('parquet_decision.verify_schema')


def integer(bits=64, unsigned=False):
    return TypeSpec('uint' if unsigned else 'int', bit_width=bits)


def f(name, typ=None, nullable=True, metadata=()):
    return FieldSpec(name, typ or integer(), nullable, metadata)


def write_source(root, source_id, fields, data, metadata=(), row_group_size=None):
    root.mkdir(exist_ok=True)
    path = root / f'source-{source_id}.parquet'
    table = pa.Table.from_pylist(data, schema=schema_to_arrow(tuple(fields), metadata))
    pq.write_table(table, path, row_group_size=row_group_size, version='2.6', compression='NONE', use_dictionary=False)
    raw = path.read_bytes()
    source = SourceSpec(source_id, path.name, hashlib.sha256(raw).hexdigest(), len(raw),
                        table.num_rows, tuple(fields), metadata)
    return Snapshot(source, path)


def approve(snapshots, proposal, limits=None):
    sources = tuple(s.source for s in snapshots)
    plan = Plan('NEEDS_DECISIONS' if proposal.conflicts else 'READY_FOR_REVIEW', sources,
                proposal, True, limits or Limits(), preflight_counters=PreflightCounters(
                    len(sources), sum(s.num_rows for s in sources), sum(s.size_bytes for s in sources)))
    return ApprovedPlan(plan, tuple(Resolution(c.conflict_digest, 'archive_only')
                                    for c in sorted(proposal.conflicts, key=lambda c: c.conflict_digest)))


def output_schema(proposal):
    return schema_to_arrow(proposal.fields, proposal.schema_metadata).append(
        pa.field('__pc_source', pa.uint32(), nullable=False)).append(
        pa.field('__pc_row', pa.uint64(), nullable=False))


def write_part(root, source_id, proposal, data, schema=None, row_group_size=None):
    root.mkdir(exist_ok=True)
    path = root / f'part-{source_id:05d}.parquet'
    schema = schema if schema is not None else output_schema(proposal)
    rows = [dict(row, __pc_source=row.get('__pc_source', source_id),
                 __pc_row=row.get('__pc_row', i)) for i, row in enumerate(data)]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, row_group_size=row_group_size, version='2.6', compression='NONE', use_dictionary=False)
    raw = path.read_bytes()
    return PartRecord(path.name, source_id, table.num_rows, len(raw), hashlib.sha256(raw).hexdigest(),
                      schema_digest(proposal.fields, proposal.schema_metadata), path)


def simple_case(tmp_path, *, count=11, source_batch_rows=8192, output_groups=None):
    field = f('x', integer(32), False)
    data = [{'x': i * 3 - 10} for i in range(count)]
    snapshots = [write_source(tmp_path / 'sources', 0, (field,), data)]
    proposal = SchemaProposal((field,), operations=(Operation(0, ('x',), field.type, field.type, 'identity'),))
    approved = approve(snapshots, proposal, replace(Limits(), batch_rows=source_batch_rows))
    parts = [write_part(tmp_path / 'parts', 0, proposal, data, row_group_size=output_groups)]
    return approved, snapshots, parts, data


def union_case(tmp_path):
    a_fields = (f('z', integer(16), False), f('rec', TypeSpec('struct', fields=(
        f('x', integer(8), metadata=((b'unit', b'm'),)),))), f('n', TypeSpec('null')))
    b_fields = (f('added', integer(32), False), f('n', integer(32)),
                f('rec', TypeSpec('struct', fields=(f('x', integer(16), metadata=((b'unit', b'cm'),)),
                                                   f('y', integer(32), False)))), f('z', integer(16), False))
    snapshots = [write_source(tmp_path / 'sources', 0, a_fields,
                             [{'z': 1, 'rec': {'x': 2}, 'n': None}, {'z': 3, 'rec': None, 'n': None},
                              {'z': 4, 'rec': {'x': None}, 'n': None}], ((b'producer', b'old'),)),
                 write_source(tmp_path / 'sources', 1, b_fields,
                              [{'added': 6, 'n': 7, 'rec': {'x': 8, 'y': 9}, 'z': 10}], ((b'producer', b'new'),))]
    target = (f('added', integer(32)), f('n', integer(32)),
              f('rec', TypeSpec('struct', fields=(f('x', integer(16)), f('y', integer(32))))), f('z', integer(16), False))
    operations = (
        Operation(0, ('added',), None, integer(32), 'null_insert'),
        Operation(0, ('n',), TypeSpec('null'), integer(32), 'null_insert'),
        Operation(0, ('rec',), a_fields[1].type, target[2].type, 'struct_union'),
        Operation(0, ('rec', 'x'), integer(8), integer(16), 'widen'),
        Operation(0, ('rec', 'y'), None, integer(32), 'null_insert'),
        Operation(0, ('z',), integer(16), integer(16), 'identity'),
        Operation(1, ('added',), integer(32), integer(32), 'identity'),
        Operation(1, ('n',), integer(32), integer(32), 'identity'),
        Operation(1, ('rec',), b_fields[2].type, target[2].type, 'struct_union'),
        Operation(1, ('rec', 'x'), integer(16), integer(16), 'identity'),
        Operation(1, ('rec', 'y'), integer(32), integer(32), 'identity'),
        Operation(1, ('z',), integer(16), integer(16), 'identity'))
    conflicts = (Conflict((), b'producer', ((0, b'old'), (1, b'new'))),
                 Conflict(('rec', 'x'), b'unit', ((0, b'm'), (1, b'cm'))))
    proposal = SchemaProposal(target, operations=operations, conflicts=conflicts)
    data = [[{'added': None, 'n': None, 'rec': {'x': 2, 'y': None}, 'z': 1},
             {'added': None, 'n': None, 'rec': None, 'z': 3},
             {'added': None, 'n': None, 'rec': {'x': None, 'y': None}, 'z': 4}],
            [{'added': 6, 'n': 7, 'rec': {'x': 8, 'y': 9}, 'z': 10}]]
    parts = [write_part(tmp_path / 'parts', i, proposal, rows) for i, rows in enumerate(data)]
    return approve(snapshots, proposal), snapshots, parts, data
