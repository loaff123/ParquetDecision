from dataclasses import replace
import ast
from pathlib import Path
import pytest
from parquet_decision.model import Operation, TypeSpec, UnsupportedError
from tests.verify_fixtures import approve, derivation, f, integer, union_case, verifier, write_part, write_source


def test_actual_source_derivation_has_independent_expected_constants(tmp_path):
    approved, snapshots, _, _ = union_case(tmp_path)
    assert derivation().derive_expected_schema(snapshots) == approved.plan.proposal


@pytest.mark.parametrize('mutation', ['drop_field', 'change_type', 'hide_conflict', 'nullability', 'drop_operation',
                                      'change_operation', 'schema_metadata', 'field_metadata'])
def test_actual_source_derivation_overrules_plan(tmp_path, mutation):
    approved, snapshots, _, rows = union_case(tmp_path)
    p = approved.plan.proposal
    if mutation == 'drop_field':
        p = replace(p, fields=p.fields[1:], operations=tuple(o for o in p.operations if o.path != ('added',)))
    elif mutation == 'change_type':
        p = replace(p, fields=(*p.fields[:-1], replace(p.fields[-1], type=integer(64))),
                    operations=tuple(replace(o, target_type=integer(64), kind='widen') if o.path == ('z',) else o for o in p.operations))
    elif mutation == 'hide_conflict': p = replace(p, conflicts=())
    elif mutation == 'nullability': p = replace(p, fields=(*p.fields[:-1], replace(p.fields[-1], nullable=True)))
    elif mutation == 'drop_operation': p = replace(p, operations=p.operations[1:])
    elif mutation == 'change_operation': p = replace(p, operations=(replace(p.operations[0], source_type=TypeSpec('null')), *p.operations[1:]))
    elif mutation == 'schema_metadata': p = replace(p, schema_metadata=((b'producer', b'old'),))
    else: p = replace(p, fields=(replace(p.fields[0], metadata=((b'invented', b'metadata'),)), *p.fields[1:]))
    forged = approve(snapshots, p)
    parts = [write_part(tmp_path / 'forged', i, p, values) for i, values in enumerate(rows)]
    result = verifier().verify_streams(forged, snapshots, parts)
    assert result.status == 'CONFLICT', result
    assert not result.verified
    assert 'derived' in ' '.join(result.diagnostics)


@pytest.mark.parametrize('field_change', ['drop', 'type', 'nullable', 'metadata'])
def test_forged_original_source_records_cannot_waive_actual_schema(tmp_path, field_change):
    approved, snapshots, parts, _ = union_case(tmp_path)
    original = snapshots[0].source; fields = original.fields
    if field_change == 'drop': fields = fields[:-1]
    elif field_change == 'type': fields = (replace(fields[0], type=integer(64)), *fields[1:])
    elif field_change == 'nullable': fields = (replace(fields[0], nullable=True), *fields[1:])
    else: fields = (replace(fields[0], metadata=((b'new', b'value'),)), *fields[1:])
    snapshots[0] = replace(snapshots[0], source=replace(original, fields=fields))
    result = verifier().verify_streams(approve(snapshots, approved.plan.proposal), snapshots, parts)
    assert result.status == 'CONFLICT', result
    assert 'source=0' in ' '.join(result.diagnostics)


@pytest.mark.parametrize('left,right,target', [
    (integer(8), integer(8, True), integer(16)), (integer(16), integer(16, True), integer(32)),
    (integer(32), integer(32, True), integer(64)),
    (integer(64), integer(64, True), TypeSpec('decimal128', precision=20, scale=0)),
    (TypeSpec('decimal128', precision=38, scale=19), integer(64, True), TypeSpec('decimal256', precision=39, scale=19)),
    (TypeSpec('decimal256', precision=5, scale=2), integer(8), TypeSpec('decimal256', precision=5, scale=2)),
    (TypeSpec('float', bit_width=32), TypeSpec('float', bit_width=64), TypeSpec('float', bit_width=64)),
    (TypeSpec('timestamp', unit='ms', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='UTC'))])
def test_independent_type_union_constants(tmp_path, left, right, target):
    snapshots = [write_source(tmp_path / 'sources', 0, (f('x', left, False),), []),
                 write_source(tmp_path / 'sources', 1, (f('x', right, False),), [])]
    result = derivation().derive_expected_schema(snapshots)
    assert result.fields == (f('x', target, False),)
    assert result.operations == tuple(Operation(i, ('x',), t, target, 'identity' if t == target else 'widen') for i, t in enumerate((left, right)))


@pytest.mark.parametrize('left,right', [
    (integer(), TypeSpec('float', bit_width=64)),
    (TypeSpec('decimal128', precision=10, scale=0), TypeSpec('float', bit_width=32)),
    (TypeSpec('bool'), integer()), (TypeSpec('binary'), TypeSpec('string')),
    (TypeSpec('timestamp', unit='ms', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='Etc/UTC')),
    (TypeSpec('decimal256', precision=76, scale=76), integer())])
def test_independent_unsupported_unions(tmp_path, left, right):
    snapshots = [write_source(tmp_path / 'sources', 0, (f('x', left),), []), write_source(tmp_path / 'sources', 1, (f('x', right),), [])]
    with pytest.raises(UnsupportedError): derivation().derive_expected_schema(snapshots)


def test_verifier_dependency_direction_and_no_whole_file_reads():
    verifier(); derivation()
    root = Path(__import__('parquet_decision').__file__).resolve().parents[1] / 'parquet_decision'
    for name in ('verify_schema.py', 'verify_values.py'):
        for node in ast.walk(ast.parse((root / name).read_text())):
            if isinstance(node, ast.ImportFrom): assert (node.module or '').split('.')[-1] not in {'schema', 'decisions', 'preflight', 'transform'}
            if isinstance(node, ast.Import): assert not any(n.name.split('.')[-1] in {'schema', 'decisions', 'preflight', 'transform'} for n in node.names)
            if isinstance(node, ast.Attribute): assert node.attr not in {'read_table', 'combine_chunks', 'unify_schemas'}


def test_null_absent_struct_and_metadata_applicability(tmp_path):
    from parquet_decision.model import Conflict, SchemaProposal
    # Empty and dotted names are literal; null/missing parents do not invent
    # child metadata variants or widen child nullability.
    child = f('', integer(16), False, ((b'\xff', b'\x00'),))
    parent = f('a', TypeSpec('struct', fields=(child,)), False)
    literal = f('a.', integer(32), False, ((b'', b''),))
    snapshots = [write_source(tmp_path / 'sources', 0, (parent, literal), [], ((b'common', b'v'), (b'optional', b''))),
                 write_source(tmp_path / 'sources', 1, (f('a', TypeSpec('null')), literal), [], ((b'common', b'v'),)),
                 write_source(tmp_path / 'sources', 2, (literal,), [], ((b'common', b'v'),))]
    expected_fields = (replace(parent, nullable=True), literal)
    expected = SchemaProposal(expected_fields, ((b'common', b'v'),), (
        Operation(0, ('a',), parent.type, parent.type, 'identity'),
        Operation(0, ('a', ''), child.type, child.type, 'identity'),
        Operation(0, ('a.',), literal.type, literal.type, 'identity'),
        Operation(1, ('a',), TypeSpec('null'), parent.type, 'null_insert'),
        Operation(1, ('a.',), literal.type, literal.type, 'identity'),
        Operation(2, ('a',), None, parent.type, 'null_insert'),
        Operation(2, ('a.',), literal.type, literal.type, 'identity')),
        (Conflict((), b'optional', ((0, b''), (1, None), (2, None))),))
    assert derivation().derive_expected_schema(snapshots) == expected


@pytest.mark.parametrize('component', ['python', 'pyarrow', 'platform', 'package_version'])
def test_verification_refuses_changed_runtime_binding(tmp_path, component):
    from tests.verify_fixtures import simple_case
    approved, snapshots, parts, _ = simple_case(tmp_path)
    forged_plan = replace(approved.plan, runtime=replace(approved.plan.runtime, **{component: 'unqualified'}), plan_digest='')
    forged = replace(approved, plan=forged_plan, approval_digest='')
    report = verifier().verify_streams(forged, snapshots, parts)
    assert report.status == 'UNSUPPORTED', report


def test_public_verifier_imports_are_native_free(tmp_path):
    import subprocess, sys
    root = Path(__import__('parquet_decision').__file__).resolve().parents[1]
    code = 'import sys; sys.path.insert(0, sys.argv[1]); import parquet_decision.verify_schema, parquet_decision.verify_values; assert "pyarrow" not in sys.modules'
    result = subprocess.run([sys.executable, '-I', '-S', '-c', code, str(root)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_actual_source_schema_metadata_is_independently_checked(tmp_path):
    approved, snapshots, parts, _ = union_case(tmp_path)
    snapshots[0] = replace(snapshots[0], source=replace(snapshots[0].source, schema_metadata=()))
    forged = approve(snapshots, approved.plan.proposal)
    report = verifier().verify_streams(forged, snapshots, parts)
    assert report.status == 'CONFLICT' and 'source=0' in ' '.join(report.diagnostics)


def test_upstream_plan_matches_hand_written_and_independently_derived_contract(tmp_path):
    from parquet_decision.preflight import preflight
    from parquet_decision.model import ApprovedPlan, Limits, Resolution
    expected, snapshots, parts, _ = union_case(tmp_path)
    planned = preflight([Path(s.source.relative_path) for s in snapshots], tmp_path / 'sources', Limits())
    assert planned.proposal == expected.plan.proposal
    assert derivation().derive_expected_schema(snapshots) == expected.plan.proposal
    approved = ApprovedPlan(planned, tuple(Resolution(c.conflict_digest, 'archive_only')
                                          for c in sorted(planned.proposal.conflicts, key=lambda c: c.conflict_digest)))
    assert verifier().verify_streams(approved, snapshots, parts).verified
