from decimal import Decimal, localcontext
import pytest
from parquet_decision.model import Operation, SchemaProposal, TypeSpec
from tests.verify_fixtures import approve, f, integer, simple_case, union_case, verifier, write_part, write_source


def test_complete_target_values(tmp_path):
    approved, snapshots, parts, _ = union_case(tmp_path)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.verified, report
    assert dict(report.counters)['rows'] == 4
    assert report.plan_digest == approved.plan.plan_digest
    assert report.approval_digest == approved.approval_digest
    assert report.source_digest and report.output_digest and report.checked_at_utc


@pytest.mark.parametrize('source_batch', [1, 3, 7])
@pytest.mark.parametrize('output_batch', [2, 5, 8])
def test_chunk_boundary_neutrality(tmp_path, source_batch, output_batch):
    approved, snapshots, parts, _ = simple_case(tmp_path, count=23, output_groups=output_batch)
    report = verifier().verify_streams(approved, snapshots, parts, source_batch_rows=source_batch, output_batch_rows=output_batch)
    assert report.verified, report
    assert dict(report.counters)['rows'] == 23
    assert dict(report.counters)['values'] == 23


@pytest.mark.parametrize('count', [0, 1, 19])
def test_empty_and_all_null_sources(tmp_path, count):
    field = f('x', TypeSpec('null')); rows = [{'x': None}] * count
    snapshots = [write_source(tmp_path / 'sources', 0, (field,), rows)]
    proposal = SchemaProposal((field,), operations=(Operation(0, ('x',), field.type, field.type, 'identity'),))
    parts = [write_part(tmp_path / 'parts', 0, proposal, rows)]
    assert verifier().verify_streams(approve(snapshots, proposal), snapshots, parts).verified


@pytest.mark.parametrize('bits', [8, 16, 32, 64])
@pytest.mark.parametrize('unsigned', [False, True])
def test_exact_logical_values_integer_boundaries(tmp_path, bits, unsigned):
    typ = integer(bits, unsigned)
    values = [0, 2**bits - 1] if unsigned else [-2**(bits - 1), 2**(bits - 1) - 1]
    rows = [{'x': n} for n in values]; field = f('x', typ, False)
    snapshots = [write_source(tmp_path / 'sources', 0, (field,), rows)]
    proposal = SchemaProposal((field,), operations=(Operation(0, ('x',), typ, typ, 'identity'),))
    parts = [write_part(tmp_path / 'parts', 0, proposal, rows)]
    assert verifier().verify_streams(approve(snapshots, proposal), snapshots, parts).verified


@pytest.mark.parametrize('source_type,target_type,values,expected', [
    (integer(64, True), TypeSpec('decimal128', precision=20, scale=0), [0, 2**64-1], [Decimal(0), Decimal(2**64-1)]),
    (integer(64), TypeSpec('decimal128', precision=20, scale=0), [-2**63, 2**63-1], [Decimal(-2**63), Decimal(2**63-1)]),
    (TypeSpec('decimal128', precision=38, scale=19), TypeSpec('decimal256', precision=39, scale=19), [Decimal('9999999999999999999.1234567890123456789')], [Decimal('9999999999999999999.1234567890123456789')]),
    (TypeSpec('decimal128', precision=10, scale=2), TypeSpec('decimal128', precision=12, scale=4), [Decimal('-12345678.91'), Decimal('0.01')], [Decimal('-12345678.9100'), Decimal('0.0100')]),
    (TypeSpec('float', bit_width=32), TypeSpec('float', bit_width=64), [0.1, 1.5, -0.0, float('nan'), float('inf'), -float('inf'), None], [0.10000000149011612, 1.5, -0.0, -float('nan'), float('inf'), -float('inf'), None]),
    (TypeSpec('timestamp', unit='ms', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='UTC'), [-9223372036854, 0, 9223372036854], [-9223372036854000000, 0, 9223372036854000000]),
    (TypeSpec('timestamp', unit='ns', timezone='UTC'), TypeSpec('timestamp', unit='ns', timezone='UTC'), [-2**63, 1700000000000000001, 2**63-1], [-2**63, 1700000000000000001, 2**63-1])])
def test_exact_logical_values_widening(tmp_path, source_type, target_type, values, expected):
    other = target_type
    if source_type == integer(64, True): other = integer(64)
    if source_type == integer(64): other = integer(64, True)
    snapshots = [write_source(tmp_path / 'sources', 0, (f('x', source_type),), [{'x': v} for v in values]),
                 write_source(tmp_path / 'sources', 1, (f('x', other),), [])]
    proposal = SchemaProposal((f('x', target_type),), operations=tuple(Operation(i, ('x',), t, target_type, 'identity' if t == target_type else 'widen') for i, t in enumerate((source_type, other))))
    parts = [write_part(tmp_path / 'parts', 0, proposal, [{'x': v} for v in expected]), write_part(tmp_path / 'parts', 1, proposal, [])]
    with localcontext() as ctx:
        ctx.prec = 3
        report = verifier().verify_streams(approve(snapshots, proposal), snapshots, parts)
    assert report.verified, report


@pytest.mark.parametrize('kind,values', [('string', ['', 'µ\x00z', None]), ('binary', [b'', b'\x00\xff', None]), ('bool', [False, True, None])])
def test_exact_primitive_logical_values(tmp_path, kind, values):
    typ = TypeSpec(kind); field = f('x', typ); rows = [{'x': v} for v in values]
    snapshots = [write_source(tmp_path / 'sources', 0, (field,), rows)]
    proposal = SchemaProposal((field,), operations=(Operation(0, ('x',), typ, typ, 'identity'),))
    parts = [write_part(tmp_path / 'parts', 0, proposal, rows)]
    assert verifier().verify_streams(approve(snapshots, proposal), snapshots, parts).verified


@pytest.mark.parametrize('terminal', ['ERROR', 'CANCELLED', 'INCOMPLETE', 'LIMIT_EXCEEDED', 'CONFLICT'])
def test_late_terminal_failure_overrides_leftover_success(tmp_path, monkeypatch, terminal):
    from dataclasses import replace
    from parquet_decision import supervise
    approved, snapshots, parts, _ = simple_case(tmp_path)
    original = supervise.run_worker
    def late_failure(request, limits):
        result = original(request, limits)
        assert result.status == 'COMPLETE'
        assert __import__('pathlib').Path(dict(request.args)['destination']).exists()
        return replace(result, status=terminal, diagnostics=('controlled late failure',))
    monkeypatch.setattr(supervise, 'run_worker', late_failure)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == terminal and not report.verified


@pytest.mark.parametrize('change', ['missing', 'invalid', 'request_binding', 'counter_binding', 'coherent_wrong_counters', 'approval_binding'])
def test_terminal_artifact_bindings(tmp_path, monkeypatch, change):
    from dataclasses import replace
    from pathlib import Path
    from parquet_decision import supervise
    from parquet_decision.model import canonical_json, parse_json
    approved, snapshots, parts, _ = simple_case(tmp_path)
    original = supervise.run_worker
    def changed_result(request, limits):
        result = original(request, limits)
        assert result.status == 'COMPLETE'
        path = Path(dict(request.args)['destination'])
        envelope = parse_json(path.read_bytes())
        if change == 'missing': path.unlink()
        elif change == 'invalid': path.write_text('{}')
        elif change == 'request_binding':
            envelope['request_digest'] = '0' * 64; path.write_bytes(canonical_json(envelope))
        elif change == 'counter_binding':
            result = replace(result, counters=tuple((k, v+1 if k == 'rows' else v) for k, v in result.counters))
        elif change == 'coherent_wrong_counters':
            result = replace(result, counters=tuple((k, v+1 if k == 'rows' else v) for k, v in result.counters))
            envelope['result']['counters'] = [[k, v+1 if k == 'rows' else v] for k, v in envelope['result']['counters']]
            path.write_bytes(canonical_json(envelope))
        elif change == 'approval_binding':
            envelope['result']['approval_digest'] = '0' * 64; path.write_bytes(canonical_json(envelope))
        return result
    monkeypatch.setattr(supervise, 'run_worker', changed_result)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'ERROR', report


@pytest.mark.parametrize('failure', ['read_memory', 'read_enomem', 'cleanup_enomem'])
def test_parent_resource_failure_preserved(tmp_path, monkeypatch, failure):
    import errno
    from parquet_decision import verify_values as module
    approved, snapshots, parts, _ = simple_case(tmp_path)
    if failure.startswith('read_'):
        original = module._read_private
        def failing_read(path, limits):
            if path.name == 'result.json':
                if failure == 'read_memory': raise MemoryError('original controlled allocation refusal')
                raise OSError(errno.ENOMEM, 'original controlled resource refusal')
            return original(path, limits)
        monkeypatch.setattr(module, '_read_private', failing_read)
    else:
        original = module.tempfile.TemporaryDirectory
        class ControlledCleanup(original):
            def __exit__(self, *args):
                super().__exit__(*args)
                raise OSError(errno.ENOMEM, 'original controlled cleanup refusal')
        monkeypatch.setattr(module.tempfile, 'TemporaryDirectory', ControlledCleanup)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'LIMIT_EXCEEDED', report


def test_direct_native_verifier_helpers_require_fresh_worker(tmp_path):
    from parquet_decision.model import ModelError
    from tests.verify_fixtures import derivation
    approved, snapshots, parts, _ = simple_case(tmp_path)
    with pytest.raises(ModelError, match='fresh qualified worker'):
        verifier()._verify_streams(approved, snapshots, parts)
    with pytest.raises(ModelError, match='fresh qualified worker'):
        derivation()._derive_expected_schema(snapshots, approved.plan.limits)


@pytest.mark.parametrize('mode', ['null_parent', 'missing_parent'])
def test_null_and_absent_parent_are_not_valid_all_null_structs(tmp_path, mode):
    from dataclasses import replace
    parent = f('p', TypeSpec('struct', fields=(f('x', integer(16), False),)))
    left_fields = (f('p', TypeSpec('null')),) if mode == 'null_parent' else (f('z', integer(8)),)
    right_fields = (parent,) if mode == 'null_parent' else (parent, f('z', integer(8)))
    left_rows = [{'p': None}] if mode == 'null_parent' else [{'z': 1}]
    snapshots = [write_source(tmp_path / 'sources', 0, left_fields, left_rows),
                 write_source(tmp_path / 'sources', 1, right_fields, [{'p': {'x': 7}, 'z': 2}])]
    target = (parent,) if mode == 'null_parent' else right_fields
    ops = [Operation(0, ('p',), TypeSpec('null') if mode == 'null_parent' else None, parent.type, 'null_insert')]
    if mode == 'missing_parent': ops.append(Operation(0, ('z',), integer(8), integer(8), 'identity'))
    ops += [Operation(1, ('p',), parent.type, parent.type, 'identity'), Operation(1, ('p', 'x'), integer(16), integer(16), 'identity')]
    if mode == 'missing_parent': ops.append(Operation(1, ('z',), integer(8), integer(8), 'identity'))
    proposal = SchemaProposal(target, operations=tuple(ops))
    rows = [[{'p': None, 'z': 1}], [{'p': {'x': 7}, 'z': 2}]]
    parts = [write_part(tmp_path / 'parts', i, proposal, row) for i, row in enumerate(rows)]
    approved = approve(snapshots, proposal)
    assert verifier().verify_streams(approved, snapshots, parts).verified
    parts[0] = write_part(tmp_path / 'parts', 0, proposal, [{'p': {'x': 999}, 'z': 1}])
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'CONFLICT' and 'path=["p"]' in ' '.join(report.diagnostics)


def test_hidden_timestamp_child_under_null_parent_is_unobserved(tmp_path):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dataclasses import replace
    from parquet_decision.model import schema_to_arrow
    coarse = TypeSpec('timestamp', unit='us', timezone='UTC')
    fine = TypeSpec('timestamp', unit='ns', timezone='UTC')
    a = f('p', TypeSpec('struct', fields=(f('t', coarse),)))
    b = f('p', TypeSpec('struct', fields=(f('t', fine),)))
    snapshots = [write_source(tmp_path / 'sources', 0, (a,), [{'p': None}, {'p': {'t': 1700000000000000}}]),
                 write_source(tmp_path / 'sources', 1, (b,), [])]
    # Construct original benign hidden int64-max child under a null parent.
    array = pa.StructArray.from_arrays([pa.array([2**63-1, 1700000000000000], type=pa.timestamp('us', tz='UTC'))],
                                      fields=list(schema_to_arrow((a,)).field(0).type), mask=pa.array([True, False]))
    table = pa.Table.from_arrays([array], schema=schema_to_arrow((a,)))
    path = snapshots[0].local_path
    pq.write_table(table, path, version='2.6', compression='NONE', use_dictionary=False)
    raw = path.read_bytes()
    snapshots[0] = replace(snapshots[0], source=replace(snapshots[0].source, sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw)))
    proposal = SchemaProposal((b,), operations=(Operation(0, ('p',), a.type, b.type, 'struct_union'),
        Operation(0, ('p', 't'), coarse, fine, 'widen'), Operation(1, ('p',), b.type, b.type, 'identity'),
        Operation(1, ('p', 't'), fine, fine, 'identity')))
    parts = [write_part(tmp_path / 'parts', 0, proposal, [{'p': None}, {'p': {'t': 1700000000000000000}}]),
             write_part(tmp_path / 'parts', 1, proposal, [])]
    assert verifier().verify_streams(approve(snapshots, proposal), snapshots, parts).verified


def test_integer_decimal_coefficients_ignore_global_decimal_context():
    from decimal import Decimal, localcontext
    value = Decimal('-12345678901234567890123456789012345678.90123456789012345678901234567890123456')
    with localcontext() as context:
        context.prec = 2
        assert verifier()._coefficient(value, 38) == -1234567890123456789012345678901234567890123456789012345678901234567890123456


@pytest.mark.parametrize('limit_name', ['max_output_bytes', 'max_staging_bytes'])
def test_verification_counts_lowered_byte_limits(tmp_path, limit_name):
    from dataclasses import replace
    approved, snapshots, parts, _ = __import__('tests.verify_fixtures', fromlist=['simple_case']).simple_case(tmp_path)
    value = sum(p.bytes for p in parts)-1 if limit_name == 'max_output_bytes' else 1
    plan = replace(approved.plan, limits=replace(approved.plan.limits, **{limit_name: value}), plan_digest='')
    approved = replace(approved, plan=plan, approval_digest='')
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'LIMIT_EXCEEDED' and not report.verified


@pytest.mark.parametrize('which', ['source_batch_rows', 'output_batch_rows'])
def test_batch_override_cannot_raise_approved_cap(tmp_path, which):
    approved, snapshots, parts, _ = __import__('tests.verify_fixtures', fromlist=['simple_case']).simple_case(tmp_path, source_batch_rows=1)
    assert verifier().verify_streams(approved, snapshots, parts, **{which: 2}).status == 'CONFLICT'


def test_total_dataset_byte_limit_includes_existing_bundle_metadata(tmp_path):
    from dataclasses import replace
    from tests.verify_fixtures import simple_case
    approved, snapshots, parts, _ = simple_case(tmp_path)
    metadata = b'original benign placeholder for later bundle integration'
    (parts[0].local_path.parent / '_manifest.json').write_bytes(metadata)
    total = sum(p.bytes for p in parts) + len(metadata)
    plan = replace(approved.plan, limits=replace(approved.plan.limits, max_output_bytes=total-1), plan_digest='')
    report = verifier().verify_streams(replace(approved, plan=plan, approval_digest=''), snapshots, parts)
    assert report.status == 'LIMIT_EXCEEDED', report


def test_timestamp_overflow_cannot_be_waived_by_forged_preflight(tmp_path):
    coarse = TypeSpec('timestamp', unit='us', timezone='UTC')
    fine = TypeSpec('timestamp', unit='ns', timezone='UTC')
    snapshots = [write_source(tmp_path / 'sources', 0, (f('x', coarse),), [{'x': 2**63-1}]),
                 write_source(tmp_path / 'sources', 1, (f('x', fine),), [])]
    proposal = SchemaProposal((f('x', fine),), operations=(Operation(0, ('x',), coarse, fine, 'widen'),
                                                         Operation(1, ('x',), fine, fine, 'identity')))
    parts = [write_part(tmp_path / 'parts', 0, proposal, [{'x': -1000}]), write_part(tmp_path / 'parts', 1, proposal, [])]
    report = verifier().verify_streams(approve(snapshots, proposal), snapshots, parts)
    assert report.status == 'CONFLICT', report
    assert 'source=0 row=0 path=["x"]' in ' '.join(report.diagnostics)
    assert 'overflow' in ' '.join(report.diagnostics)


@pytest.mark.parametrize('source_batch', [1, 3, 7])
@pytest.mark.parametrize('output_batch', [2, 5, 8])
def test_nested_absence_and_null_validity_across_irregular_chunks(tmp_path, source_batch, output_batch):
    approved, snapshots, parts, expected = union_case(tmp_path)
    original = [[{'z': 1, 'rec': {'x': 2}, 'n': None}, {'z': 3, 'rec': None, 'n': None}, {'z': 4, 'rec': {'x': None}, 'n': None}],
                [{'added': 6, 'n': 7, 'rec': {'x': 8, 'y': 9}, 'z': 10}]]
    snapshots = [write_source(tmp_path / 'sources', i, s.source.fields, original[i] * 9, s.source.schema_metadata)
                 for i, s in enumerate(snapshots)]
    parts = [write_part(tmp_path / 'parts', i, approved.plan.proposal, rows * 9) for i, rows in enumerate(expected)]
    report = verifier().verify_streams(approve(snapshots, approved.plan.proposal), snapshots, parts,
                                       source_batch_rows=source_batch, output_batch_rows=output_batch)
    assert report.verified, report
    assert dict(report.counters)['rows'] == 36 and dict(report.counters)['values'] == 198


def test_verification_never_changes_source_or_part_bytes(tmp_path):
    import hashlib
    approved, snapshots, parts, _ = union_case(tmp_path)
    paths = [s.local_path for s in snapshots] + [p.local_path for p in parts]
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]
    assert verifier().verify_streams(approved, snapshots, parts).verified
    assert before == [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]


@pytest.mark.parametrize('mutation', [
    'extra_report_counter', 'supervisor_counter_in_report',
    'missing_files', 'missing_rows', 'missing_input_bytes', 'missing_part_bytes', 'missing_values',
    'zero_values', 'excess_values', 'extra_terminal_counter',
    'missing_address_space_bytes', 'missing_cpu_seconds', 'missing_peak_rss_kib',
    'wrong_address_space_bytes', 'wrong_cpu_seconds'])
def test_success_counter_contract_rejects_coherent_result_mistakes(tmp_path, monkeypatch, mutation):
    """Mutate only result serialization after a real complete fixed worker."""
    from dataclasses import replace
    from pathlib import Path
    from parquet_decision import supervise
    from parquet_decision.model import canonical_json, parse_json
    approved, snapshots, parts, _ = simple_case(tmp_path, count=3)
    original = supervise.run_worker
    def mutate(request, limits):
        terminal = original(request, limits)
        assert terminal.status == 'COMPLETE'
        path = Path(dict(request.args)['destination'])
        envelope = parse_json(path.read_bytes())
        counters = dict(envelope['result']['counters'])
        terminal_counters = dict(terminal.counters)
        if mutation == 'extra_report_counter':
            counters['unrelated'] = terminal_counters['unrelated'] = 99
        elif mutation == 'supervisor_counter_in_report':
            counters['peak_rss_kib'] = terminal_counters['peak_rss_kib']
        elif mutation.startswith('missing_'):
            key = mutation.removeprefix('missing_')
            counters.pop(key, None)
            terminal_counters.pop(key)
        elif mutation == 'zero_values':
            counters['values'] = terminal_counters['values'] = 0
        elif mutation == 'excess_values':
            counters['values'] = terminal_counters['values'] = 4
        elif mutation == 'extra_terminal_counter':
            terminal_counters['unrelated'] = 99
        elif mutation.startswith('wrong_'):
            key = mutation.removeprefix('wrong_')
            terminal_counters[key] -= 1
        envelope['result']['counters'] = [list(pair) for pair in sorted(counters.items())]
        path.write_bytes(canonical_json(envelope))
        return replace(terminal, counters=tuple(sorted(terminal_counters.items())))
    monkeypatch.setattr(supervise, 'run_worker', mutate)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'ERROR' and not report.verified, report


def _counter_boundary_case(tmp_path, shape):
    if shape == 'empty_fields':
        fields, data, observations = (), [], 0
    elif shape == 'empty_rows':
        fields, data, observations = (f('x', integer(16)),), [], 0
    else:
        parent = f('p', TypeSpec('struct', fields=(f('a', integer(16)), f('b', integer(16)))))
        fields = (parent,)
        if shape == 'null_parents':
            data, observations = [{'p': None}] * 3, 3
        elif shape == 'valid_all_null_parents':
            data, observations = [{'p': {'a': None, 'b': None}}] * 3, 9
        else:
            data, observations = [{'p': None}, {'p': {'a': None, 'b': None}}, {'p': None}], 5
    operations = []
    for field in fields:
        operations.append(Operation(0, (field.name,), field.type, field.type, 'identity'))
        if field.type.kind == 'struct':
            operations.extend(Operation(0, (field.name, child.name), child.type, child.type, 'identity')
                              for child in field.type.fields)
    proposal = SchemaProposal(fields, operations=tuple(operations))
    snapshots = [write_source(tmp_path / 'sources', 0, fields, data)]
    parts = [write_part(tmp_path / 'parts', 0, proposal, data)]
    return approve(snapshots, proposal), snapshots, parts, observations


@pytest.mark.parametrize('shape', ['empty_fields', 'empty_rows', 'null_parents', 'valid_all_null_parents', 'mixed_parents'])
def test_success_counter_bounds_preserve_empty_and_nested_validity(tmp_path, shape):
    approved, snapshots, parts, expected = _counter_boundary_case(tmp_path, shape)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.verified, report
    assert dict(report.counters)['values'] == expected
    assert {name for name, _ in report.counters} == {'files', 'rows', 'input_bytes', 'part_bytes', 'values'}


@pytest.mark.parametrize('shape,bad_count', [
    ('empty_fields', 1), ('empty_rows', 1), ('null_parents', 2),
    ('valid_all_null_parents', 10), ('mixed_parents', 2), ('mixed_parents', 10)])
def test_success_counter_bounds_reject_impossible_empty_and_nested_counts(tmp_path, monkeypatch, shape, bad_count):
    from dataclasses import replace
    from pathlib import Path
    from parquet_decision import supervise
    from parquet_decision.model import canonical_json, parse_json
    approved, snapshots, parts, _ = _counter_boundary_case(tmp_path, shape)
    original = supervise.run_worker
    def mutate(request, limits):
        terminal = original(request, limits)
        assert terminal.status == 'COMPLETE'
        path = Path(dict(request.args)['destination'])
        envelope = parse_json(path.read_bytes())
        envelope['result']['counters'] = [[key, bad_count if key == 'values' else value]
                                         for key, value in envelope['result']['counters']]
        path.write_bytes(canonical_json(envelope))
        return replace(terminal, counters=tuple((key, bad_count if key == 'values' else value)
                                                for key, value in terminal.counters))
    monkeypatch.setattr(supervise, 'run_worker', mutate)
    report = verifier().verify_streams(approved, snapshots, parts)
    assert report.status == 'ERROR' and not report.verified, report
