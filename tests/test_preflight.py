"""Original well-formed Arrow-written fixtures, no parser-internals probes."""
from dataclasses import replace
from importlib import import_module, util
from pathlib import Path
import hashlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_decision.model import Limits, ApprovedPlan, ModelError, PreflightCounters


def preflight_module():
    assert util.find_spec('parquet_decision.preflight') is not None, 'complete supervised preflight is missing'
    return import_module('parquet_decision.preflight')


def write(root, name, fields, columns, *, metadata=None):
    root.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(fields, metadata=metadata)
    pq.write_table(pa.Table.from_arrays(columns, schema=schema), root / name, version='2.6')
    return Path(name)


def test_preflight_requires_all_rows(tmp_path):
    root = tmp_path / 'sources'
    tick = pa.timestamp('us', tz='UTC')
    a = write(root, 'a.parquet', [pa.field('x', tick)], [pa.array([1, 2, 3, 2**63 - 1], type=tick)])
    b = write(root, 'b.parquet', [pa.field('x', pa.timestamp('ns', tz='UTC'))], [pa.array([1], type=pa.timestamp('ns', tz='UTC'))])
    plan = preflight_module().preflight([a, b], root, replace(Limits(), batch_rows=2))
    assert plan.status == 'DATA_INCOMPATIBLE'
    assert not plan.preflight_complete
    assert plan.preflight_counters.rows == 4
    with pytest.raises(ModelError):
        ApprovedPlan(plan, ())


def test_metadata_decisions_only_after_full_success_and_empty_shards(tmp_path):
    root = tmp_path / 'sources'
    names = [write(root, 'a.parquet', [pa.field('x', pa.int16(), metadata={b'unit': b'm'})], [pa.array([1, 2, 3], type=pa.int16())]),
             write(root, 'b.parquet', [pa.field('x', pa.int32(), metadata={b'unit': b'cm'})], [pa.array([4], type=pa.int32())]),
             write(root, 'empty.parquet', [pa.field('x', pa.int8(), metadata={b'unit': b'm'})], [pa.array([], type=pa.int8())])]
    originals = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}
    plan = preflight_module().preflight(names, root, replace(Limits(), batch_rows=2))
    assert plan.status == 'NEEDS_DECISIONS'
    assert plan.preflight_complete
    assert plan.preflight_counters == PreflightCounters(3, 4, sum((root / p).stat().st_size for p in names))
    assert [s.num_rows for s in plan.sources] == [3, 1, 0]
    assert len(plan.proposal.conflicts) == 1
    assert originals == {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}


def test_empty_only_source_reconciles_zero_rows(tmp_path):
    root = tmp_path / 'sources'
    name = write(root, 'empty.parquet', [pa.field('x', pa.int64())], [pa.array([], type=pa.int64())])
    plan = preflight_module().preflight([name], root, Limits())
    assert plan.status == 'READY_FOR_REVIEW' and plan.preflight_complete
    assert plan.preflight_counters.rows == 0


def test_preflight_signed_unsigned_decimal_and_nested_null_parent(tmp_path):
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('id', pa.int64()), pa.field('obj', pa.struct([pa.field('t', pa.timestamp('us'))]))],
              [pa.array([-2**63, 1], type=pa.int64()), pa.array([{'t': 1}, None], type=pa.struct([pa.field('t', pa.timestamp('us'))]))])
    b = write(root, 'b.parquet', [pa.field('id', pa.uint64()), pa.field('obj', pa.struct([pa.field('t', pa.timestamp('ns')), pa.field('y', pa.string())]))],
              [pa.array([2**64 - 1], type=pa.uint64()), pa.array([{'t': 2, 'y': 'hello'}], type=pa.struct([pa.field('t', pa.timestamp('ns')), pa.field('y', pa.string())]))])
    plan = preflight_module().preflight([a, b], root, replace(Limits(), batch_rows=1))
    assert plan.status == 'READY_FOR_REVIEW'
    assert plan.proposal.fields[0].type.kind == 'decimal128'
    assert plan.preflight_counters.rows == 3


@pytest.mark.parametrize('key', [b'ARROW:extension:name', b'ARROW:extension:metadata'])
@pytest.mark.parametrize('nested', [False, True])
def test_fresh_worker_rejects_ordinary_extension_metadata(tmp_path, key, nested):
    root = tmp_path / 'sources'
    field = pa.field('x', pa.int64(), metadata={key: b'original.benign'})
    if nested:
        field = pa.field('obj', pa.struct([field])); array = pa.array([{'x': 1}], type=field.type)
    else:
        array = pa.array([1], type=pa.int64())
    name = write(root, 'metadata.parquet', [field], [array])
    plan = preflight_module().preflight([name], root, Limits())
    assert plan.status == 'UNSUPPORTED'
    assert not plan.preflight_complete


@pytest.mark.parametrize("registered_parent", [False, True])
def test_fresh_worker_rejects_original_extension_written_files(tmp_path, registered_parent):
    # Writer-only benign extension. It is deliberately never registered in the
    # worker; no executable metadata or serialization internals are investigated.
    class OriginalExtension(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), 'parquet_decision.original_benign')
        def __arrow_ext_serialize__(self):
            return b'original-declarative-marker'
        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            raise AssertionError('caller extension hook must not enter a fresh worker')
    root = tmp_path / 'sources'; extension = OriginalExtension()
    arr = pa.ExtensionArray.from_storage(extension, pa.array([1, 2], type=pa.int64()))
    names = [write(root, 'extension.parquet', [pa.field('x', extension)], [arr]),
             write(root, 'nested-extension.parquet', [pa.field('obj', pa.struct([pa.field('x', extension)]))],
                   [pa.StructArray.from_arrays([arr], fields=[pa.field('x', extension)])])]
    if registered_parent:
        pa.register_extension_type(extension)
    try:
        for name in names:
            plan = preflight_module().preflight([name], root, Limits())
            assert plan.status == 'UNSUPPORTED'
            assert not plan.preflight_complete
    finally:
        if registered_parent:
            pa.unregister_extension_type(extension.extension_name)


def test_worker_ignores_caller_pythonpath_and_data_root_modules(tmp_path, monkeypatch):
    root = tmp_path / 'sources'
    name = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    marker = tmp_path / 'unexpected-code-marker'
    original = "from pathlib import Path\nPath(%r).write_text('caller code ran')\nraise RuntimeError('caller code')\n" % str(marker)
    for module in ('pyarrow.py', 'sitecustomize.py', 'usercustomize.py', 'parquet_decision.py'):
        (root / module).write_text(original)
    monkeypatch.setenv('PYTHONPATH', str(root))
    monkeypatch.setenv('PYTHONSTARTUP', str(root / 'sitecustomize.py'))
    assert preflight_module().preflight([name], root, Limits()).status == 'READY_FOR_REVIEW'
    assert not marker.exists()


@pytest.mark.parametrize('cap', ['max_rows', 'max_fields', 'max_metadata_bytes', 'max_input_bytes', 'max_output_bytes', 'max_staging_bytes'])
def test_preflight_reports_measured_caps_without_review_status(tmp_path, cap):
    root = tmp_path / 'sources'
    name = write(root, 'a.parquet', [pa.field('x', pa.int64(), metadata={b'key': b'value'})], [pa.array([1, 2])])
    values = {'max_rows': 1, 'max_fields': 1, 'max_metadata_bytes': 1, 'max_input_bytes': 1,
              'max_output_bytes': 1, 'max_staging_bytes': (root / name).stat().st_size}
    inputs = [name]
    if cap == 'max_fields':
        second = write(root, 'b.parquet', [pa.field('x', pa.int64())], [pa.array([3])]); inputs.append(second)
    plan = preflight_module().preflight(inputs, root, replace(Limits(), **{cap: values[cap]}))
    assert plan.status == 'LIMIT_EXCEEDED'
    assert not plan.preflight_complete
    assert plan.diagnostics


def test_unsupported_mixture_never_gets_metadata_decision_status(tmp_path):
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    b = write(root, 'b.parquet', [pa.field('x', pa.float64())], [pa.array([1.0])])
    assert preflight_module().preflight([a, b], root, Limits()).status == 'UNSUPPORTED'


def test_native_free_imports_in_new_interpreter(tmp_path):
    import subprocess
    result = subprocess.run([sys.executable, '-c', "import sys; sys.path.insert(0, sys.argv[1]); import parquet_decision.source, parquet_decision.preflight, parquet_decision.supervise; assert 'pyarrow' not in sys.modules", str(Path(__import__('parquet_decision').__file__).resolve().parents[1])],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('tick, expected', [(2**63 // 1000 - 1, 'READY_FOR_REVIEW'),
                                           ((2**63 - 1) // 1000, 'READY_FOR_REVIEW'),
                                           ((2**63 - 1) // 1000 + 1, 'DATA_INCOMPATIBLE'),
                                           (-(2**63 // 1000), 'READY_FOR_REVIEW'),
                                           (-(2**63 // 1000) - 1, 'DATA_INCOMPATIBLE')])
def test_integer_timestamp_scale_boundary_without_datetime(tmp_path, tick, expected):
    root = tmp_path / 'sources'; us, ns = pa.timestamp('us'), pa.timestamp('ns')
    a = write(root, 'a.parquet', [pa.field('x', us)], [pa.array([tick], type=us)])
    b = write(root, 'b.parquet', [pa.field('x', ns)], [pa.array([0], type=ns)])
    plan = preflight_module().preflight([a, b], root, Limits())
    assert plan.status == expected


def test_all_supported_primitive_families_and_decimal_scale(tmp_path):
    from decimal import Decimal
    root = tmp_path / 'sources'
    types_values = [(pa.null(), [None, None]), (pa.bool_(), [True, None]),
                    (pa.float32(), [-0.0, float('nan')]), (pa.string(), ['original π', None]),
                    (pa.binary(), [b'\x00\xff', None]), (pa.decimal256(50, 2), [Decimal('1.23'), None])]
    fields = [pa.field(f'x{i}', typ) for i, (typ, _) in enumerate(types_values)]
    a = write(root, 'a.parquet', fields, [pa.array(values, type=typ) for typ, values in types_values])
    fields2 = [pa.field(f'x{i}', pa.float64() if i == 2 else (pa.decimal256(52, 4) if i == 5 else typ))
               for i, (typ, _) in enumerate(types_values)]
    b = write(root, 'b.parquet', fields2, [pa.array(values, type=f.type) for f, (_, values) in zip(fields2, types_values)])
    plan = preflight_module().preflight([a, b], root, replace(Limits(), batch_rows=1))
    assert plan.status == 'READY_FOR_REVIEW'
    assert plan.preflight_counters.rows == 4


def test_missing_source_is_incomplete(tmp_path):
    root = tmp_path / 'sources'; root.mkdir()
    plan = preflight_module().preflight([Path('missing.parquet')], root, Limits())
    assert plan.status == 'INCOMPLETE' and not plan.preflight_complete


def test_duplicate_input_is_refused(tmp_path):
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    plan = preflight_module().preflight([a, a], root, Limits())
    assert plan.status == 'ERROR' and not plan.preflight_complete


def test_failed_terminal_worker_cannot_promote_leftover_review_artifact(tmp_path, monkeypatch):
    # The substitution is exactly the process/artifact boundary: a controlled
    # terminal failure after a genuine completed run must outweigh its artifact.
    module = preflight_module()
    import parquet_decision.supervise as supervise
    from parquet_decision.model import WorkerResult
    real_run = supervise.run_worker
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    def completed_then_error(request, limits):
        assert real_run(request, limits).status == 'COMPLETE'
        return WorkerResult(1, request.operation, 'ERROR', diagnostics=('controlled terminal failure',))
    monkeypatch.setattr(supervise, 'run_worker', completed_then_error)
    plan = module.preflight([a], root, Limits())
    assert plan.status == 'ERROR' and not plan.preflight_complete


def test_native_iterator_rejects_changed_snapshot_inside_qualified_worker(tmp_path, monkeypatch):
    # Primitive boundary test only. This does not confer caller object safety;
    # full production native paths use the real isolated run_worker above.
    import parquet_decision.supervise as supervise
    import parquet_decision.source as source
    from parquet_decision.model import Snapshot, SourceSpec, _fields_from_arrow
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1, 2, 3])])
    data = (root / a).read_bytes(); fields, meta = _fields_from_arrow(pq.read_schema(root / a))
    spec = SourceSpec(0, str(a), hashlib.sha256(data).hexdigest(), len(data), 3, fields, meta)
    stage = tmp_path / 'stage'; stage.mkdir()
    snap = source.snapshot_inventory([spec], root, stage, Limits())[0]
    monkeypatch.setattr(supervise, '_WORKER_READY', True)
    batches = list(source.iter_source_batches(snap, 2))
    assert [(b.first_row, b.rows) for b in batches] == [(0, 2), (2, 1)]
    snap.local_path.chmod(0o600); snap.local_path.write_bytes(b'changed benign snapshot bytes')
    with pytest.raises(ModelError, match='snapshot'):
        list(source.iter_source_batches(snap, 2))


@pytest.mark.parametrize('error_number', [12, 28, 122])
def test_native_known_os_resource_failure_has_limit_status(tmp_path, monkeypatch, error_number):
    import parquet_decision.supervise as supervise
    import parquet_decision.source as source
    import parquet_decision.model as model
    module = preflight_module()
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    stage = tmp_path / 'stage'; stage.mkdir()
    copies = source._snapshot_inputs([a], root, stage, Limits())
    monkeypatch.setattr(supervise, '_WORKER_READY', True)
    def allocation_or_disk_refusal(schema):
        raise OSError(error_number, 'original controlled resource refusal')
    monkeypatch.setattr(model, '_fields_from_arrow', allocation_or_disk_refusal)
    plan = module._native_scan(copies, stage, Limits())
    assert plan.status == 'LIMIT_EXCEEDED', plan.diagnostics
    assert not plan.preflight_complete


def test_preflight_above_protocol_file_count_is_a_limit_plan(tmp_path):
    root = tmp_path / 'sources'; root.mkdir()
    plan = preflight_module().preflight([Path(f'{i}.parquet') for i in range(257)], root, Limits())
    assert plan.status == 'LIMIT_EXCEEDED' and not plan.preflight_complete


def test_alpha_file_and_field_boundaries_are_complete(tmp_path):
    root = tmp_path / 'sources'
    fields = [pa.field(f'x{i}', pa.int64()) for i in range(4)]
    names = [write(root, f'{i:03d}.parquet', fields, [pa.array([i]) for _ in fields]) for i in range(128)]
    plan = preflight_module().preflight(names, root, Limits())
    assert plan.status == 'READY_FOR_REVIEW', plan.diagnostics
    assert len(plan.sources) == 128 and plan.preflight_counters.rows == 128
    # Every original declaration counts, even when all target names coincide.
    over_fields = write(root, '000.parquet', [*fields, pa.field('extra', pa.int64())], [pa.array([0]) for _ in range(5)])
    assert preflight_module().preflight(names, root, Limits()).status == 'LIMIT_EXCEEDED'
    assert preflight_module().preflight([*names, Path('129.parquet')], root, Limits()).status == 'LIMIT_EXCEEDED'


def test_alpha_row_boundary_is_scanned_then_above_cap_refused(tmp_path):
    root = tmp_path / 'sources'
    name = write(root, 'million.parquet', [pa.field('x', pa.int64())], [pa.array(range(1_000_000), type=pa.int64())])
    plan = preflight_module().preflight([name], root, Limits())
    assert plan.status == 'READY_FOR_REVIEW', plan.diagnostics
    assert plan.preflight_counters.rows == 1_000_000
    write(root, 'million.parquet', [pa.field('x', pa.int64())], [pa.array(range(1_000_001), type=pa.int64())])
    assert preflight_module().preflight([name], root, Limits()).status == 'LIMIT_EXCEEDED'


def test_missing_source_root_is_incomplete(tmp_path):
    plan = preflight_module().preflight([Path('a.parquet')], tmp_path / 'missing-root', Limits())
    assert plan.status == 'INCOMPLETE' and not plan.preflight_complete


@pytest.mark.parametrize('depth, expected', [(8, 'READY_FOR_REVIEW'), (9, 'LIMIT_EXCEEDED')])
def test_alpha_struct_depth_native_boundary(tmp_path, depth, expected):
    root = tmp_path / 'sources'
    typ, value = pa.int64(), 1
    for _ in range(depth):
        typ = pa.struct([pa.field('x', typ)]); value = {'x': value}
    a = write(root, 'nested.parquet', [pa.field('root', typ)], [pa.array([value], type=typ)])
    plan = preflight_module().preflight([a], root, Limits())
    assert plan.status == expected, plan.diagnostics


@pytest.mark.parametrize('size, expected', [(1_048_575, 'READY_FOR_REVIEW'), (1_048_576, 'LIMIT_EXCEEDED')])
def test_alpha_metadata_bytes_native_boundary(tmp_path, size, expected):
    root = tmp_path / 'sources'
    a = write(root, 'metadata.parquet', [pa.field('x', pa.int64(), metadata={b'k': b'v' * size})], [pa.array([1])])
    plan = preflight_module().preflight([a], root, Limits())
    assert plan.status == expected, plan.diagnostics


@pytest.mark.parametrize('number', [12, 28, 122, 13])
def test_parent_preflight_directory_creation_preserves_exact_os_cause(tmp_path, monkeypatch, number):
    module = preflight_module()
    root = tmp_path / 'sources'; root.mkdir()
    def refuse_directory(*args, **kwargs):
        raise OSError(number, 'original controlled parent preflight directory refusal')
    monkeypatch.setattr(module.tempfile, 'TemporaryDirectory', refuse_directory)
    plan = module.preflight([Path('a.parquet')], root, Limits())
    assert plan.status == ('LIMIT_EXCEEDED' if number in (12, 28, 122) else 'ERROR')
    assert not plan.preflight_complete
    assert all(len(item.encode()) <= 4096 for item in plan.diagnostics)


@pytest.mark.parametrize('number', [12, 28, 122, 13])
def test_parent_artifact_load_resource_failure_overrides_success(tmp_path, monkeypatch, number):
    module = preflight_module()
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    def refuse_artifact(*args, **kwargs):
        raise OSError(number, 'original controlled parent artifact-read refusal')
    monkeypatch.setattr(module, 'load_plan', refuse_artifact)
    plan = module.preflight([a], root, Limits())
    assert plan.status == ('LIMIT_EXCEEDED' if number in (12, 28, 122) else 'ERROR')
    assert not plan.preflight_complete
    assert 'RLIMIT_AS' not in ' '.join(plan.diagnostics)


@pytest.mark.parametrize('entry', ['preflight', 'run_worker'])
def test_parent_directory_cleanup_resource_failure_overrides_success(tmp_path, monkeypatch, entry):
    import parquet_decision.supervise as supervise
    from parquet_decision.model import WorkerRequest
    module = preflight_module()
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    stage = tmp_path / 'stage'; stage.mkdir()
    request = WorkerRequest(1, 'preflight', (str(a),),
                            (('source_root', str(root)), ('staging', str(stage)), ('destination', str(stage / 'plan.json'))))
    real_directory = module.tempfile.TemporaryDirectory; directories = []
    class ControlledCleanupFailure:
        def __init__(self, *args, **kwargs):
            self.real = real_directory(*args, **kwargs)
        def __enter__(self):
            directory = self.real.__enter__(); directories.append(Path(directory)); return directory
        def __exit__(self, *args):
            self.real.__exit__(*args)
            raise OSError(28, 'original controlled parent cleanup refusal')
    monkeypatch.setattr(module.tempfile, 'TemporaryDirectory', ControlledCleanupFailure)
    result = (module.preflight([a], root, Limits()) if entry == 'preflight' else supervise.run_worker(request, Limits()))
    assert result.status == 'LIMIT_EXCEEDED'
    if entry == 'preflight':
        assert not result.preflight_complete
    assert directories and all(not directory.exists() for directory in directories)


@pytest.mark.parametrize('identifier, expected', [('sub/a.parquet', 'COMPLETE'),
                                                 ('./sub/a.parquet', 'ERROR'),
                                                 ('sub//a.parquet', 'ERROR'),
                                                 ('sub/a.parquet/', 'ERROR'),
                                                 ('sub/../sub/a.parquet', 'ERROR')])
def test_raw_worker_source_identifiers_are_validated_before_path(tmp_path, identifier, expected):
    import parquet_decision.supervise as supervise
    from parquet_decision.model import WorkerRequest, load_plan
    root = tmp_path / 'sources'; (root / 'sub').mkdir(parents=True)
    pq.write_table(pa.table({'x': [1]}), root / 'sub/a.parquet')
    stage = tmp_path / 'stage'; stage.mkdir()
    request = WorkerRequest.from_dict({'protocol_version': 1, 'operation': 'preflight', 'paths': [identifier],
        'args': [['source_root', str(root)], ['staging', str(stage)], ['destination', str(stage / 'plan.json')]]})
    result = supervise.run_worker(request, Limits())
    assert result.status == expected
    if expected == 'COMPLETE':
        plan = load_plan(stage / 'plan.json', Limits())
        assert plan.sources[0].relative_path == identifier
    else:
        assert not (stage / 'plan.json').exists()
    assert pq.read_table(root / 'sub/a.parquet')['x'].to_pylist() == [1]


def test_worker_cleanup_resource_status_survives_leftover_review_artifact(tmp_path, monkeypatch):
    import parquet_decision.supervise as supervise
    from parquet_decision.model import load_plan
    module = preflight_module()
    root = tmp_path / 'sources'
    a = write(root, 'a.parquet', [pa.field('x', pa.int64())], [pa.array([1])])
    real_directory = module.tempfile.TemporaryDirectory
    real_worker = supervise.run_worker
    class WorkerCleanupFailure:
        def __init__(self, *args, **kwargs):
            self.is_worker = kwargs.get('prefix') == 'parquet-decision-worker-'
            self.real = real_directory(*args, **kwargs)
        def __enter__(self):
            return self.real.__enter__()
        def __exit__(self, *args):
            self.real.__exit__(*args)
            if self.is_worker:
                raise OSError(28, 'original controlled worker-only cleanup refusal')
    def observe_failed_worker(request, limits):
        result = real_worker(request, limits)
        assert result.status == 'LIMIT_EXCEEDED'
        assert load_plan(Path(dict(request.args)['destination']), limits).preflight_complete
        return result
    monkeypatch.setattr(module.tempfile, 'TemporaryDirectory', WorkerCleanupFailure)
    monkeypatch.setattr(supervise, 'run_worker', observe_failed_worker)
    plan = module.preflight([a], root, Limits())
    assert plan.status == 'LIMIT_EXCEEDED'
    assert not plan.preflight_complete
    assert 'cleanup' in ' '.join(plan.diagnostics)
