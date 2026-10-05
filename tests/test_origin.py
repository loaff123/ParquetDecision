"""Integrity labels and exact provenance on original trusted fixtures."""
from importlib import import_module, util
import json
import pytest
from parquet_decision.model import ModelError, canonical_json
from parquet_decision.bundle import apply
from tests.verify_fixtures import union_case


def origin():
    assert util.find_spec('parquet_decision.origin'), 'origin interface is missing'
    return import_module('parquet_decision.origin')


def bundle_case(tmp_path):
    approved, snapshots, _, _ = union_case(tmp_path)
    destination = tmp_path / 'result'
    assert apply(approved, snapshots[0].local_path.parent, destination).verified
    return destination, snapshots[0].local_path.parent


def test_origin_integrity_labels(tmp_path):
    module = origin()
    destination, root = bundle_case(tmp_path)
    record = module.lookup_origin(destination, 1, 0)
    assert record.integrity_level == 'OUTPUT_HASHES_MATCH'
    assert record.verification_label == 'HISTORICAL'
    assert record.source.relative_path == 'source-1.parquet'
    assert record.original_fields[2].type.fields[0].metadata == ((b'unit', b'cm'),)
    text = module.render_origin(record)
    assert 'cm' in text and 'OUTPUT_ONLY_CHECKED' in text
    assert 'Source value correctness was not checked' in text
    assert 'Historical verification' in text
    fresh = module.lookup_origin(destination, 1, 0, root)
    assert fresh.integrity_level == 'VERIFIED'
    assert fresh.verification_label == 'FRESH_SOURCE_BOUND'


@pytest.mark.parametrize('source_id,row', [(True, 0), (0, True), (-1, 0), (0, -1), (2, 0), (1, 1)])
def test_origin_coordinates_are_exact_integers_in_range(tmp_path, source_id, row):
    module = origin()
    destination, _ = bundle_case(tmp_path)
    with pytest.raises(ModelError):
        module.lookup_origin(destination, source_id, row)


@pytest.mark.parametrize('name', ['_decision.json', '_verification.json', '_manifest.json', 'part-00001.parquet'])
def test_origin_refuses_edited_bundle(tmp_path, name):
    module = origin()
    destination, _ = bundle_case(tmp_path)
    path = destination / name
    path.write_bytes(path.read_bytes() + b'edited')
    with pytest.raises(ModelError):
        module.lookup_origin(destination, 1, 0)


def test_source_bound_origin_checks_whole_inventory(tmp_path):
    module = origin()
    destination, root = bundle_case(tmp_path)
    (root / 'source-0.parquet').write_bytes(b'changed source other than selected origin')
    with pytest.raises(module.OriginCheckError) as caught:
        module.lookup_origin(destination, 1, 0, root)
    assert caught.value.status == 'CONFLICT'
    (root / 'source-0.parquet').unlink()
    with pytest.raises(module.OriginCheckError) as caught:
        module.lookup_origin(destination, 1, 0, root)
    assert caught.value.status == 'INCOMPLETE'


def test_origin_rebound_wrong_provenance_is_refused(tmp_path):
    module = origin()
    destination, _ = bundle_case(tmp_path)
    import hashlib
    from dataclasses import replace
    import pyarrow as pa
    import pyarrow.parquet as pq
    from parquet_decision.bundle import read_bundle, _manifest
    from parquet_decision.model import digest_json
    approved, parts, historical, _ = read_bundle(destination)
    path = destination / parts[1].filename
    table = pq.read_table(path)
    index = table.schema.get_field_index('__pc_source')
    table = table.set_column(index, table.schema.field(index), pa.array([0], type=pa.uint32()))
    pq.write_table(table, path)
    raw = path.read_bytes()
    parts[1] = replace(parts[1], bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    historical = replace(historical, output_digest=digest_json([p.to_dict() for p in parts]),
                         counters=tuple(sorted(dict(historical.counters, part_bytes=sum(p.bytes for p in parts)).items())))
    decision_raw, verification_raw = canonical_json(approved.to_dict()), canonical_json(historical.to_dict())
    (destination / '_verification.json').write_bytes(verification_raw)
    (destination / '_manifest.json').write_bytes(canonical_json(_manifest(approved, parts, historical, decision_raw, verification_raw)))
    with pytest.raises(module.OriginCheckError) as caught:
        module.lookup_origin(destination, 1, 0)
    assert caught.value.status == 'CONFLICT'


def test_corrupt_bundle_cli_is_conflict_but_bad_coordinate_is_invalid(tmp_path, capsys):
    from parquet_decision.cli import main
    destination, _ = bundle_case(tmp_path)
    assert main(['origin', str(destination), '--source-id', '-1', '--row', '0']) == 2
    capsys.readouterr()
    (destination / '_manifest.json').write_bytes(b'edited benign manifest')
    assert main(['origin', str(destination), '--source-id', '1', '--row', '0']) == 1
    assert 'CONFLICT' in capsys.readouterr().out


def test_origin_failed_terminal_never_promotes_result(tmp_path, monkeypatch):
    from parquet_decision.model import WorkerResult
    module = origin(); destination, _ = bundle_case(tmp_path)
    real = module.run_worker
    def failed(request, limits):
        terminal = real(request, limits)
        assert terminal.status == 'COMPLETE'
        return WorkerResult(1, 'verify', 'ERROR', diagnostics=('controlled late original origin failure',))
    monkeypatch.setattr(module, 'run_worker', failed)
    with pytest.raises(module.OriginCheckError) as caught:
        module.lookup_origin(destination, 1, 0)
    assert caught.value.status == 'ERROR'


@pytest.mark.parametrize('source_bound', [False, True])
def test_origin_late_elapsed_limit_cannot_return_success(tmp_path, monkeypatch, source_bound):
    module = origin(); destination, root = bundle_case(tmp_path)
    import time
    clock = [0.0]
    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    name = 'verify_streams' if source_bound else '_check_output_provenance'
    real = getattr(module, name)
    def late(*args, **kwargs):
        result = real(*args, **kwargs)
        clock[0] = 301.0
        return result
    monkeypatch.setattr(module, name, late)
    with pytest.raises(ModelError):
        module.lookup_origin(destination, 1, 0, root if source_bound else None)
