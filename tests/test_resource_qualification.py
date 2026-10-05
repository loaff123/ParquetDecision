"""Benign all-cap enforcement, counter and bounded-stream qualification."""
from dataclasses import replace
import json
from pathlib import Path
import pytest
from parquet_decision.model import Limits, LimitError, parse_json
from parquet_decision.transform import _Budget
from parquet_decision.source import _staging_bytes


def test_qualification_boundaries_are_honestly_documented():
    path = Path(__file__).resolve().parents[1] / 'docs' / 'qualification.md'
    assert path.is_file(), 'complete resource evidence labels are missing'
    text = path.read_text()
    for cap in Limits().to_dict():
        assert cap in text, 'unqualified cap: ' + cap
    for required in ['native workloads', 'counter', 'RSS', 'SIGSEGV', 'historical', 'never run']:
        assert required in text


@pytest.mark.parametrize('name', ['max_output_bytes', 'max_staging_bytes'])
def test_actual_alpha_byte_budget_counter_cap_and_cap_plus_one(tmp_path, name):
    dataset = tmp_path / 'dataset'; dataset.mkdir()
    budget = _Budget(Limits(), tmp_path, dataset)
    maximum = getattr(Limits(), name)
    budget.consume(maximum, output=name == 'max_output_bytes')
    assert getattr(budget, 'output_bytes' if name == 'max_output_bytes' else 'staging_bytes') == maximum
    with pytest.raises(LimitError): budget.consume(1, output=name == 'max_output_bytes')


def test_actual_alpha_staging_sparse_file_content_accounting(tmp_path):
    # Sparse ordinary file tests the file-content accounting contract, not a
    # giant native workload or allocation/performance behavior.
    path = tmp_path / 'sparse-counter'
    with path.open('xb') as stream: stream.truncate(Limits().max_staging_bytes)
    assert _staging_bytes(tmp_path, Limits()) == Limits().max_staging_bytes
    with path.open('r+b') as stream: stream.truncate(Limits().max_staging_bytes + 1)
    with pytest.raises(LimitError): _staging_bytes(tmp_path, Limits())


@pytest.mark.parametrize('maximum', [128, Limits().max_plan_bytes])
def test_json_byte_envelope_exact_boundary(maximum):
    raw = b'"' + b'x' * (maximum - 2) + b'"'
    assert len(parse_json(raw, max_bytes=maximum)) == maximum - 2
    with pytest.raises(LimitError): parse_json(raw + b' ', max_bytes=maximum)


def test_actual_alpha_input_inventory_byte_counter_boundary():
    from tests.fixtures import source
    from parquet_decision.model import validate_inventory
    original = source(0)
    at = replace(original, size_bytes=Limits().max_input_bytes)
    validate_inventory((at,), Limits())
    with pytest.raises(LimitError):
        validate_inventory((replace(at, size_bytes=at.size_bytes + 1),), Limits())


def test_report_limits_argument_cannot_relax_plan_contract(tmp_path):
    from tests.fixtures import plan
    from parquet_decision.artifacts import write_json_exclusive
    from parquet_decision.model import ModelError
    original = plan()
    lowered = replace(original, limits=replace(Limits(), max_plan_bytes=len(__import__('parquet_decision.model', fromlist=['canonical_json']).canonical_json(original.to_dict())) + 100), plan_digest='')
    with pytest.raises(ModelError):
        write_json_exclusive(lowered, tmp_path / 'new.json', [], limits=Limits())
    assert not (tmp_path / 'new.json').exists()
