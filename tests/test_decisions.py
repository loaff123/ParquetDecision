"""Explicit operator policy over the existing strict portable model."""
from dataclasses import replace
from importlib import import_module, util
import json

import pytest

from tests.fixtures import field, model, plan, schema, source


def decisions():
    assert util.find_spec('parquet_decision.decisions') is not None, 'explicit approval workflow is missing'
    return import_module('parquet_decision.decisions')


def conflict_plan():
    m = model()
    fields = lambda unit: (field('literal.dot', metadata=((b'unit', unit), (b'\xff', b'\x00'))),
                           field('obj', 'struct', fields=(field('x', metadata=((b'calibration', unit),)),)))
    sources = (source(0, fields(b'm'), ((b'producer', b'old'),)),
               source(1, fields(b'cm'), ((b'producer', b'new'),)))
    return m.Plan('NEEDS_DECISIONS', sources, schema().plan_schema(list(sources)), True,
                  preflight_counters=m.PreflightCounters(2, 2, 20))


def choices(p, action='archive_only'):
    return [model().Resolution(c.conflict_digest, action) for c in p.proposal.conflicts]


def test_approval_state_machine():
    d, m = decisions(), model()
    for p in (plan(), conflict_plan()):
        approved = d.approve(p, choices(p))
        assert approved.status == 'APPROVED'
        assert approved.plan == p
        assert approved.resolutions == tuple(sorted(choices(p), key=lambda r: r.conflict_digest))
        assert m.ApprovedPlan.from_dict(approved.to_dict()) == approved


@pytest.mark.parametrize('status', ['UNSUPPORTED', 'LIMIT_EXCEEDED', 'DATA_INCOMPATIBLE', 'INCOMPLETE', 'ERROR', 'CANCELLED'])
def test_approval_state_machine_refuses_failed_preflight(status):
    d, m = decisions(), model()
    p = replace(plan(), status=status, preflight_complete=False, plan_digest='')
    with pytest.raises(d.DecisionConflictError, match='complete successful'):
        d.approve(p, [])


def test_approval_state_machine_rechecks_edited_incomplete_counters():
    d, m = decisions(), model()
    # Original ordinary operator-edit case: a failure plan changes only status.
    raw = replace(plan(), status='INCOMPLETE', preflight_complete=False, plan_digest='').to_dict()
    raw.update(status='READY_FOR_REVIEW', preflight_complete=True, plan_digest='')
    raw['preflight_counters']['rows'] = 0
    raw['plan_digest'] = m.digest_json({k: v for k, v in raw.items() if k != 'plan_digest'})
    with pytest.raises(m.ModelError, match='counters'):
        m.Plan.from_dict(raw)


@pytest.mark.parametrize('which', ['missing', 'duplicate', 'stale', 'abort'])
def test_no_implicit_metadata_choice(which):
    d = decisions()
    p = conflict_plan()
    resolutions = choices(p)
    if which == 'missing':
        resolutions.pop()
    elif which == 'duplicate':
        resolutions.append(resolutions[0])
    elif which == 'stale':
        resolutions[0] = model().Resolution('0' * 64, 'archive_only')
    else:
        resolutions[0] = model().Resolution(resolutions[0].conflict_digest, 'abort')
    with pytest.raises(d.DecisionConflictError):
        d.approve(p, resolutions)


def test_no_implicit_metadata_choice_retains_all_original_bytes():
    d = decisions()
    p = conflict_plan()
    approved = d.approve(p, choices(p))
    assert approved.plan.sources == p.sources
    assert approved.plan.proposal.schema_metadata == ()
    assert approved.plan.proposal.fields[0].metadata == ((b'\xff', b'\x00'),)
    assert approved.plan.proposal.fields[1].type.fields[0].metadata == ()
    assert choices(p)  # Each conflict has an explicit choice, not a default.
    assert d.approve(plan(), []).resolutions == ()


def test_archive_refuses_plan_that_still_exposes_conflicting_canonical_metadata():
    d = decisions()
    p = conflict_plan()
    proposal = replace(p.proposal, schema_metadata=((b'producer', b'old'),))
    p = replace(p, proposal=proposal, plan_digest='')
    with pytest.raises(model().ModelError, match='canonical.*conflict|conflict.*canonical'):
        d.approve(p, choices(p))


def test_strict_decisions_file_binds_plan_digest(tmp_path):
    d, m = decisions(), model()
    p = conflict_plan()
    path = tmp_path / 'decisions.json'
    value = {'format_version': 1, 'plan_digest': p.plan_digest,
             'resolutions': [r.to_dict() for r in choices(p)]}
    path.write_bytes(m.canonical_json(value))
    assert d.load_resolutions(path, p) == choices(p)
    # Same conflict list, revised source byte inventory: exact plan binding matters.
    revised = replace(p, sources=(replace(p.sources[0], sha256='9' * 64), p.sources[1]), plan_digest='')
    assert revised.proposal.conflicts == p.proposal.conflicts
    with pytest.raises(d.DecisionConflictError, match='plan digest'):
        d.load_resolutions(path, revised)


@pytest.mark.parametrize('mutation', [
    lambda value: value.pop('plan_digest'),
    lambda value: value.update(format_version=True),
    lambda value: value.update(format_version=2),
    lambda value: value.update(yes=True),
    lambda value: value.update(resolutions={}),
    lambda value: value['resolutions'][0].update(action='yes'),
    lambda value: value['resolutions'][0].update(extra='no'),
])
def test_decision_file_has_no_blanket_yes_or_unknown_grammar(tmp_path, mutation):
    d, m = decisions(), model()
    p = conflict_plan()
    value = {'format_version': 1, 'plan_digest': p.plan_digest, 'resolutions': [r.to_dict() for r in choices(p)]}
    mutation(value)
    path = tmp_path / 'choices.json'
    path.write_text(json.dumps(value))
    with pytest.raises(m.ModelError):
        d.load_resolutions(path, p)


def test_approve_requires_explicit_list():
    d = decisions()
    with pytest.raises(model().ModelError, match='list'):
        d.approve(plan(), ())


@pytest.mark.parametrize('name,value', [('python', '3.12.13'), ('pyarrow', '24.0.0'), ('platform', 'darwin'), ('package_version', '0.1.0a0')])
def test_approval_refuses_unqualified_plan_runtime(name, value):
    d, m = decisions(), model()
    p = plan()
    p = replace(p, runtime=replace(p.runtime, **{name: value}), plan_digest='')
    with pytest.raises(m.UnsupportedError, match='qualified runtime'):
        d.approve(p, [])
