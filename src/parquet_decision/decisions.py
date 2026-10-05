"""Explicit metadata choices without scientific-semantic reconciliation.

Portable digests bind reviewed content, not operator authenticity. Approval is a
structural lifecycle gate; independently checking actual sources remains required
before any transformed dataset is published.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path

from .model import (ApprovedPlan, ModelError, Plan, Resolution, RuntimeInfo, UnsupportedError,
                    MAX_RESOLUTIONS, REVIEW_STATUSES, _array, _cap, _digest,
                    _int, _keys, canonical_json, read_json)

DISCLAIMER = 'Metadata provenance is preserved. Scientific semantic compatibility is not established.'


class DecisionConflictError(ModelError):
    """An explicit abort or missing, duplicate or stale choice refuses approval."""


def _check_coverage(plan: Plan, resolutions: list[Resolution]) -> None:
    expected = sorted(c.conflict_digest for c in plan.proposal.conflicts)
    actual = sorted(r.conflict_digest for r in resolutions)
    if actual != expected:
        raise DecisionConflictError('decisions require exact complete conflict coverage; missing, duplicate or stale resolutions refuse approval')


def _check_archived_canonical_metadata(plan: Plan) -> None:
    # The reviewed proposal already omits each conflicting entry. Never alter the
    # reviewed digest silently to fix a noncanonical externally edited proposal.
    for conflict in plan.proposal.conflicts:
        metadata = plan.proposal.schema_metadata
        fields = plan.proposal.fields
        for segment in conflict.path:
            field = next((f for f in fields if f.name == segment), None)
            if field is None:
                raise ModelError('conflict path is absent from canonical target schema')
            metadata = field.metadata
            fields = field.type.fields if field.type.kind == 'struct' else ()
        if any(key == conflict.key for key, _ in metadata):
            raise ModelError('canonical target retains conflicting metadata; regenerate and review the plan')


def approve(plan: Plan, resolutions: list[Resolution]) -> ApprovedPlan:
    """Approve only complete preflight with an explicit exact per-conflict list."""
    if not isinstance(plan, Plan) or type(resolutions) is not list:
        raise ModelError('approve requires a Plan and an explicit list of Resolution records')
    _cap(len(resolutions), 'resolution count', MAX_RESOLUTIONS)
    if any(not isinstance(r, Resolution) for r in resolutions):
        raise ModelError('invalid resolution record')
    if plan.status not in REVIEW_STATUSES or not plan.preflight_complete:
        raise DecisionConflictError('only complete successful review plans can be approved')
    # Revalidate the immutable contracts, including all counters and digests.
    Plan.from_dict(plan.to_dict())
    if plan.runtime != RuntimeInfo():
        raise UnsupportedError('approval requires the qualified runtime Linux/Python 3.12.14/PyArrow 25.0.0/package 0.1.0a1')
    _check_coverage(plan, resolutions)
    if any(r.action == 'abort' for r in resolutions):
        raise DecisionConflictError('explicit abort resolution refuses approval')
    _check_archived_canonical_metadata(plan)
    return ApprovedPlan(plan, tuple(sorted(resolutions, key=lambda r: r.conflict_digest)))


def load_resolutions(path: Path, plan: Plan) -> list[Resolution]:
    """Strict version-1 decisions: format_version, plan_digest, resolutions only."""
    if not isinstance(plan, Plan):
        raise ModelError('decisions require a strict Plan')
    value = _keys(read_json(path, plan.limits), ('format_version', 'plan_digest', 'resolutions'))
    _int(value['format_version'], 'decisions format_version', 1, 1)
    _digest(value['plan_digest'], 'plan_digest')
    if value['plan_digest'] != plan.plan_digest:
        raise DecisionConflictError('decisions plan digest does not match the reviewed plan digest')
    raw = _array(value['resolutions'], 'resolutions')
    _cap(len(raw), 'resolution count', MAX_RESOLUTIONS)
    return [Resolution.from_dict(r) for r in raw]


def _type_label(typ) -> str:
    if typ is None:
        return 'absent'
    if typ.kind in ('int', 'uint', 'float'):
        return typ.kind + str(typ.bit_width)
    if typ.kind.startswith('decimal'):
        return f'{typ.kind}({typ.precision},{typ.scale})'
    if typ.kind == 'timestamp':
        return f'timestamp({typ.unit}, timezone={json.dumps(typ.timezone, ensure_ascii=False)})'
    if typ.kind == 'struct':
        return f'struct({len(typ.fields)} fields)'
    return typ.kind


def _byte_preview(value: bytes | None) -> str:
    if value is None:
        return 'absent'
    try:
        return 'UTF-8 ' + json.dumps(value.decode('utf-8', errors='strict'), ensure_ascii=False)
    except UnicodeError:
        return 'base64 ' + json.dumps(base64.b64encode(value).decode('ascii'))


def render_plan(plan: Plan) -> str:
    """Complete human inspection, preserving literal segment arrays and bytes."""
    if not isinstance(plan, Plan):
        raise ModelError('render_plan requires a strict Plan')
    lines = [f'Plan status: {plan.status}', f'Plan digest: {plan.plan_digest}',
             DISCLAIMER, 'Structural convergence only; no unit conversion or inferred scientific compatibility.',
             f'Preflight complete: {str(plan.preflight_complete).lower()}',
             'Preflight counters: ' + canonical_json(plan.preflight_counters.to_dict()).decode(),
             'Runtime: ' + canonical_json(plan.runtime.to_dict()).decode(),
             'Limits: ' + canonical_json(plan.limits.to_dict()).decode(),
             'Sources (explicit inventory order; original schemas and exact base64 metadata):']
    for source in plan.sources:
        lines.append(f'Source {source.source_id}: {json.dumps(source.relative_path, ensure_ascii=False)}; rows {source.num_rows}; bytes {source.size_bytes}')
        lines.append('Source ' + canonical_json(source.to_dict()).decode())
    proposal = plan.proposal
    if proposal is not None:
        lines.append('Target schema (exact codepoint field order; paths are literal segment arrays):')
        def add_fields(fields, prefix=()):
            for field in fields:
                path = canonical_json([*prefix, field.name]).decode()
                lines.append('Target field ' + path + ' ' + canonical_json(field.to_dict()).decode())
                if field.type.kind == 'struct':
                    add_fields(field.type.fields, (*prefix, field.name))
        add_fields(proposal.fields)
        lines.append('Target schema metadata: ' + canonical_json(proposal.to_dict()['schema_metadata']).decode())
        lines.append('Operations (every source/path transition, including identity and absence):')
        for operation in proposal.operations:
            lines.append(f'Action: source {operation.source_id} {canonical_json(list(operation.path)).decode()} {operation.kind} {_type_label(operation.source_type)} -> {_type_label(operation.target_type)}')
            lines.append('Operation ' + canonical_json(operation.to_dict()).decode())
        lines.append('Metadata conflicts (path [] means schema metadata; values null means absent key):')
        for conflict in proposal.conflicts:
            lines.append('Metadata choice at ' + canonical_json(list(conflict.path)).decode() + ': key ' + _byte_preview(conflict.key))
            lines.append('Variants: ' + '; '.join(f'source {source_id}: {_byte_preview(value)}' for source_id, value in conflict.values))
            lines.append('Conflict ' + canonical_json(conflict.to_dict()).decode())
    for diagnostic in plan.diagnostics:
        lines.append('Diagnostic: ' + diagnostic)
    lines.append('Approval instructions:')
    if plan.status not in REVIEW_STATUSES or not plan.preflight_complete:
        lines.append('This plan cannot be approved. Complete successful preflight is required.')
    else:
        lines.extend(['Copy the following JSON to a new decisions file. For EVERY conflict, replace',
                      'CHOOSE_archive_only_OR_abort with your explicit archive_only or abort action.',
                      'archive_only omits the conflicting canonical entry and retains all original byte variants;',
                      'it does not reconcile scientific meaning. abort produces no approval.',
                      'An explicit empty resolutions array approves a successful zero-conflict plan.',
                      json.dumps({'format_version': 1, 'plan_digest': plan.plan_digest,
                                  'resolutions': [{'conflict_digest': c.conflict_digest, 'action': 'CHOOSE_archive_only_OR_abort'}
                                                  for c in proposal.conflicts]}, ensure_ascii=False, indent=2),
                      'Run: pdecision decide plan.json --decisions decisions.json --out approved.json',
                      'Choose a new output path. Optional --sources ROOT protects the portable source identifiers even if files are absent.',
                      'Approval is not verification; apply and verify perform separate complete source-bound checks.'])
    return '\n'.join(lines) + '\n'
