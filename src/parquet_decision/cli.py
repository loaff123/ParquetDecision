"""Six-command trusted-local structural-convergence workflow."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from .artifacts import ArtifactConflictError, _destination, write_json_exclusive
from .decisions import DecisionConflictError, approve, load_resolutions, render_plan
from .model import (Limits, LimitError, ModelError, REVIEW_STATUSES, UnsupportedError,
                    _relative_path, load_approved_plan, load_plan, read_json)
from .preflight import preflight
from .bundle import apply, read_bundle
from .origin import OriginCheckError, lookup_origin, render_origin, verify_bundle

STATUS_EXITS = {'READY_FOR_REVIEW': 0, 'APPROVED': 0, 'VERIFIED': 0, 'INSPECTED': 0, 'OUTPUT_HASHES_MATCH': 0,
                'NEEDS_DECISIONS': 1, 'DATA_INCOMPATIBLE': 1, 'CONFLICT': 1,
                'UNSUPPORTED': 2, 'INVALID_INPUT': 2,
                'LIMIT_EXCEEDED': 3, 'INCOMPLETE': 3, 'CANCELLED': 3, 'ERROR': 4}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='pdecision', description='Trusted-local plan, inspect, decide, apply, verify and origin workflow')
    commands = parser.add_subparsers(dest='command', required=True)
    planning = commands.add_parser('plan', help='complete source feasibility and publish a review plan')
    planning.add_argument('--sources', type=Path, required=True)
    # Retain exact raw strings until strict portable-identifier validation.
    planning.add_argument('--input', action='append', required=True)
    planning.add_argument('--out', type=Path, required=True)
    planning.add_argument('--limits', type=Path, help='strict full Limits JSON; only lower alpha caps')
    inspection = commands.add_parser('inspect', help='show every operation/conflict and explicit choice instructions')
    inspection.add_argument('plan', type=Path)
    decision = commands.add_parser('decide', help='bind explicit per-conflict metadata actions to the plan')
    decision.add_argument('plan', type=Path)
    decision.add_argument('--decisions', type=Path, required=True)
    decision.add_argument('--out', type=Path, required=True)
    decision.add_argument('--sources', type=Path, help='optional root to protect even absent portable source identities')
    application = commands.add_parser('apply', help='publish a complete independently verified new bundle')
    application.add_argument('approved', type=Path)
    application.add_argument('--sources', type=Path, required=True)
    application.add_argument('--out', type=Path, required=True)
    verification = commands.add_parser('verify', help='fresh full source-bound schema/value/provenance check')
    verification.add_argument('approved', type=Path)
    verification.add_argument('--sources', type=Path, required=True)
    verification.add_argument('--dataset', type=Path, required=True)
    verification.add_argument('--report', type=Path, help='optional exclusive fresh report outside protected inputs/bundle')
    provenance = commands.add_parser('origin', help='original row interpretation; --sources enables full fresh verification')
    provenance.add_argument('dataset', type=Path)
    provenance.add_argument('--source-id', type=int, required=True)
    provenance.add_argument('--row', type=int, required=True)
    provenance.add_argument('--sources', type=Path)
    return parser


class _ReportingFailure(Exception):
    """A command output write/flush failed; never retry that output stream."""


def _emit(*args, **kwargs) -> None:
    try:
        print(*args, **kwargs, flush=True)
    except (OSError, MemoryError) as exc:
        raise _ReportingFailure() from exc


def _mute_stream(stream) -> None:
    # A failed buffered flush can retain pending bytes. Redirect the real stdio
    # descriptor before clearing those bytes, so interpreter shutdown cannot
    # replace the chosen exit with its own failed-flush status 120.
    sink = None
    try:
        descriptor = stream.fileno()
        sink = os.open(os.devnull, os.O_WRONLY)
        os.dup2(sink, descriptor)
        stream.flush()
    except (OSError, ValueError, AttributeError):
        pass
    finally:
        if sink is not None and sink != descriptor:
            os.close(sink)


def _report_output_failure(exc: _ReportingFailure, committed: Path | None) -> int:
    from .supervise import _os_error_status, _diagnostic
    cause = exc.__cause__
    status = 'LIMIT_EXCEEDED' if isinstance(cause, MemoryError) else _os_error_status(cause)
    _mute_stream(sys.stdout)
    context = (f'artifact already published: {committed}; inspect it before retrying'
               if committed is not None else 'no artifact publication was confirmed in this command')
    message = _diagnostic(f'{status}: stdout reporting failed: {cause}; {context}')
    try:
        print(message, file=sys.stderr, flush=True)
    except (OSError, MemoryError):
        _mute_stream(sys.stderr)
    return STATUS_EXITS[status]


def _status(status: str, message: str = '') -> int:
    _emit(status + (': ' + message if message else ''))
    return STATUS_EXITS[status]


def _model_error_status(exc: ModelError, *, missing_status: str = 'INVALID_INPUT') -> str:
    """Recover explicit reader causes without inspecting diagnostic wording."""
    from .supervise import _os_error_status
    cause, seen = exc, set()
    while isinstance(cause, ModelError) and cause.__cause__ is not None and id(cause) not in seen:
        seen.add(id(cause))
        cause = cause.__cause__
    if isinstance(cause, MemoryError):
        return 'LIMIT_EXCEEDED'
    if isinstance(cause, FileNotFoundError):
        return missing_status
    if isinstance(cause, OSError):
        return _os_error_status(cause)
    # A nonexistent requested JSON artifact remains invalid input, preserving
    # the reader's existing public behavior. Missing output parents are separate
    # direct FileNotFoundError / INCOMPLETE operation failures.
    return 'INVALID_INPUT'


def _inspect(path: Path) -> int:
    if path.is_dir():
        try:
            approved, parts, historical, manifest = read_bundle(path)
        except (LimitError, UnsupportedError):
            raise
        except ModelError as exc:
            status = _model_error_status(exc, missing_status='INCOMPLETE')
            raise OriginCheckError('CONFLICT' if status == 'INVALID_INPUT' else status, str(exc)) from exc
        _emit('Historical verification label: HISTORICAL')
        _emit('Stored verification status: ' + historical.status)
        _emit('Stored verification checked at: ' + historical.checked_at_utc)
        _emit('OUTPUT_HASHES_MATCH: Source value correctness was not checked.')
        _emit('Historical verification record: ' + __import__('json').dumps(historical.to_dict(), sort_keys=True))
        _emit(render_plan(approved.plan), end='')
        return _status('INSPECTED', 'bundle output inventory/hashes/cross-bindings checked; stored report is historical')
    raw = read_json(path, Limits())
    if type(raw) is dict and 'approval_digest' in raw:
        approved = load_approved_plan(path, Limits())
        _emit('Stored approval: APPROVED')
        _emit('Stored approval is not fresh source-bound verification.')
        _emit('Embedded historical reviewed plan follows; its original Plan status is retained.')
        _emit(render_plan(approved.plan), end='')
        _emit('Approval digest: ' + approved.approval_digest)
        for resolution in approved.resolutions:
            _emit('Stored resolution: ' + resolution.conflict_digest + ' ' + resolution.action)
        return _status('INSPECTED', 'stored approval loaded and rendered')
    plan = load_plan(path, Limits())
    _emit(render_plan(plan), end='')
    return _status('INSPECTED', 'plan loaded and rendered; displayed plan status is not this command status')


def main(argv: list[str] | None = None) -> int:
    """Supported graceful signals use the same cancellation/reaping boundary."""
    import signal
    import threading
    if threading.current_thread() is not threading.main_thread():
        return _main(argv)
    def cancel(signum, frame):
        raise KeyboardInterrupt
    old = signal.signal(signal.SIGTERM, cancel)
    try:
        return _main(argv)
    finally:
        signal.signal(signal.SIGTERM, old)


def _main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    committed = None
    try:
        try:
            if args.command == 'plan':
                identifiers = [_relative_path(raw) for raw in args.input]
                if len(set(identifiers)) != len(identifiers):
                    raise ModelError('duplicate input identifier')
                limits = Limits.from_dict(read_json(args.limits, Limits())) if args.limits else Limits()
                protected = [args.sources / name for name in identifiers]
                if args.limits:
                    protected.append(args.limits)
                plan = preflight([Path(name) for name in identifiers], args.sources, limits)
                write_json_exclusive(plan, args.out, protected)
                committed = args.out
                for diagnostic in plan.diagnostics:
                    _emit('Diagnostic: ' + diagnostic)
                return _status(plan.status, f'review artifact {args.out}; plan digest {plan.plan_digest}')
            if args.command == 'inspect':
                return _inspect(args.plan)
            if args.command == 'origin':
                record = lookup_origin(args.dataset, args.source_id, args.row, args.sources)
                _emit(render_origin(record), end='')
                return STATUS_EXITS[record.integrity_level]
            if args.command in ('apply', 'verify'):
                approved = load_approved_plan(args.approved, Limits())
                protected = [args.approved, args.sources, *[args.sources / source.relative_path for source in approved.plan.sources]]
                if args.command == 'apply':
                    _destination(args.out, protected)
                    report = apply(approved, args.sources, args.out)
                    if report.verified or args.out.is_dir() and any('committed' in d for d in report.diagnostics):
                        committed = args.out
                else:
                    protected.append(args.dataset)
                    if args.report:
                        _destination(args.report, protected)
                    report = verify_bundle(args.dataset, args.sources, approved)
                    if args.report:
                        write_json_exclusive(report, args.report, protected, limits=approved.plan.limits)
                        committed = args.report
                for diagnostic in report.diagnostics:
                    _emit('Diagnostic: ' + diagnostic)
                return _status(report.status, ('fresh complete source-bound check' if report.verified else 'source-bound operation refused'))
            plan = load_plan(args.plan, Limits())
            if plan.status not in REVIEW_STATUSES:
                return _status(plan.status, 'cannot approve without complete successful preflight')
            resolutions = load_resolutions(args.decisions, plan)
            approved = approve(plan, resolutions)
            protected = [args.plan, args.decisions]
            if args.sources:
                protected.extend(args.sources / source.relative_path for source in plan.sources)
            write_json_exclusive(approved, args.out, protected)
            committed = args.out
            return _status(approved.status, f'approval artifact {args.out}; approval digest {approved.approval_digest}')
        except _ReportingFailure:
            raise
        except OriginCheckError as exc:
            return _status(exc.status, str(exc))
        except (ArtifactConflictError, DecisionConflictError) as exc:
            return _status('CONFLICT', str(exc))
        except UnsupportedError as exc:
            return _status('UNSUPPORTED', str(exc))
        except LimitError as exc:
            return _status('LIMIT_EXCEEDED', str(exc))
        except ModelError as exc:
            from .supervise import _diagnostic
            return _status(_model_error_status(exc), _diagnostic(exc))
        except FileNotFoundError as exc:
            return _status('INCOMPLETE', str(exc))
        except OSError as exc:
            from .supervise import _os_error_status, _diagnostic
            return _status(_os_error_status(exc), _diagnostic(exc))
        except MemoryError as exc:
            from .supervise import _diagnostic
            return _status('LIMIT_EXCEEDED', _diagnostic(f'parent allocation refusal: {exc}'))
        except KeyboardInterrupt:
            return _status('CANCELLED', 'operation cancelled')
        except Exception as exc:
            from .supervise import _diagnostic
            return _status('ERROR', _diagnostic(f'{type(exc).__name__}: {exc}'))
    except _ReportingFailure as exc:
        return _report_output_failure(exc, committed)


if __name__ == '__main__':
    sys.exit(main())
