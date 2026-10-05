"""Native-free parent supervision and a fixed isolated worker bootstrap.

This is resource supervision of trusted local operations, not an OS sandbox.
The Python interpreter and standard-library bootstrap precede resource setup;
third-party native imports occur only afterward in an isolated fresh process.
"""
from __future__ import annotations

import os
import errno
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import sysconfig
import tempfile
import time

from .model import (Limits, LimitError, ModelError, UnsupportedError, WorkerRequest,
                    WorkerResult, canonical_json, parse_json)

MAX_REQUEST_BYTES = 4 * 1024**2
MAX_RESULT_BYTES = 512 * 1024
MAX_STDERR_BYTES = 128 * 1024
_WORKER_READY = False

# This source is fixed application code. Requests cannot choose a callable,
# module, command, sys.path entry or extension registration. -I ignores caller
# PYTHONPATH/cwd; -S suppresses site, .pth and sitecustomize execution; -B
# disables bytecode writes. Only the selected installed runtime dependency root
# and our product root are added.
_BOOTSTRAP = """import sys, resource, os
memory_failure = ('{"protocol_version":1,"operation":"' + sys.argv[5] +
    '","status":"LIMIT_EXCEEDED","counters":[],"diagnostics":[' +
    '"MemoryError: recognized allocation refusal during fixed worker bootstrap"]}').encode()
os_failure = ('{"protocol_version":1,"operation":"' + sys.argv[5] +
    '","status":"LIMIT_EXCEEDED","counters":[],"diagnostics":[' +
    '"OSError: recognized ENOMEM/ENOSPC/EDQUOT resource refusal during fixed worker bootstrap"]}').encode()
try:
    resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[1]), int(sys.argv[1])))
    resource.setrlimit(resource.RLIMIT_CPU, (int(sys.argv[2]), int(sys.argv[2]) + 1))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    sys.path.extend([sys.argv[3], sys.argv[4]])
    from parquet_decision.supervise import _worker_entry
    _worker_entry()
except MemoryError:
    os.write(1, memory_failure)
except OSError as exc:
    if exc.errno in (12, 28, 122):
        os.write(1, os_failure)
    else:
        raise
"""


def _diagnostic(value: object) -> str:
    return str(value).replace('\x00', '?').encode('utf-8', errors='replace')[:4096].decode('utf-8', errors='ignore')


def _result(request: WorkerRequest, status: str, *diagnostics: object) -> WorkerResult:
    return WorkerResult(1, request.operation, status, diagnostics=tuple(_diagnostic(d) for d in diagnostics[:32]))


def _os_error_status(exc: OSError) -> str:
    """Preserve exact observed OS resource causes, without an RLIMIT claim."""
    return 'LIMIT_EXCEEDED' if exc.errno in (errno.ENOMEM, errno.ENOSPC, errno.EDQUOT) else 'ERROR'


def _stop(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _supervise_process(command: list[str], payload: bytes, request: WorkerRequest, limits: Limits,
                       *, cwd: Path, env: dict[str, str], cancel_event=None) -> WorkerResult:
    """Private bounded pipe/watchdog primitive; production uses only _BOOTSTRAP.

    Tests use controlled original process fixtures with this primitive. It is not
    a public arbitrary-command WorkerRequest operation or plugin surface.
    """
    if len(payload) > MAX_REQUEST_BYTES:
        return _result(request, 'ERROR', 'worker request exceeds bounded protocol envelope')
    start = time.monotonic()
    try:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True, close_fds=True, bufsize=0)
    except OSError as exc:
        return _result(request, _os_error_status(exc), f'parent worker start failed: {exc}')
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    caps = {'stdout': MAX_RESULT_BYTES, 'stderr': MAX_STDERR_BYTES}
    sent = 0
    reason = None
    try:
        with selectors.DefaultSelector() as selector:
            for name, stream in (('stdout', process.stdout), ('stderr', process.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            if payload:
                os.set_blocking(process.stdin.fileno(), False)
                selector.register(process.stdin, selectors.EVENT_WRITE, 'stdin')
            else:
                process.stdin.close()
            while selector.get_map():
                if cancel_event is not None and cancel_event.is_set():
                    reason = ('CANCELLED', 'worker cancelled by caller')
                    break
                remaining = limits.wall_seconds - (time.monotonic() - start)
                if remaining <= 0:
                    reason = ('LIMIT_EXCEEDED', f'wall watchdog exceeded {limits.wall_seconds} seconds')
                    break
                for key, _ in selector.select(min(0.05, remaining)):
                    stream, name = key.fileobj, key.data
                    if name == 'stdin':
                        try:
                            sent += os.write(stream.fileno(), payload[sent:sent + 65536])
                        except BrokenPipeError:
                            sent = len(payload)
                        if sent == len(payload):
                            selector.unregister(stream); stream.close()
                        continue
                    chunk = os.read(stream.fileno(), 65536)
                    if not chunk:
                        selector.unregister(stream); stream.close()
                        continue
                    available = caps[name] - len(buffers[name])
                    buffers[name].extend(chunk[:available])
                    if len(chunk) > available:
                        reason = ('ERROR', f'worker {name} exceeded bounded protocol cap {caps[name]} bytes')
                        break
                if reason is not None:
                    break
            if reason is not None:
                _stop(process)
            else:
                # Pipes may close before the process exits. The same operation
                # watchdog/cancellation remains active until terminal exit.
                while process.poll() is None:
                    if cancel_event is not None and cancel_event.is_set():
                        reason = ('CANCELLED', 'worker cancelled by caller'); _stop(process); break
                    if time.monotonic() - start >= limits.wall_seconds:
                        reason = ('LIMIT_EXCEEDED', f'wall watchdog exceeded {limits.wall_seconds} seconds'); _stop(process); break
                    time.sleep(0.01)
    except KeyboardInterrupt:
        reason = ('CANCELLED', 'worker cancelled by keyboard interruption')
        _stop(process)
    except (OSError, ValueError) as exc:
        status = _os_error_status(exc) if isinstance(exc, OSError) else 'ERROR'
        reason = (status, f'parent worker pipe supervision failed: {exc}')
        _stop(process)
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        if process.poll() is None:
            _stop(process)
    stderr = buffers['stderr'].decode('utf-8', errors='replace')
    if reason is not None:
        return _result(request, reason[0], reason[1], *([stderr] if stderr else []))
    if process.returncode == -signal.SIGXCPU:
        return _result(request, 'LIMIT_EXCEEDED', f'SIGXCPU established worker CPU limit {limits.cpu_seconds} seconds')
    if process.returncode != 0:
        if process.returncode < 0:
            number = -process.returncode
            try:
                cause = signal.Signals(number).name
            except ValueError:
                cause = f'signal {number}'
        else:
            cause = f'exit {process.returncode}'
        return _result(request, 'ERROR', f'unexpected worker termination: {cause}', *([stderr] if stderr else []))
    try:
        result = WorkerResult.from_dict(parse_json(bytes(buffers['stdout']), max_bytes=MAX_RESULT_BYTES))
        if result.operation != request.operation or result.protocol_version != request.protocol_version:
            raise ModelError('worker terminal result does not match request')
        return result
    except (ModelError, ValueError, UnicodeError, RecursionError) as exc:
        return _result(request, 'ERROR', f'invalid or truncated worker protocol: {exc}', *([stderr] if stderr else []))


def run_worker(request: WorkerRequest, limits: Limits) -> WorkerResult:
    """Execute only fixed internal operations in a fresh bounded interpreter."""
    if not isinstance(request, WorkerRequest) or not isinstance(limits, Limits):
        raise ModelError('run_worker requires strict WorkerRequest and Limits records')
    try:
        if sys.platform != 'linux' or sys.version_info[:3] != (3, 12, 14):
            return _result(request, 'UNSUPPORTED', 'qualified worker requires Linux and Python 3.12.14')
        # Resolve only the selected interpreter's installation scheme. Parent
        # sys.path/PYTHONPATH and already-imported caller pyarrow objects are ignored.
        dependency_root = Path(sysconfig.get_path('platlib')).resolve()
        if not (dependency_root / 'pyarrow' / '__init__.py').is_file():
            return _result(request, 'UNSUPPORTED', 'pinned PyArrow installation is unavailable in the selected interpreter')
        package_root = Path(__file__).resolve().parent.parent
        payload = canonical_json({'request': request.to_dict(), 'limits': limits.to_dict()})
        command = [sys.executable, '-B', '-I', '-S', '-c', _BOOTSTRAP, str(limits.address_space_bytes),
                   str(limits.cpu_seconds), str(package_root), str(dependency_root),
                   request.operation]
        # Strip native loader hooks, startup code, caller import paths and application
        # environments. Installed runtime/package roots remain trusted, not sandboxed.
        env = {'LANG': 'C.UTF-8', 'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1',
               'MKL_NUM_THREADS': '1', 'NUMEXPR_NUM_THREADS': '1'}
        with tempfile.TemporaryDirectory(prefix='parquet-decision-worker-') as directory:
            return _supervise_process(command, payload, request, limits, cwd=Path(directory), env=env)
    except OSError as exc:
        return _result(request, _os_error_status(exc), f'parent worker setup/cleanup failed: {exc}')


def _require_worker() -> None:
    if not _WORKER_READY:
        raise ModelError('native operation requires a fresh qualified worker')


def _worker_entry() -> None:
    """Fixed bootstrap target. Only installed internal handlers can run."""
    import errno
    import resource
    request = WorkerRequest(1, 'preflight')
    try:
        envelope = parse_json(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1), max_bytes=MAX_REQUEST_BYTES)
        if type(envelope) is not dict or set(envelope) != {'request', 'limits'}:
            raise ModelError('invalid worker envelope')
        request = WorkerRequest.from_dict(envelope['request'])
        limits = Limits.from_dict(envelope['limits'])
        if (resource.getrlimit(resource.RLIMIT_AS) != (limits.address_space_bytes, limits.address_space_bytes)
                or resource.getrlimit(resource.RLIMIT_CPU) != (limits.cpu_seconds, limits.cpu_seconds + 1)):
            raise ModelError('worker limits were not installed before package/native imports')
        if 'pyarrow' in sys.modules:
            raise ModelError('native library imported before qualified worker boundary')
        global _WORKER_READY
        _WORKER_READY = True
        if request.operation == 'preflight':
            from .preflight import _preflight_worker
            result = _preflight_worker(request, limits)
        elif request.operation == 'verify':
            from .verify_values import _verify_worker
            result = _verify_worker(request, limits)
        elif request.operation == 'rewrite':
            from .bundle import _rewrite_worker
            result = _rewrite_worker(request, limits)
        else:
            raise ModelError('unknown fixed operation')
    except UnsupportedError as exc:
        result = _result(request, 'UNSUPPORTED', exc)
    except LimitError as exc:
        result = _result(request, 'LIMIT_EXCEEDED', exc)
    except MemoryError:
        result = _result(request, 'LIMIT_EXCEEDED', 'MemoryError: recognized allocation refusal under RLIMIT_AS')
    except FileNotFoundError as exc:
        result = _result(request, 'INCOMPLETE', exc)
    except Exception as exc:
        pa = sys.modules.get('pyarrow')
        if ((pa is not None and isinstance(exc, pa.ArrowMemoryError))
                or (isinstance(exc, OSError) and exc.errno in (errno.ENOMEM, errno.ENOSPC, errno.EDQUOT))):
            result = _result(request, 'LIMIT_EXCEEDED', f'{type(exc).__name__}: recognized allocation/disk resource refusal')
        else:
            result = _result(request, 'ERROR', f'{type(exc).__name__}: {exc}')
    observations = (('address_space_bytes', resource.getrlimit(resource.RLIMIT_AS)[0]),
                    ('cpu_seconds', resource.getrlimit(resource.RLIMIT_CPU)[0]),
                    ('peak_rss_kib', resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    result = WorkerResult(result.protocol_version, result.operation, result.status,
                          tuple(sorted((*result.counters, *observations))), result.diagnostics)
    sys.stdout.buffer.write(canonical_json(result.to_dict()))
    sys.stdout.buffer.flush()
