"""Regression for Python-file background I/O in the fixed native worker.

The controlled bootstrap instrumentation belongs only to this test. The public
worker request grammar, limits, status handling and completion checks stay fixed.
"""
import ast
import inspect
import json
from pathlib import Path

import pytest

from tests.verify_fixtures import simple_case, verifier


@pytest.mark.parametrize("module_name", ["source", "preflight", "verify_schema", "verify_values", "origin"])
def test_every_native_reader_explicitly_disables_prefetch(module_name):
    # Omitting this setting enables background I/O in the pinned Arrow version,
    # even though iter_batches(use_threads=False) disables column parallelism.
    from importlib import import_module
    module = import_module("parquet_decision." + module_name)
    tree = ast.parse(inspect.getsource(module))
    readers = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
               and isinstance(node.func, ast.Attribute) and node.func.attr == "ParquetFile"]
    assert len(readers) == 1
    options = {option.arg: option.value for option in readers[0].keywords}
    assert isinstance(options.get("pre_buffer"), ast.Constant)
    assert options["pre_buffer"].value is False


def test_original_verification_reads_python_files_on_worker_main_thread(tmp_path, monkeypatch):
    from parquet_decision import supervise
    approved, snapshots, parts, _ = simple_case(tmp_path, count=23, output_groups=2)
    trace = tmp_path / "read-callbacks.json"
    instrumentation = """    import threading
    _read_events = []
    _main_thread = threading.get_native_id()
    _original_fdopen = os.fdopen
    class _ObservedStream:
        def __init__(self, stream):
            self.stream = stream
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def read(self, *args):
            result = self.stream.read(*args)
            _read_events.append((threading.get_native_id(), len(result)))
            return result
        def readinto(self, *args):
            result = self.stream.readinto(*args)
            _read_events.append((threading.get_native_id(), result))
            return result
    def _observed_fdopen(*args, **kwargs):
        return _ObservedStream(_original_fdopen(*args, **kwargs))
    os.fdopen = _observed_fdopen
"""
    marker = "    _worker_entry()\n"
    assert supervise._BOOTSTRAP.count(marker) == 1
    observed = supervise._BOOTSTRAP.replace(marker, instrumentation + marker)
    observed += ("\nimport json\nwith open(" + repr(str(trace)) + ", 'x') as _trace:\n"
                 "    json.dump({'main': _main_thread, 'reads': _read_events}, _trace)\n")
    monkeypatch.setattr(supervise, "_BOOTSTRAP", observed)
    report = verifier().verify_streams(approved, snapshots, parts,
        source_batch_rows=1, output_batch_rows=2)
    assert report.verified, report
    assert dict(report.counters)["rows"] == 23
    assert dict(report.counters)["values"] == 23
    callbacks = json.loads(trace.read_text())
    assert callbacks["reads"], "the probe must observe real Python-file reads"
    assert all(thread == callbacks["main"] for thread, _ in callbacks["reads"]), callbacks
