# Alpha logical and resource profile

ParquetDecision is a trusted-local structural-convergence workflow. Metadata
provenance is preserved. Scientific semantic compatibility is not established.
It is not a hostile-input sanitizer, general converter, or scientific unit
reconciliation engine. The original Arrow baseline reproduced the successful
logical values; converter superiority and real-user demand remain unproven.

## Runtime and supported types

The qualification target is Linux, Python 3.12.14, PyArrow 25.0.0. Distribution:
`parquet-decision`; import: `parquet_decision`; command: `pdecision`; version:
`0.1.0a1`. DuckDB 1.5.4 is a development-only comparison dependency. These names
remain provisional until a separately authorized publication check.

Supported logical types:

- null, bool, signed/unsigned 8/16/32/64-bit integers, float32/float64
- UTF-8 string and variable binary
- decimal128, precision 1–38; decimal256, precision 1–76; scale 0–precision
- timestamps in s/ms/us/ns, with exactly equal timezone labels
- recursively nested structs, including empty structs at the schema-proposal
  layer (native Parquet write feasibility is a separate preflight requirement)

Lists, maps, dictionaries, extensions, unions, dates, durations, fixed-size
binary, large strings/binary, float16, and negative-scale decimals are refused.
No module registers/unregisters extension types. Private worker adapters refuse
direct extension types and ordinary field metadata bearing
`ARROW:extension:name` or `ARROW:extension:metadata`, inspecting field metadata
before field types where the supported API permits. This narrow refusal does not
promise to prevent hooks on arbitrary caller-created registered Arrow extension
objects: PyArrow can invoke a deserializer merely while exposing nested children.
Those caller-object environments are explicitly outside the alpha profile.

Public alpha inputs are strict declarative JSON and trusted local file paths,
processed only through fresh qualified workers without caller application
extension registrations, plugins, or code. The Arrow-object input helpers
`_type_from_arrow`, `_field_from_arrow`, `_fields_from_arrow` are private worker
utilities, not public Python object-input APIs. Source preflight now uses fresh isolated workers and hash-bound snapshots.
Native qualification uses only original well-formed files on Linux, Python
3.12.14 and PyArrow 25.0.0, including benign extension-written files. Native
interpreter/library initialization and process bootstrap remain trusted runtime
boundaries; this is resource supervision, not a sandbox guarantee. Independent
actual-source verification, transformation and bundle integration are implemented
and locally tested on the original fixtures. The complete CLI/origin journey and sanitized source/wheel/extracted-sdist
qualification are described in qualification.md. Public publication is a separate gate.

Integer unions use the smallest full-domain-covering integer. Signed/unsigned
combinations beyond int64 use decimal128(20,0). Decimal/integer unions retain
enough integer digits plus the largest scale, refusing precision above 76.
Decimal256 stays decimal256 even when a narrower decimal could hold its values.
Float32 can widen to float64; integer/float and decimal/float mixtures are refused.
Timestamps propose the finest existing unit only with identical timezone labels;
a proposal does not prove int64 multiplication feasibility.

Struct children union by exact name. An absent field is nullable; a null-typed
field is nullable even if its original declared nullable flag was false. Missing
or null parents generate a parent absence operation, rather than invented child
metadata or child values. An existing null struct must stay null during execution.
No renames, Unicode normalization, default values, filtering, or deduplication
are inferred.

## Limits

All limits are positive integers; booleans and floats are rejected. Defaults are
also the alpha maximums. Callers can lower them but cannot raise them. These are
configured contracts with the original local worker/integration observations
described below, not universal measured allocation guarantees.

| Limits field | Alpha maximum |
| --- | ---: |
| max_files | 128 |
| max_input_bytes | 536,870,912 (512 MiB) |
| max_rows | 1,000,000 |
| max_fields | 512 |
| max_depth | 8 struct levels |
| max_metadata_bytes | 1,048,576 (1 MiB) |
| max_plan_bytes | 8,388,608 (8 MiB) |
| batch_rows | 8,192 |
| max_output_bytes | 1,073,741,824 (1 GiB) |
| max_staging_bytes | 2,147,483,648 (2 GiB) |
| wall_seconds | 300 |
| cpu_seconds | 120 |
| address_space_bytes | 4,294,967,296 (4 GiB) |

`max_fields` counts every original top-level/nested field across the entire
selected source inventory, including repeated declarations in different sources.
`max_metadata_bytes` sums raw key/value bytes across every original source's
schema and field metadata. Target fields and target metadata are separately
checked against those same caps; repeated JSON representations in operation and
conflict records remain bounded by `max_plan_bytes`. Struct depth counts structs,
not the final primitive child. Names are exact valid UTF-8 strings without NUL;
`__pc_` names at every depth are reserved. Empty field names are allowed and remain
unambiguous path segments.

The model bounds JSON bytes before decoding and raw source/target schema
counts/depth/metadata and fixed preflight counters before recursive record
construction. Effective caller-lowered caps apply to failure/partial counters
even if no accepted source inventory is present. Actual in-memory Arrow types,
fields and schemas are iteratively checked for depth/cumulative field/metadata
bounds before recursive model conversion. Workers enforce resource limits before native imports and check actual counters
during file/batch operations. Batch row counts are not byte-sized allocation limits.
RLIMIT_AS is an address-space ceiling, not an RSS guarantee or OS sandbox.

## Strict portable record encoding

Version 1 uses canonical UTF-8 JSON with sorted object keys, compact separators,
no floats/NaN/Infinity, no duplicate/unknown keys, and no Unicode normalization.
JSON arrays retain their logical order. Source order and original field order
are retained; canonical target fields are exact-codepoint sorted at every level.
Metadata is a sequence of exact byte pairs, raw-key sorted, encoded using padded
canonical RFC 4648 base64. Duplicate metadata keys are invalid.
Empty keys/values are exact supported bytes. Raw metadata byte caps do not serve
as record-count caps: conflicts and resolutions have an independent ceiling of
1,048,576 records each, while the plan JSON byte envelope normally binds earlier.

Paths into a schema are arrays of literal field-name segments. A literal `a.b`
field and nested `a` → `b` are distinct, as are raw metadata keys containing dots.
A missing whole field contributes an absence operation, not a fabricated absent
metadata variant. Among corresponding present fields, a differing value or
presence/absence is an explicit conflict. Only byte-identical common entries
survive in the target schema. Conflicts are path/raw-key sorted; their values
are source-id sorted. Operations are source-id/path sorted.

Source identity is a normalized relative POSIX filename plus SHA256, byte and row
counts and original schema. Source IDs are contiguous in the user's explicit
order starting at zero. Absolute paths, parent traversal, backslashes, repeated
slashes and `.` segments are rejected. Source filesystem adapters must additionally
check actual roots, symlinks and file identity. Snapshot/part local `Path` values
are internal and omitted from JSON; native batches cannot be reconstructed from
JSON at all.

Plan, approval and conflict SHA256 digests hash canonical JSON after omitting
only that record's own self-digest key. Nested digests remain included. Digests
are integrity bindings, not signatures or proof of operator authenticity.

## Approval and report labels

The fixed `PreflightCounters` fields are `files`, `rows`, `input_bytes`.
READY_FOR_REVIEW and NEEDS_DECISIONS require completed successful preflight,
source inventory, schema proposal and exactly reconciled counters.
NEEDS_DECISIONS means only explicit metadata decisions remain. Failure/incomplete
statuses cannot carry a successful preflight flag. Approval requires sorted exact
resolution coverage and every action `archive_only`; any `abort` refuses approval.
The model enforces structural admissibility, while actual-source value feasibility
and independent verification remain separate stages.

A fresh complete verification result uses VERIFIED with all four digest bindings,
UTC check time and counters. Displaying stored verification metadata is historical
and cannot assert fresh verification. Origin records without source access use
OUTPUT_HASHES_MATCH; only fresh source-bound verification may use VERIFIED and
FRESH_SOURCE_BOUND. Hash matching alone does not check source values or scientific
meaning.

Error classes are native-free: `ModelError` for malformed records/input,
`UnsupportedError(ModelError)` for an actual excluded logical type or supported
but incompatible type mixture, and `LimitError(ModelError)` for explicit resource
bounds. Downstream code must catch the subclasses first; no diagnostic-string
matching is needed. Invalid tagged JSON is malformed input, even if it resembles
an excluded type.

## Internal worker protocol

Protocol version 1 has only `preflight`, `rewrite`, `verify` operation tags. No
callable, module or command is accepted. Request paths are bounded at 256 entries;
args at 32 entries, with exact allowed keys `source_root`, `staging`, `destination`,
`plan`, `approved_plan`, `dataset`, `batch_rows`. Path/string args are bounded at
8,192 UTF-8 bytes. Terminal worker statuses are COMPLETE, UNSUPPORTED,
DATA_INCOMPATIBLE, CONFLICT, LIMIT_EXCEEDED, INCOMPLETE, ERROR, CANCELLED.
Counter names are unique, counts nonnegative integers, and records use at most
32 counters. Diagnostics use at most 32 strings of at most 4,096 UTF-8 bytes each.
The model/schema modules do not implement native parsing or execution.
`supervise.py` implements bounded fresh-worker execution; `source.py` implements
snapshots and worker-local batch reads; `preflight.py` implements complete
feasibility. The package provides native-free review/approval and qualified Linux public JSON
publication. Independent verification uses a fixed fresh worker; apply integrates transformation
and bundle publication beneath one supervised operation.
See decisions.md for the strict per-conflict file grammar and status/exit mapping.


## Source/preflight worker qualification

The public `preflight` wrapper imports no PyArrow. Its fresh worker covers copying,
streamed hashes, schema reads, every source row, safe cast feasibility and temporary
Parquet writes under one operation wall watchdog. `snapshot_sources` and
`snapshot_inventory` are native-free low-level filesystem adapters, not separately
wall-supervised operations; callers must use them inside the appropriate worker.
`iter_source_batches` requires the worker boundary before native import, checks
snapshot hash/size on the same descriptor, checks actual schema/row identity and
reconciles the final batch. The private guard is an API boundary, not security
isolation against arbitrary caller Python code.

Inputs must be relative `Path` identifiers beneath a real, non-symlink root.
Python's `Path` normalizes some spellings before this API receives them; strict
portable JSON identity retains the model's exact spelling validation. The fixed
preflight worker validates retained raw source strings before constructing Paths;
generic internal worker paths keep their existing grammar. Hard-link
aliases in the selected inventory are refused. Snapshot copying checks source
identity/size/timestamps before and after the copy and checks the current selected
path again. Snapshots have private filenames and read-only file mode. Source files
are never written. Owned staging must be outside the source root and existing
staging bytes count toward the cap; only a newly owned snapshot subdirectory is
removed on copy failure. Trusted quiescent files remain required.

Workers run with Python `-B -I -S`, in a fresh temporary working directory, without
caller `PYTHONPATH`, startup code, site/.pth initialization or native-loader
variables. Product and dependency roots come from the product location and the
selected interpreter's installation scheme, never caller import-path lookup.
Linux RLIMIT_AS and RLIMIT_CPU are installed before product/native imports; CPU
soft/hard limits are `cpu_seconds` and `cpu_seconds + 1`. Core dumps are disabled.
Interpreter and stdlib bootstrap precede those calls; installed runtime/package
code is trusted. No application extension registry, plugin or caller code is
loaded. Ordinary extension field metadata/types are refused before value access
where the supported Arrow API permits. Direct caller-created registered Arrow
objects remain outside the supported public profile.

The internal request envelope is capped at 4 MiB, stdout result at 512 KiB, and
stderr at 128 KiB. Selector-based pipe consumption uses no reading threads or
unbounded `communicate` buffers. Watchdog/cancellation/protocol overflow kills the
owned process group and waits for the child. Terminal JSON is validated against
the request's operation/version. A failed terminal process cannot promote a
leftover successful plan artifact. Fixed operations remain `preflight`, `rewrite`,
`verify`; rewrite now has the fixed declarative handler documented in bundles.md.
Verify uses the fixed strict descriptor protocol documented in verification.md.

Known counter violations, wall watchdog, SIGXCPU, MemoryError/ArrowMemoryError and
OSError ENOMEM/ENOSPC/EDQUOT produce LIMIT_EXCEEDED. Cancellation produces CANCELLED.
Other native/import exceptions, unexplained signals (including SIGKILL/SIGSEGV),
nonzero exits and invalid/truncated protocol produce ERROR. Missing files/roots
produce INCOMPLETE. Unnamed real-time signals retain their numeric value in a
bounded ERROR diagnostic. No signal is guessed to be memory exhaustion. Exact
parent OS resource failures during spawn, pipe handling, staging setup/cleanup or
artifact reads also yield bounded LIMIT_EXCEEDED; this does not claim that a
worker RLIMIT_AS was hit. A hard terminal resource failure overrides any leftover
success artifact.

On the pinned runtime, all 22 frozen original files were processed under the
default 4 GiB AS/120-second CPU limits. Every successful case scanned all rows;
expected unsupported/incompatible cases stopped with explicit refusals. The five-file 100,000-row case completed with
88,600 KiB descriptive peak RSS. Original hashes remained unchanged. Additional
original tests completed 128 files/512 cumulative fields, 1,000,000 rows, depth 8,
and 1 MiB raw metadata, and refused the immediately-over-bound cases. Lowered
byte/output/staging limits are counted before writes. This does not prove a
worst-case allocation bound for a large individual logical value.

Exploratory ordinary-file lower-AS observations: 256 and 512 MiB completed; 64 MiB
returned an untyped native mapping ImportError (ERROR); 128 MiB terminated SIGSEGV
(ERROR). These are runtime observations, not a native-internals investigation or
an advertised reliable minimum. Very small limits can prevent bootstrap itself;
typed MemoryError/known OSError failures have bounded fixed fallback results when
possible. RLIMIT_AS is a virtual-address ceiling, not an RSS guarantee. Default
maximums were not raised or safeguards removed. Original-fixture end-to-end
rewrite/bundle verification now passes; sanitized source/wheel/sdist and the complete
CLI/origin journey have separate receipts in qualification.md; public release remains unrun.


## Review/publication boundary

The CLI implements plan/inspect/decide/apply/verify/origin. Plan can consume a strict
full Limits JSON file to lower caps. Explicit decisions bind both the exact plan
digest and every conflict digest; only complete successful review plans on the
qualified recorded runtime can approve. Archive-only retains original metadata
bytes and leaves conflicting canonical entries absent. No scientific semantic
reconciliation or output verification is claimed.

Public JSON files and the shared directory-publication primitive use libc Linux
renameat2(RENAME_NOREPLACE), with actual destination-filesystem file/directory
qualification and no unsafe fallback. Existing destinations and protected input
identities are preserved, including an ordinary competing destination creator.
Destination parents must be trusted quiescent real directories. This is neither
hostile-filesystem race isolation nor a power-loss durability guarantee. Portable
plans omit absolute roots: optional decide --sources protects even missing source
identities; without it existing sources/aliases are still protected by no-clobber,
but unspecified roots are not checked.


CLI reporting writes and flushes within its controlled command boundary. A failed
stdout uses stderr when available and preserves the exit class (EPIPE ERROR/4;
recognized resource or allocation refusal LIMIT_EXCEEDED/3). Post-publication
diagnostics identify the committed artifact and retries remain no-clobber. Strict
JSON-read ModelError wrappers retain typed operational causes at the CLI boundary,
without changing model grammar or guessing from diagnostic text. Destination
parents are checked using their supplied traversal before canonical output-path
construction, so a static symlink/.. component cannot disappear from validation.


## Verified bundle integration

The apply worker snapshots source bytes, writes one part per source (including
empty sources), hashes outputs, calls the independent actual-source verifier and
finishes all metadata beneath one watchdog. Writers use Parquet 2.6, compression
NONE and no dictionaries. The parent publishes only the inner dataset after a
strict successful terminal record, complete cross-bindings, streamed integrity
hashes and complete byte-budget checks. Source/control files and the INCOMPLETE
marker remain outside that dataset. See bundles.md for exact metadata and failure
states. The shared no-replace qualifier adds 12 transient file-content bytes to
the simultaneous owned-staging budget. Directory entries, allocated blocks and
filesystem journals are not logical file-content bytes.

Worker AS/CPU limits do not cover parent/bootstrap/publication memory or CPU.
Parent JSON reads are bounded and hashes stream in 1 MiB chunks; apply checks
total elapsed time around worker completion, parent hashes and publication.
It cannot interrupt an uninterruptible filesystem call. Bytecode cache writes
are disabled in every fixed worker, including preflight and verification. These
controls are not a filesystem sandbox for the parent/interpreter or trusted
native libraries, a hard RSS bound, or a power-loss durability promise.
