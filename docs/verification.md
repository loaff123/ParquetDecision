# Independent source-bound verification

Metadata provenance is preserved. Scientific semantic compatibility is not
established. This alpha checks trusted, quiescent local files on the qualified
Linux / Python 3.12.14 / PyArrow 25.0.0 runtime. It is not a hostile-file sanitizer,
a signature scheme, or an independent Parquet decoder.

## What is checked

`verify_streams(approved, snapshots, parts)` reopens the actual source snapshots,
streams SHA256 on the same descriptors used for native reads, and compares actual
source fields, original field order, types, nullability, raw metadata and row
counts to the approved inventory. A recorded schema cannot hide a source field.

`verify_schema.py` independently derives the canonical union from those actual
source schemas. It implements whole-domain integer coverage, exact decimal
precision/scale rules, float-width promotion, exact timestamp timezone labels and
units, recursive struct union, nullable absence and metadata conflicts. It does
not call the planner, approval policy, preflight alignment or transformation.
The complete independently derived fields, metadata, operations and conflicts
must equal the approved proposal. A newly digest-bound, internally consistent
plan that drops a field, changes a type/nullability, hides a conflict or omits an
operation is still refused.

The complete ordered output inventory has one `part-NNNNN.parquet` per source,
including empty sources. Parts must be real files in one dataset directory with
those exact local names. The only additional allowed entries are the three
specified bundle files: `_decision.json`, `_manifest.json`, `_verification.json`.
Their content/cross-binding verification belongs to bundle integration; they are
not an oracle for this schema/value check. Extra files, directories and symlinks
are refused. Actual total dataset bytes, including existing bundle metadata,
are bounded by the approved output-byte cap.

Every part's byte count/hash, source ID, row count and logical-target schema
digest are checked. Actual output schema order, types, nullability and metadata
must exactly match, recursively. The appended provenance fields are exactly
nonnullable `__pc_source: uint32` followed by `__pc_row: uint64`, without field
metadata. Every row has the expected source ID and original zero-based row index.

Every canonical target field is visited at every applicable row:

- An absent or null-typed source field must produce null, with the independently
  derived target type
- A missing nested child must be null whenever its parent is valid
- Parent validity is checked before children: a null struct differs from a valid
  all-null struct; hidden child values beneath null parents are unobserved
- Integers are compared exactly; integer-to-decimal and decimal rescaling use
  integer coefficients/exponents, without global decimal-context arithmetic
- Timestamps use signed integer ticks and bounded powers of ten, with an explicit
  int64 overflow check; no Python datetime conversion occurs for data values
- Float32-to-float64 compares the original representable value, all NaNs form one
  declared logical class, infinities compare normally, and zero signs must match
- Boolean, UTF-8 string and binary values/null validity compare exactly, without
  normalization or inferred semantic conversion

This includes the original benign false-PASS case: source A `{x: 1}`, source B
`{x: 2, y: 3}`, and an erroneous A output `y: 999` fails at source 0, row 0, path
`["y"]`. Output shape or a matching edited plan never supplies its own oracle.

## Streaming and resource boundaries

Only current source/output record batches are retained by the chunk zipper.
The two batch boundaries are independent. No verifier path calls whole-file
`read_table`, `combine_chunks`, schema unification or forward alignment/casting.
Optional `source_batch_rows` and `output_batch_rows` keyword arguments can lower,
but never exceed, the approved batch-row cap. Empty streams are reconciled too.

Public `derive_expected_schema(snapshots)` and `verify_streams(...)` are native-free
wrappers. They use the existing fixed `verify` operation in a fresh isolated,
resource-supervised worker. No request can choose a callable, module or command.
Private `_derive_expected_schema(snapshots, limits)` and `_verify_streams(...)`
require the qualified-worker guard and are available to the single supervised
apply operation; apply must not launch an unbounded nested verifier process.

Private control files are exclusive, byte-bounded canonical JSON. An approved
plan is stored once, separately from the small part/read-size descriptor; source
schemas are not duplicated in transport. Existing WorkerRequest limits remain
4 MiB for the request envelope, 256 path strings and the fixed argument allowlist.
Each private record/artifact file is bounded by `max_plan_bytes`. All files owned
by the verifier's private transport directory count against `max_staging_bytes`.
Borrowed snapshots and parts are read-only; the outer apply operation must also
account for its own staging, snapshots and bundle writes. Resource ceilings are
installed before native imports. A batch-row cap is not a byte-allocation cap,
and RLIMIT_AS is an address-space ceiling, not RSS or an OS sandbox guarantee.

Terminal worker failure always overrides any leftover successful artifact.
Request digest, strict artifact shape, terminal status, counters and all report
bindings are checked by the public wrapper after worker completion. The report
has exactly the five semantic counters listed below. The terminal has exactly
those counters plus address_space_bytes, cpu_seconds and peak_rss_kib; its first
two observations must match the requested limits. Semantic counters agree with
the report, and input-bound file/row/byte counts reconcile with the inventory. Late
setup/read/cleanup ENOMEM/ENOSPC/EDQUOT or recognized allocation failures remain
LIMIT_EXCEEDED; unexplained native/process/protocol failures remain ERROR.

## Results and labels

Only complete fresh source-bound success returns `VERIFIED` (`verified=True`).
There is no output-only mode; unavailable sources return `INCOMPLETE`. Ordinary
schema/value/inventory disagreements return `CONFLICT`. Unsupported type/runtime
profiles return `UNSUPPORTED`; measured/recognized resource failures return
`LIMIT_EXCEEDED`; cancellation remains `CANCELLED`; unexpected native/process or
invalid worker-result failures return `ERROR`. Ordinary value diagnostics identify
`source=<id> row=<zero-based index> path=<JSON segment array>`. Schema and inventory
errors have schema/inventory coordinates rather than a fictitious data row.
Diagnostics stay within the strict bounded protocol.

The report binds:

- `plan_digest` and `approval_digest`: the exact strict approved records
- `source_digest`: SHA256 of canonical JSON for the ordered complete SourceSpec
  list, including hashes, relative identities, original schemas and metadata
- `output_digest`: SHA256 of canonical JSON for the ordered complete PartRecord
  list, including part hashes, source IDs, rows, bytes and target-schema digests
- `checked_at_utc`: the fresh completed check time

`target_schema_digest` is the shared strict model digest of canonical logical
fields and schema metadata. Reserved provenance fields are checked independently
and are not encoded as ordinary FieldSpec records. Local filesystem paths never
enter these portable digest records.

Report counters are `files`, `rows`, `input_bytes`, `part_bytes`, `values`.
`input_bytes`/`part_bytes` count distinct inventory bytes, not cumulative I/O;
source bytes are streamed more than once. `values` counts checked logical target
observations, including parent validity and inserted nulls; children beneath a
null parent do not count as observations. The parent requires values to lie
between rows times the number of top-level target fields and rows times the
number of all target fields counted recursively. These bounds require zero for
empty rows or an empty target, and are exact for scalar-only schemas. The parent
does not recompute the exact data-dependent count for nested null validity.
Provenance is always checked separately.
`part_bytes` does not pretend to include the report's own future serialized size
or all bundle metadata. The outer bundle publisher must check its final complete
byte budget before publication.

Saving a successful report does not make later display a fresh verification.
Stored `_verification.json` is historical. Output-hash checks without sources
must say `OUTPUT_HASHES_MATCH` and must not assert fresh value correctness.
Origin/bundle APIs enforce those separate display and integrity labels in their
integration stage. Hashes are integrity bindings, not operator authenticity or
scientific compatibility claims.

## Qualification scope

Original synthetic tests cover independently specified schemas, forged plans,
all target fields and null insertions, parent validity, exact scalar boundaries,
metadata/order/type/nullability mutations, row/provenance/inventory errors,
irregular 1/3/7 versus 2/5/8 read sizes, empty streams and transport failures.
A 100,000-row original fixture completed under the default worker limits with
8,192-source / 257-output batch sizes and unchanged input hashes. This is a
local moderate-size streaming observation, not a worst-case allocation guarantee,
benchmark superiority, a hostile-parser claim or end-to-end release qualification.
Transformation, bundle publication, CLI/origin integration and sanitized source /
installed wheel / extracted-sdist qualification have separate receipts in
qualification.md. Public release and remote CI remain unrun.
