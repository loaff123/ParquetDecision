# Verified exclusive bundles

`bundle.apply(ApprovedPlan, source_root: Path, destination: Path)` returns a
VerificationReport. Only a complete fresh source-bound success returns VERIFIED.
The sources and destination parent must be trusted, quiescent, real directories.
The existing destination parent must be outside the source root. Existing or
competing destinations are refused and preserved. No fallback overwrite exists.
The apply command and origin/verify CLI journey are documented in README.md;
qualification receipts distinguish source, wheel and extracted-sdist gates.

`transform.rewrite(ApprovedPlan, list[Snapshot], staging: Path)` is a native-free
fresh-worker wrapper around the forward transformer. Its staging argument is an
existing empty caller-owned dataset directory. It returns ordered PartRecords,
including an empty part for an empty source. Standalone rewrite is not a complete
verified bundle. On failure it raises and may leave caller-owned partial parts;
there is no cleanup or crash-recovery command for those caller-owned files.

## Layout and exact manifest version 1

Published directories contain exactly part-NNNNN.parquet for every source ID,
plus `_decision.json`, `_manifest.json` and `_verification.json`, all regular
files. The appended provenance fields are nonnullable uint32 `__pc_source` and
uint64 `__pc_row`, with no field metadata. Source rows stay in original order.
Normal Arrow directory discovery ignores the underscore-prefixed JSON. DuckDB
Parquet-glob row reading is a compatibility check; its documented timezone-aware
nanosecond limit is unchanged.

The decision file is the canonical strict ApprovedPlan JSON, unwrapped. The
verification file is the canonical unchanged VerificationReport. It is always a
historical record when read later, even though its saved status is VERIFIED.
Its exact semantic counters remain files, rows, input_bytes, part_bytes, values.
Part bytes exclude metadata. Counts describe distinct inventory bytes, not total
I/O, because source/output hashes are read multiple times.

The manifest is canonical strict JSON with exactly these keys:

- format_version: integer 1
- status: COMPLETE
- plan_digest, approval_digest, source_digest, output_digest
- target_schema_digest
- parts: the complete ordered portable PartRecord list
- metadata: exactly `_decision.json` and `_verification.json`, each with bytes
  and sha256
- accounting: exactly part_bytes, decision_bytes, verification_bytes,
  manifest_bytes, bundle_bytes
- verification_label: HISTORICAL
- manifest_digest

All byte counts are exact file-content lengths. bundle_bytes is the sum of all
parts and all three metadata files, including the manifest's own serialized
size. Its size is resolved to a stable decimal-length fixed point before writing.
manifest_digest is SHA256 of the canonical manifest object with only that digest
key omitted; it is not a SHA256 of its own file bytes. Its accounting includes
its own final length. The worker's private response additionally binds the SHA256
of the exact bytes of all three metadata files, including the complete manifest.

The source digest covers canonical JSON of the ordered complete SourceSpec list;
the output digest covers canonical JSON of the ordered complete PartRecord list.
Local paths are not serialized. target_schema_digest covers canonical logical
fields/schema metadata through model.schema_digest. Reserved provenance fields
are checked separately and excluded from that logical digest; part file hashes
still bind all physical bytes.

`bundle.read_bundle(dataset: Path, *, deadline=None)` returns approved, parts,
stored_report, manifest after complete output-only inventory, byte-hash and
cross-binding checks. Parts have internal local paths. It does not inspect source
values and does not establish fresh VERIFIED status. The report is historical;
origin callers should label this mode OUTPUT_HASHES_MATCH. The independent
source-bound verifier is still required for fresh correctness. These hashes do
not establish authorship, approval authenticity or scientific compatibility.

Bundle and fixed-worker approved-control readers reuse the established raw
approval precheck before recursive SourceSpec/FieldSpec/TypeSpec adapters.
Declared and caller file/row/field/depth/metadata/byte limits and the independent
resolution-count cap remain enforced. Known manifest/standalone result part
counts are checked before PartRecord adaptation; complete strict records,
digests, source bindings and physical-file checks still follow. Reads remain
bounded, canonical and anchored to the same regular-file descriptor.

## Worker and publication boundary

Apply creates a unique .pdecision-apply-* outer owned container in the destination
parent. It contains source snapshots (mode 0400), approved control JSON, the
private response, an INCOMPLETE marker and an inner dataset. The worker handles
copying, hash checks, transformation, independent `_verify_streams`, metadata,
final hashes and counters under one fixed fresh rewrite operation. Only the
inner dataset is passed to the shared Linux RENAME_NOREPLACE publisher, after
parent terminal validation. No verifier exception or inventory relaxation is
used for the outer marker.

The fixed worker accepts exactly approved_plan/staging/dataset/destination args;
apply additionally requires source_root and no path list. Standalone rewrite
uses a snapshot path list in exact source order and no source_root. Requests
cannot select functions, code or modules. Every worker starts with -B -I -S.

The private response has exactly request_digest, mode, result, bundle_bytes,
staging_bytes, metadata_sha256. Its result is the report for apply or portable
ordered parts for standalone rewrite. Its staging_bytes includes its own final
serialized length, every control file, source snapshot and dataset file. Apply
terminal counters are the five report counters plus bundle_bytes, staging_bytes
and the three existing supervisor observations. Standalone rewrite omits values.
The parent reconciles every semantic/byte counter and requested AS/CPU ceiling.
Known failures and invalid/missing/malformed result records never promote an
older successful artifact.

Every writer header/footer and metadata/control/response byte is charged before
writing. Both final inventory and byte sums are checked. Apply reserves 12 extra
simultaneous staging bytes for the shared publication helper's two six-byte
capability probe tokens. Staging_bytes names the persistent owned file sum;
publication's transient peak is that sum plus 12. File-content accounting does
not measure allocated blocks, directory metadata, filesystem journals or total
cumulative I/O.

Parent JSON/control reads have fixed byte caps; integrity hashing uses 1 MiB
chunks and checks elapsed time. Worker resource limits cover worker operations,
not parent/bootstrap/publication CPU or memory. Apply checks its total elapsed
time before/after these boundaries. This is not an uninterruptible-I/O guarantee,
filesystem sandbox, hard RSS bound or independent Parquet decoder.

## Failure states and retries

Before publication, source changes, verifier disagreement, disk/byte limits,
worker cancellation/crash, pipe overflow, malformed protocol or metadata failure
leave no final destination. Only operation-owned staging is removed. A hard
parent kill can leave a hidden outer container with INCOMPLETE; its inner report
is not a published result. There is no broad cleanup/crash-recovery command.
Cleanup failures identify retained owned staging; the marker is recreated when
possible within the byte cap, with a bounded diagnostic if that write also fails.

A successful final rename followed by a parent reporting/cleanup/elapsed-time
failure may leave a committed bundle. The returned failure identifies this
possibility and its destination. Inspect the existing bundle before retry; apply
refuses to overwrite it even when the prior response was lost. Publication is
atomic no-replace visibility, not a promise that data/metadata survive power loss;
there is no complete fsync durability protocol.

Metadata provenance is preserved. Scientific semantic compatibility is not established.
