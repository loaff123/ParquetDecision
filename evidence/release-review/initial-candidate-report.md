# ParquetDecision 0.1.0a1 local release candidate

Status: local source/distributions/qualification prepared. Independent final
code/product review remains an owner-run gate. Public publication and remote CI
are not claimed.

Metadata provenance is preserved. Scientific semantic compatibility is not established.

## Concrete result

Six commands now implement plan, inspect, decide, apply, verify and origin.
Origin without sources has primary OUTPUT_HASHES_MATCH integrity and explanatory
OUTPUT_ONLY_CHECKED provenance; it explicitly says source value correctness was
not checked. It checks complete inventory, canonical decision/manifest/report
bindings, part hashes, exact output schema and all output provenance rows in a
fixed fresh worker. Origin with sources completes the whole independent
source-schema/value/provenance verifier before VERIFIED / FRESH_SOURCE_BOUND.
Stored verification metadata is always historical, including saved VERIFIED.
The original nested producer unit is shown intelligibly as cm, with exact raw
byte records also available.

Immutable no-clobber writes protect selected source identities and input artifacts;
apply protects the source root; optional verify reports additionally protect the
whole dataset/source directory and approval file. An optional report cannot be
written inside a completed bundle even under a new filename. Caller-lowered
report JSON/staging caps remain enforced. Real stdout EPIPE and /dev/full failures
preserve correct exits and already-committed artifact context. Graceful SIGINT /
SIGTERM cancellation reaps the owned worker; abrupt death is documented separately.

No strict portable record grammar or existing operation tag changed. The existing
fixed verify worker adds a declarative origin descriptor with exactly mode/parts;
its request uses the existing plan/staging/destination/approved_plan argument
allowlist and only output paths. No caller code/module/function can be chosen.
write_json_exclusive adds an optional Limits argument only for VerificationReport;
Plan/ApprovedPlan cannot use it to relax their own contracts. Independent schema /
value conclusions remain unchanged. Supported origin/verify operations check
elapsed time before returning success, in addition to the worker watchdog.

## Exact fresh gates

- Sanitized public source: 725 passed in 66.66 seconds
- Installed wheel, external source-free harness: 725 passed in 66.21 seconds
- Extracted-sdist installation, external source-free harness: 725 passed in 66.56 seconds
- Same full relevant suite byte hashes match source, both harnesses and extracted
  sdist; pytest has no pythonpath override
- Actual imports were checked per mode and all runtime .py hashes match the
  sanitized public source; exact Python 3.12.14 / PyArrow 25.0.0 and version 0.1.0a1
- Literal installed README-only journeys: all 18 fresh command processes matched
  expected exits for both wheel and extracted-sdist installations
- Original ten-workload production replay: 8 verified complete bundles / 2
  explicit refusals, fresh full checks and output-only origin, all 22 hashes unchanged
- 1 MiB actual native metadata observation: 1,398,649 input bytes, 108,792 KiB
  descriptive peak RSS under 4 GiB AS / 120-second CPU configuration
- Public source text/archive entry scans: no private paths/history findings;
  original fixture bytes and two final comparison JSON bodies independently matched

Full receipts, import/test/hash manifests and observations accompany this report.
Dependencies were pre-provisioned in three fresh virtual environments by explicit
allowlist copy of exact already-qualified trusted distributions. Product source,
wheel and extracted sdist were separately installed through pip. No editable
product, caller startup code or unrelated application packages were copied into
wheel/sdist environments. The mainstream build stack is recorded exactly.

## Counter versus native qualification

128 files / 512 cumulative fields, 1,000,000 rows, depth 8, 1 MiB raw metadata and
8,192-row streams have actual benign native workloads plus immediate over-cap
refusals. Maximum input/plan/output/staging byte boundaries include explicitly
labeled model/JSON/budget/sparse-file enforcement checks and actual lowered small
native/control-file boundaries. Those are not large-Parquet capacity benchmarks.
300-second wall / 120-second CPU maxima were configured; unchanged controlled
lowered watchdog/SIGXCPU tests demonstrate observed enforcement. 4 GiB AS was
configured across native runs; controlled allocation tests are unchanged.

Arrow exposes native file schemas before the logical metadata-byte cap can be
checked. Batch rows cannot bound one huge scalar. AS is virtual address space,
not RSS, parent startup/publication or an OS sandbox. Earlier 64 MiB import
failure and 128 MiB SIGSEGV remain ERROR, with no guessed memory cause or new
low-AS/native-internals investigation. No hostile/decompression corpus was used.
The parent watchdog cannot operate after parent death; incomplete/possibly
committed states and lack of crash recovery/power-loss guarantees are documented.

## Preserved comparison judgment

All 40 frozen original outcomes and 22 original source files remain byte-identical.
Direct Arrow: 7 successes / 3 refusals. DuckDB: 6 successes / 3 value changes /
1 refusal. Prototype: 8 successes / 2 refusals, with an incomplete original verifier.
Explicit-schema Arrow: the same 8 successes / 2 refusals. No new workload family
is counted as conversion-superiority evidence. The prototype is not shipped as
runtime or a correctness certificate. Close OmniMorph and paid ParquetHarmonize
alternatives were documented, not executed as full applications. Early reviewer
judgments diverged; actual demand is unproven. Workflow integration is the bounded
hypothesis, with no speed, scientific-validity, broad safety or market-gap claim.

Historical primary documentation links: [Arrow schema union](https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html),
[DuckDB Parquet tips](https://www.duckdb.org/docs/lts/data/parquet/tips),
[DuckDB timestamp limitation](https://duckdb.org/docs/current/sql/data_types/timestamp),
[OmniMorph repository](https://github.com/crjaensch/OmniMorph),
[ParquetHarmonize provider listing](https://api.market/store/parquetharmonize/parquet-harmonize).
These are provenance for the earlier evaluation, not a fresh market survey.

## Failed attempts, correction and never-run gates

The first sanitized-source/wheel/sdist suite attempts each reported 10 failures /
713 passes: all ten test_rewrite_original_workloads cases used an old unavailable
private fixture lookup. The tests now locate the shipped unchanged original
package fixtures. Final full 725-test runs above include every case and pass.
This is a portability-test correction, not additional conversion evidence.
An initial resource-observation helper used relative worker paths and refused
INCOMPLETE; its corrected absolute-path replay passed and its raw attempt is retained.
Expected TDD reds established missing interfaces, malformed bundle exit classification,
SIGTERM cancellation, late elapsed-time refusal and report cap protections before
implementation. Setup mistakes in earlier cancellation tests are retained and
are not presented as valid red proof.

Never run: public Git/repository publication, tags, package registries, Sites,
remote exact-head CI and other public/remote writes. The local workflow file is
preparation only. Provisional names have only a read-only availability check,
not trademark clearance. Independent final code/product review is still required
from the owner; implementer tests do not replace it.

## Public versus internal delivery

Public-safe: history-free source archive/tree, wheel, sdist, this review packet,
source/distribution/test/import manifests, baseline packet, README transcripts,
resource observations and normalized receipts. MIT license is present in source,
installed docs/distributions and distribution metadata.

Untouched first/final original packet, raw receipts and private planning inputs
remain separately preserved for the controller. They are not in any public-facing
archive. See presentation-normalization.md for the exact disclosure; normalized
logs are not claimed byte-identical to raw originals.
