# Local alpha qualification

Qualified stack: Linux x86_64, Python 3.12.14, PyArrow 25.0.0; pytest 8.4.2,
DuckDB 1.5.4 (development only), build 1.3.0, setuptools 80.9.0, wheel 0.45.1,
packaging 26.3 and pyproject-hooks 1.3.3. Names/version: parquet-decision /
parquet_decision / pdecision, 0.1.0a1. Runtime metadata restricts Python to the
qualified patch; this is not qualification of other platforms or 3.12 patches.

Metadata provenance is preserved. Scientific semantic compatibility is not established.

## Evidence categories for every cap

Full benign native workloads and cap+1 refusals are distinguished from accounting
and counter checks. None is a hostile/decompression benchmark or a worst-case
allocation guarantee. Full-suite receipts include these tests on the sanitized
source, installed wheel and extracted-sdist installation; import receipts bind
which product package actually ran. No pytest pythonpath override is present.

| Limits field | Maximum | Evidence category |
| --- | ---: | --- |
| max_files | 128 | Actual native preflight of 128 four-field files; 129-file refusal |
| max_fields | 512 | Same native inventory with 512 cumulative declarations; 513 refusal |
| max_rows | 1,000,000 | Actual native preflight scans every row; 1,000,001 refusal |
| max_depth | 8 | Actual nested native Parquet schema/data; depth 9 refusal |
| max_metadata_bytes | 1,048,576 | Actual native field metadata, raw key+value exactly cap; cap+1 refusal |
| max_input_bytes | 536,870,912 | Exact maximum model inventory counter and cap+1; actual small native input at exact lowered cap / one-byte-below refusal |
| max_plan_bytes | 8,388,608 | Exact JSON envelope cap / cap+1 byte enforcement; strict model and real lowered control-write boundaries |
| batch_rows | 8,192 | Actual million-row native stream under max batches; model rejects 8,193; independent irregular batch zipper tests |
| max_output_bytes | 1,073,741,824 | Exact isolated byte-budget counter cap / cap+1; actual complete small native bundle at lowered byte boundaries; includes metadata |
| max_staging_bytes | 2,147,483,648 | Exact isolated byte-budget cap / cap+1 and ordinary sparse file-content accounting; actual lowered owned snapshots/controls/native-write boundaries |
| wall_seconds | 300 | Actual worker configuration plus observed lowered 1-second watchdog and reaping; no 300-second workload saturation claim |
| cpu_seconds | 120 | Actual worker RLIMIT configuration throughout native runs; unchanged controlled 1-second SIGXCPU observation; no 120-second saturation claim |
| address_space_bytes | 4,294,967,296 | Actual RLIMIT_AS configuration during all original/full native workloads; unchanged controlled ordinary MemoryError observation; not RSS |

Native metadata allocation is observed rather than pre-decoder bounded: Arrow must
expose file schemas before the raw logical metadata cap is enforced. Ordinary
1 MiB metadata parsing's descriptive peak RSS and counters are included in the
resource observations (1,398,649 input bytes; 108,792 KiB descriptive peak RSS
on this run under 4 GiB AS / 120 seconds CPU). This does not establish a maximum
physical allocation.
Batch row counts do not bound a giant scalar value; no giant decompression corpus
or lower-AS native investigation was performed for this stage.

Earlier ordinary lower-AS observations are retained honestly: 256/512 MiB
completed; 64 MiB import mapping failure was ERROR; 128 MiB SIGSEGV was ERROR.
No signal was guessed to be memory exhaustion, and that investigation remains
closed. Parent/bootstrap/publication are outside worker AS/CPU resource limits;
the parent wall watchdog cannot operate after parent death. Graceful real
SIGINT/SIGTERM cancellation and existing KeyboardInterrupt/boundary cancellation
are tested, including worker reaping. Abrupt kill/host shutdown can leave owned
INCOMPLETE staging or a possibly committed bundle after rename. No OS sandbox,
parent RSS guarantee, power-loss durability or recovery command is claimed.

## Correctness and newcomer gates

The same full relevant suite exercises strict record grammar, source identity,
complete feasibility, explicit decisions/no-clobber writes, separately derived
actual-source schema/value comparison, every null insertion and struct validity,
exact provenance, terminal failures, byte accounting and exclusive bundles.
Only fresh complete actual-source checks produce VERIFIED. No-source origin has
primary OUTPUT_HASHES_MATCH status; the stored verification report is historical.

The README commands are executed in fresh processes from a working directory
outside the distribution, including two deliberately explicit archive choices,
ordinary Arrow directory reading, producer-specific cm interpretation, full fresh
verify, unresolved decision refusal, missing-source refusal and changed-source
refusal without completed destinations. The installed example uses the original
combined synthetic fixture; it is not another family counted as superiority.

The 22 original source files and all 40 original workload/method outcomes are
retained byte-for-byte. Baseline parity remains 8 successes / 2 refusals. Direct
Arrow's original 7/3 and DuckDB's 6/3-value-change/1 are preserved, along with the
prototype's 8/2 (its old incomplete verifier is not a correctness certificate).
OmniMorph / paid ParquetHarmonize are close documented alternatives, not executed
apps; reviewers initially diverged; actual user demand remains untested.

## Distribution and presentation

The public candidate is built from a separate sanitized source tree with no
private Git history, plans/briefs, agent instructions or development paths. It
contains runtime, source/installed docs, example and MIT license. Source, wheel
and sdist SHA256s, file manifests, build/install/import and full-suite receipts,
README transcript and resource observations are supplied in a separate public-safe
review packet. Presentation normalization removes private paths/component names
and stale development-stage comments; public receipts disclose it rather than
claiming byte identity with raw internal logs. Untouched original records/raw
receipts remain separately preserved for the local controller.

## Gates never run

Public Git/repository publication, registry upload, tags, remote exact-head CI,
Sites and other public/remote writes have **never run** for this candidate. The
local CI configuration is preparation only. Independent final code/product review
is a separate owner-run gate and is not replaced by implementer suite results.
