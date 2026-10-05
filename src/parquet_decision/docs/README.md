# ParquetDecision 0.1.0a1

Publication context: the original local-candidate narrative below is historical.
The qualification document records the later synchronous-read lifecycle change
and distinguishes the initial remote failure from subsequent qualification.

A small trusted-local workflow: plan → inspect → explicitly decide → apply →
freshly verify → retrieve a row's original producer interpretation.

**Metadata provenance is preserved. Scientific semantic compatibility is not
established.** Archive-only metadata decisions do not convert metres to centimetres,
reconcile calibration, or authorize scientific aggregation.

This original synthetic experiment does not establish conversion superiority:
an explicit-schema Arrow recipe matches all eight successful workloads and
safely refuses the same two. The first direct Arrow experiment had 7 successes /
3 refusals; DuckDB had 6 successes / 3 value changes / 1 refusal; the throwaway
prototype had 8 successes / 2 refusals. Its incomplete verifier is not production
code or a correctness certificate. All 40 frozen outcomes are retained in
[evidence/baselines](evidence/baselines/README.md). OmniMorph and the paid
ParquetHarmonize API are close documented alternatives, not executed applications
in this evaluation. Reviewers initially differed on the product judgment.
Actual user demand is unproven; bounded workflow integration is the hypothesis.

## Install the local alpha

The qualified runtime is **Linux / Python 3.12.14 / PyArrow 25.0.0**. Other Python
3.12 patches and platforms are not qualified. Names are provisional; a read-only
name check is not trademark clearance. This candidate is local only: no registry
publication, remote CI or public-release claim has been made.

In a fresh directory, create a virtual environment with Python 3.12.14 and install
the supplied wheel (replace the wheel path with the actual local candidate path):

```sh
python3.12 -m venv env
. env/bin/activate
python -m pip install /path/to/parquet_decision-0.1.0a1-py3-none-any.whl
python -c 'import platform, pyarrow; assert platform.python_version() == "3.12.14"; assert pyarrow.__version__ == "25.0.0"'
```

Installed examples and documentation are available without a source checkout:
`python -m parquet_decision.examples` generates the original fixture, and
`importlib.resources.files("parquet_decision").joinpath("docs", "README.md")`
locates this README. `python -m parquet_decision` is equivalent to `pdecision`.

## Complete original newcomer journey

Use a fresh working directory with the installed environment active. These files
are original benign synthetic data, not downloaded or arbitrary files.

```sh
python -m parquet_decision.examples sources
pdecision plan --sources sources --input legacy.parquet --input new.parquet --out plan.json
pdecision inspect plan.json
```

The plan command intentionally exits **1 with NEEDS_DECISIONS** and writes a
complete review plan. Do not run the sequence under `set -e` without handling this
expected exit. Inspect shows every action, exact original schemas, limits, two
conflicts, all complete digests, and a copyable plan-bound decisions template.
One conflict is schema `producer`; the other is field `["payload","distance"]`
`unit`, with original `m` versus `cm`. Canonical metadata omits both conflicting
keys, while the original byte records remain archived.

For this demonstration, deliberately choose `archive_only` separately for BOTH
named conflicts. This means preserve producer interpretation without reconciling
units. In your own data, inspect each conflict and use `abort` where appropriate.
There is no blanket yes or default archive option.

The following command writes the explicit two-choice decisions file with digests
from your own plan. It refuses existing files; it is an example-specific operator
choice, not automatic resolution in the product:

```sh
python - <<'PY'
import json
p = json.load(open('plan.json'))
producer, unit = p['proposal']['conflicts']
assert producer['path'] == [] and producer['key'] == 'cHJvZHVjZXI='
assert unit['path'] == ['payload', 'distance'] and unit['key'] == 'dW5pdA=='
choices = {'format_version': 1, 'plan_digest': p['plan_digest'], 'resolutions': [
    {'conflict_digest': producer['conflict_digest'], 'action': 'archive_only'},
    {'conflict_digest': unit['conflict_digest'], 'action': 'archive_only'}
]}
with open('decisions.json', 'x') as f:
    json.dump(choices, f)
PY
pdecision decide plan.json --sources sources --decisions decisions.json --out approved.json
pdecision inspect approved.json
pdecision apply approved.json --sources sources --out result
python -c 'import pyarrow.parquet as pq; t=pq.read_table("result"); print(t); assert t.num_rows == 3'
pdecision origin result --source-id 1 --row 0
pdecision origin result --source-id 1 --row 0 --sources sources
pdecision verify approved.json --sources sources --dataset result --report fresh-verification.json
pdecision inspect result
```

Apply performs complete fresh independent schema/value verification before
exclusive publication. Reading `result` through ordinary Arrow works because
underscore-prefixed JSON files are ignored. Source 1 row 0 reports original
`new.parquet`, unit `cm`, exact original metadata/type, row coordinate and decisions.
The first origin has primary integrity status **OUTPUT_HASHES_MATCH**: output
inventory/hash/cross-binding/schema/provenance checks only, with source value
correctness not checked. It may describe provenance as OUTPUT_ONLY_CHECKED.
The `--sources` origin completes the WHOLE fresh source/output verifier before
reporting **VERIFIED / FRESH_SOURCE_BOUND**. The saved `_verification.json` always
remains **historical**, including when its saved status says VERIFIED.
Inspecting a bundle never reports fresh source verification.

The fresh report must be a new file outside the dataset, source root and all
input artifacts. Every write is no-clobber and source-protected. Choose new paths
for retries; an existing plan, receipt or bundle is never overwritten.

### Expected refusals

A missing or unresolved decision exits 1 with CONFLICT and produces no approved
artifact. After the journey, reproduce an unresolved choice in a new file:

```sh
python - <<'PY'
import json
choices = json.load(open('decisions.json'))
choices['resolutions'] = choices['resolutions'][:1]
with open('unresolved.json', 'x') as f:
    json.dump(choices, f)
PY
pdecision decide plan.json --decisions unresolved.json --out never-approved.json
```

To exercise changed-source refusal safely, make a separate copy and change only
that copy. Apply exits 1 with CONFLICT; `never-complete` must not exist:

```sh
python - <<'PY'
import shutil
shutil.copytree('sources', 'changed-sources')
with open('changed-sources/legacy.parquet', 'ab') as f:
    f.write(b'original benign changed-source demonstration')
PY
pdecision apply approved.json --sources changed-sources --out never-complete
pdecision verify approved.json --sources missing-sources --dataset result
```

The last command exits 3 with INCOMPLETE. Missing sources never produce VERIFIED.
Malformed coordinates/JSON exit 2; unsupported profiles exit 2; limits,
incomplete operations and cancellation exit 3; unexpected errors exit 4.
A successful inspect exits 0 independently of the stored historical Plan status.
Post-publication reporting/cleanup failures can leave an already committed
artifact. Read the diagnostic and inspect it before retrying. Abrupt parent death
or host shutdown is different from supported graceful SIGINT/SIGTERM cancellation.

## Bounds and review

Only trusted, quiescent local files, strict declarative records and file paths
are public inputs. Fresh `-B -I -S` workers load no caller plugins/extension registry
or arbitrary code. This is not a hostile-input sanitizer or OS sandbox. Logical
limits include 128 files, 512 MiB input, 1,000,000 rows, 512 cumulative fields,
8 struct levels, 1 MiB metadata, 8 MiB JSON, 8,192 rows/batch, 1 GiB output,
2 GiB owned staging, 300 seconds wall, 120 seconds worker CPU, 4 GiB worker address
space. Batch rows are not a byte-allocation bound; worker AS is not an RSS guarantee
and does not bound parent startup/publication or all filesystem activity.

`plan --limits limits.json` accepts a strict FULL Limits record and can only
lower caps. Obtain the defaults without a checkout:

```sh
python -c 'import json; from parquet_decision.model import Limits; print(json.dumps(Limits().to_dict(), indent=2))' > limits.json
```

Edit a cap lower and pass `--limits limits.json` to plan. Effective limits remain
bound into approval. See [profile](profile.md), [decisions](decisions.md),
[verification](verification.md), [bundles](bundles.md),
[limitations](limitations.md) and [qualification](qualification.md).
The last distinguishes full native workloads from enforcement/counter checks.
Digests establish integrity, not signatures, approval authenticity or scientific
validity. Linux libc `renameat2(RENAME_NOREPLACE)` is mandatory; no overwriting
fallback or crash-recovery command exists. No speed or safety marketing claim.

## Local development / CI configuration

Install exact runtime/build/test dependencies, then run `python -m pytest`.
The suite must import the intended installed/source distribution; pytest has no
`pythonpath` override. The workflow configuration is prepared locally only.
Actual remote CI against the final published head, registries, tags and publication
have never run and require separate authorization.
