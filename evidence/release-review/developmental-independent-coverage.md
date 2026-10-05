# Earlier independent oracle and publication coverage

These are preserved **earlier developmental independent gates**, not fresh
final-candidate test counts. They strengthen the audit trail for the bounded
workflow, but do not prove correctness over all inputs, establish an independent
Parquet decoder, demonstrate hostile-file safety, or show conversion superiority.
Raw original records are separately preserved. Public filenames and private /
ephemeral paths are presentation-normalized with raw/normalized SHA256s; statuses,
counters, substantive findings and original data hashes are unchanged.

## Independently specified schema and streaming oracle

The earlier reviewer recorded:

- 584 integer-union checks
- 2,401 decimal-schema pairs
- 144 timestamp-schema pairs
- 5,462 chunk-zipper partition pairs
- 16 nested source/output batch-pair combinations
- 47 observed nested-cell mutations, covering the fixture's complete observed-cell
  counter, plus four scalar-boundary mutations and two in-place quiescence negatives
- A fully re-bound nested-source forgery refused with CONFLICT; original data
  completed verification after the negative cases

These counts describe selected original benign checks and independently authored
oracles. They are not additional experiment families or quantities to add to the
final 739-test suite totals.

## Actual writer mutations and independent refusal

All 16 actual writer mutations returned CONFLICT: invented absent top-level/child
or null-typed values; collapsed null/valid struct parents; wrong ordinary value,
target type or metadata; wrong source/row provenance; dropped, duplicated or
reordered rows; missing/extra parts; and a truncated part. A self-consistent wrong
plan was refused from independent actual-source derivation. Eight strict bundle
metadata/inventory mutation cases were rejected. Original workload integration
retained eight successful cases and two explicit refusals with original hashes.

## Publication, interruption and accounting

Two real simultaneous publishers produced one VERIFIED and one CONFLICT; retry
remained CONFLICT. Real SIGINT returned CANCELLED. Abrupt parent SIGKILL before
publication left owned INCOMPLETE staging and no public destination, illustrating
why graceful cancellation must be distinguished from a vanished parent watchdog.
Late invalid worker stdout/crash returned ERROR; recognized disk failure returned
LIMIT_EXCEEDED. Parent hashing and post-rename elapsed failures returned
LIMIT_EXCEEDED; the post-rename diagnostic disclosed the possibly committed bundle
and retry refused without overwriting it. Source hashes remained unchanged.

One combined benign observation reconciled 16,141 bundle bytes and 29,919 owned
staging file-content bytes, with a 12-byte no-replace qualification-probe reserve.
Seven actual lowered output/staging cap cases returned LIMIT_EXCEEDED. These are
logical file-content/accounting observations, not allocated-block/RSS guarantees,
a large-Parquet benchmark or a proof of every capacity maximum.

See the four preserved safe result JSON files, developmental-coverage-results.json
and developmental-evidence-normalization.json. Native resource, shared PyArrow,
trusted-quiescent-file and scientific-semantic limitations remain unchanged.
