# Review and explicit metadata decisions

The complete CLI supports `plan`, `inspect`, `decide`, `apply`, `verify` and
`origin`. Qualification
is Linux, Python 3.12.14 and PyArrow 25.0.0, with package version 0.1.0a1.

Metadata provenance is preserved. Scientific semantic compatibility is not established.

Approval permits structural convergence after complete feasibility preflight.
It does not convert units, establish calibration equivalence, authorize scientific
aggregation or prove transformed output correct. Sources remain unchanged.

## Plan and inspect

Select an explicit ordered inventory under an existing real source directory:

```sh
pdecision plan --sources source-root --input legacy.parquet --input new.parquet --out plan.json
pdecision inspect plan.json
```

The equivalent installed module invocation is `python -m parquet_decision`.
The `pdecision` console script and installed example are qualified separately
with source, wheel and extracted-sdist receipts in qualification.md.

`plan` performs full source-bound feasibility in the fresh supervised worker.
NEEDS_DECISIONS returns exit 1 and leaves a valid plan to inspect. READY_FOR_REVIEW
returns 0. Failure/incomplete plans can be inspected but cannot be approved.
A successful inspect returns 0 with command status INSPECTED, even for a plan
whose displayed Plan status is NEEDS_DECISIONS or a failure. Loading or rendering
an invalid/missing artifact still fails. Every source row must have been scanned, and the exact files/rows/input-bytes
counters must reconcile the full inventory. Editing a status or recomputing a
JSON hash cannot bypass those structural requirements. Digests are integrity
bindings, not signatures or proof of operator authenticity.

Inspection prints complete source records, the target schema in exact-codepoint
field order, every source-to-target operation, and every conflict with its full
SHA256 digest. Human-readable source/action summaries and UTF-8 previews accompany
the exact records; non-UTF-8 bytes use base64 previews without inferred meaning. Paths are JSON arrays of literal field-name segments: `["a.b"]`
is different from `["a","b"]`. Path `[]` names schema-level metadata. Keys and
values are canonical base64 so arbitrary metadata bytes stay exact; a conflict
value `null` means the corresponding present field lacks that key. Whole missing
fields are separate absence operations.

## Author each choice

Inspection prints a copyable decisions template, with the exact plan digest and
all conflict digests. Its placeholder `CHOOSE_archive_only_OR_abort` is deliberately
invalid. Copy the whole JSON to a new `decisions.json`, then replace the placeholder
on every individual resolution with one of these two exact actions:

- `archive_only`: omit that key from canonical target metadata; retain every
  original source metadata byte and all conflict variants in the approved plan
- `abort`: refuse approval and produce no approved output

For a two-conflict plan the grammar is:

```json
{
  "format_version": 1,
  "plan_digest": "COPY_THE_FULL_64_CHARACTER_PLAN_DIGEST_FROM_INSPECT",
  "resolutions": [
    {
      "conflict_digest": "COPY_THE_FIRST_FULL_CONFLICT_DIGEST_FROM_INSPECT",
      "action": "CHOOSE_archive_only_OR_abort"
    },
    {
      "conflict_digest": "COPY_THE_SECOND_FULL_CONFLICT_DIGEST_FROM_INSPECT",
      "action": "CHOOSE_archive_only_OR_abort"
    }
  ]
}
```

These labels are illustrative; the actual inspection template contains valid
complete digests. No implicit archive choice or blanket yes exists. The version
must be integer 1. Exactly the three outer keys and two resolution keys above are
allowed. Arrays must be arrays, digests lowercase SHA256 hex, and actions exactly
`archive_only` or `abort`. Unknown/duplicate JSON keys, floats, unknown actions,
missing records, duplicate resolutions, stale conflict digests and a mismatched
plan digest are refusals. Resolution order is accepted as authored; approved
records sort by digest. A revised source inventory requires a new decisions file,
even when its conflict digests happen to be identical. A successful zero-conflict
plan still requires the explicit file with `"resolutions": []`.

```sh
pdecision decide plan.json --decisions decisions.json --out approved.json
pdecision inspect approved.json
```

The approved record embeds the exact reviewed plan and its digest, sorted complete
resolution coverage, and its own approval digest. The planner already omits
conflicting canonical metadata. Approval checks that omission rather than silently
changing the reviewed plan or its digest. Original source fields, field order,
metadata and conflict variants remain exact. Noncanonical edited plans must be
regenerated and reviewed. Independent actual-source schema/value verification
remains a separate mandatory later stage, even for a self-consistent plan.

Stored approval inspection begins with the stored APPROVED label and a historical
approval warning before showing the embedded reviewed plan, its original
status and template. It displays the approval digest and each explicit action.
It does not assert fresh verification.

## Lower limits declaratively

`plan --limits limits.json` reads the existing strict full `Limits` JSON grammar:
all thirteen cap fields must be present, integers rather than booleans/floats.
Start with the defaults displayed by inspection and lower the needed values.
No field can exceed its alpha maximum in [profile.md](profile.md). The effective
limits remain bound into the plan and approval. The limits input file is protected
against output overwrite too. An above-maximum cap is LIMIT_EXCEEDED, exit 3;
malformed limit JSON is INVALID_INPUT, exit 2.

## Source protection and no-clobber publication

Every output must be a new destination under an existing real directory. Files,
directories, symlinks (including dangling links), hard-link aliases and input
artifacts are preserved. `plan` protects every selected source identity, including
missing selected paths, plus a supplied limits file. `decide` protects its actual
input plan and decisions file.

Portable plans intentionally omit absolute source roots. Without `--sources` on
`decide`, all existing source files and aliases remain protected by no-clobber;
unavailable absolute source roots are not checked. Supply the optional root to
also protect the exact source-relative destinations if source files are absent:

```sh
pdecision decide plan.json --sources source-root --decisions decisions.json --out approved.json
```

The Python publication interface also accepts explicit protected paths. Destination
overlap with protected input identities refuses. The supplied destination parent
traversal is checked before output-path canonicalization. Symlinked parent components refuse even in a static spelling
such as `alias/../output`, and missing parent directories are never created
implicitly.
Source files and destination parents must be trusted and quiescent; an ordinary
separate process creating the final destination during publication is supported
and safely refused. This is not hostile-filesystem race isolation.

Publication creates an operation-owned temporary JSON file on the destination
filesystem, writes/flushes/fsyncs it, then uses exported libc
`renameat2(RENAME_NOREPLACE)`. Before publication, both file and directory behavior
are qualified on that actual filesystem with existing-destination refusal and
successful new-destination rename. Unsupported kernels/filesystems/platforms or
an absent libc export yield UNSUPPORTED. There is no check-then-overwriting rename
fallback. A competing creator's destination is preserved. Cleanup removes only
owned temporaries, never user files or broad directory contents. This does not
promise power-loss durability; crashes can leave an owned temporary requiring
manual attention, and no recovery command is implemented.

A shared directory primitive is qualified for later bundle publication. It does
not inspect a dataset or claim that it is complete/verified; its caller must first
perform the later independent verification. Failed directory publication retains
the caller-owned staging directory.

## Status and exit agreement

- 0: READY_FOR_REVIEW, APPROVED or a successful INSPECTED command (VERIFIED requires complete fresh source-bound verification)
- 1: NEEDS_DECISIONS, DATA_INCOMPATIBLE, or an explicit CONFLICT such as abort,
  incomplete/stale choices or an occupied/protected destination
- 2: UNSUPPORTED or INVALID_INPUT
- 3: INCOMPLETE, LIMIT_EXCEEDED or CANCELLED
- 4: unexpected ERROR

When publication and reporting both succeed, plan/decide human status, stored
artifact status and exit agree. Inspect separates its successful INSPECTED
command status from the displayed embedded Plan status and does not change the
artifact. Publication refusal can prevent any artifact from being created;
the command reports that refusal and its exit instead. Choose a new output path
for retries. Command output is flushed inside the controlled reporting boundary.
If stdout fails, a remaining stderr channel receives a bounded status/diagnostic:
EPIPE is ERROR/4; recognized ENOMEM/ENOSPC/EDQUOT or typed MemoryError is
LIMIT_EXCEEDED/3. Failed buffered stdout is muted before interpreter shutdown so
it cannot replace that chosen exit with a failed-flush exit. A post-publication
failure explicitly identifies the artifact as already published; it is not rolled
back. Inspect the existing artifact rather than trying to overwrite it.

Operational JSON reads recover typed causes from the strict reader's ModelError
wrapper. Recognized resource/allocation refusals are LIMIT_EXCEEDED/3; unexpected
I/O failures such as EIO are ERROR/4. Malformed JSON and missing requested JSON
artifacts remain INVALID_INPUT/2. These parent failures do not claim a native
worker RLIMIT was exceeded. Failed preflight plans preserve their refusal status
when `decide` is attempted and never become APPROVED.
