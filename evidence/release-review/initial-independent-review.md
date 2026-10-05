# Initial independent review findings and bounded repair

This is a public-safe summarized presentation of the two independent initial
reviews. Raw reports/reproductions remain separately preserved. Their workspace,
private planning references and execution paths are omitted; no new independent
approval is implied by this summary.

## Initial verdicts

The broad final code/spec/product review requested changes for two P2 findings:
R1 bundle inspection's conflict exit classification, and R2 installed documentation
navigation. It reported no additional core data-correctness, false-VERIFIED,
mutation-gate or overwrite defect within the approved trusted-local profile.
R3 concise example rerun handling was a P3 usability improvement.

The separate newcomer review passed the substantive documented journey and found
R2 a distribution usability must-fix. It independently completed a normal wheel
installation in a fresh venv through pip, which downloaded PyArrow 25.0.0, with
no source checkout/import redirect or dependency suppression. pip check passed.
The literal original journey, explicit choices, producer-specific cm interpretation,
output-only/fresh/historical labels, expected refusals and rerun byte immutability
passed. The example rerun safely refused but printed a raw FileExistsError traceback.

The broad review independently ran the initial full relevant suite in source,
installed wheel and extracted-sdist modes: 725 passed in 64.96, 60.53 and 63.56
seconds respectively. These are initial-candidate receipts, not results for the
subsequent fix. It also checked source/archive/import identities, preserved original
bytes and benign rebound wrong-value/provenance/report/source cases. Its additional
cases corroborated honest output-only labels and full fresh verifier refusals.
No remote CI, publication, tags or registry upload ran.

## Findings reproduced before the fix

- R1: changing an ordinary output part byte identity made inspect report
  INVALID_INPUT/2, while origin and verify reported CONFLICT/1 for the same bundle
- R2: all seven installed README relative links failed: six doc targets resolved
  into nonexistent docs/docs; linked baseline evidence was not installed
- R3: example rerun exited 1 and preserved every existing byte, but exposed a raw traceback

## Bounded implementation response

- R1: contextual bundle-inspect ModelError handling reports CONFLICT/1, while
  malformed standalone plan JSON remains INVALID_INPUT/2. Typed limits,
  unsupported runtime, direct/wrapped missing metadata and OS/resource causes
  retain their established classes. Reporting remains outside the integrity catch
- R2: the installed README uses six sibling-document targets and retains its
  baseline link; the four linked baseline assets are packaged byte-identically.
  Source README targets remain unchanged. Only installed link presentation changes;
  literal shell journey commands remain identical
- R3: the example entry point handles FileExistsError with a concise preservation /
  new-directory diagnostic and exit 1; exclusive creation and original bytes stay unchanged

Five focused red regressions demonstrated the reported defects before implementation.
The full relevant suite, literal installed journeys, relative-link checks,
import/test-byte identities and archive scans are rerun for the repaired artifacts.
The implementation does not change verified schema/value algorithms, types,
resource maxima, source bindings or no-clobber publication primitives.

## Remaining gate

Focused independent code and newcomer rereviews of this repair are owner-run and
pending. Implementer passes are evidence for those reviews, not independent
sign-off or a public release/remote-CI outcome.
