# Independent publication CI review

Verdict: **APPROVE** for the scoped publication changes. No blocking code-review findings remain. This approval is not a claim that remote CI has run or passed.

## Reviewed scope

- The qualification workflow and README publication context are changed; `PUBLICATION.md` is added.
- All original runtime/package files (50), tests (21), documentation files (6), example files (23), and original baseline/qualification evidence (5) are byte-identical to the reviewed local candidate.
- The historical review packet is preserved under `evidence/release-review`: all 67 packet files and the two original artifact manifest/scan receipts were independently compared byte-for-byte.
- The final staged source has no Git history, symlinks, or generated build/cache directories.

## Qualification gate findings

- Ubuntu 24.04 and Python 3.12.14 are selected explicitly. PyArrow 25.0.0 and the build/test dependencies are pinned. Dependency checks fail the job on incompatible installations.
- The source suite precedes a fresh wheel/sdist build. The two installed modes use separate newly created virtual environments and non-editable installs.
- Installed suites run outside the source trees, in directories containing tests and support files but no `src` directory. Python isolated mode, cleared Python path overrides, site-packages origin checks, forbidden-source path checks, and non-editable metadata checks prevent accidental checkout imports in the reviewed layout.
- The module-origin assertion covers the package's current flat runtime module layout. Existing subprocess tests derive their package location from the imported distribution; they do not redirect installed tests to the checkout.
- The sdist installation, tests, documentation, examples, and evidence are taken from the extracted freshly built sdist. The wheel suite uses the unchanged source test/support files against the installed wheel.
- GitHub Actions YAML structure, Bash syntax, and embedded Python syntax were checked independently. The final staged workflow's shell body exactly matches the local replay script.
- Publication wording accurately separates historical receipts, current gate configuration, and the still-required exact-commit remote result. It does not expand scientific, platform, performance, or hostile-input qualification claims.

## Execution evidence and remaining publication gate

The local replay output inspected during this review records 739 passing tests in each mode: source (64.18 seconds), installed wheel (62.67 seconds), and installed extracted-sdist (62.06 seconds). Both installed import probes passed on Python 3.12.14 / PyArrow 25.0.0. These are inspected local execution receipts, not a remote GitHub Actions result.

The local replay uses the pre-publication tree. Final publication context and preserved historical evidence were staged separately, and the added evidence changes the sdist contents. The final published commit must therefore complete the exact-head remote workflow, and its remote tree must match the final publication inventory, before the publication qualification gate can be called complete. Historical/local receipts do not substitute for those checks.

## Reviewed file hashes

- `.github/workflows/ci.yml`: `c1618cc99fd2e077e6c8a669b3a30773df397b4dbc16d0e64ac1207d0925d6f5`
- `README.md`: `42c2448314443fd95aef9f5bcfebef1babe6479abb48174ff3c8c1a21b602586`
- `PUBLICATION.md`: `964166a6df8c8c23b211feda60339540a954f14ac8155ffb708ba7dd6cbe7e65`
