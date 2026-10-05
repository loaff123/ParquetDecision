# Public alpha and qualification scope

ParquetDecision 0.1.0a1 is a narrowly qualified trusted-local workflow for explicit
Parquet structural decisions and preserved producer metadata. Scientific semantic
compatibility, conversion superiority and user demand remain unproven.

This public source was prepared from the clean, history-free local candidate.
The original fixtures and forty baseline outcomes are unchanged. The first public
revision added isolated distribution CI gates and publication context. A subsequent
narrow reader-lifecycle change explicitly disables Arrow background pre-buffering
at all five native reader calls; see the incident note below. No private development
history is included.

## Qualification workflow

The `qualified-linux-alpha` workflow uses Ubuntu 24.04 and exact Python 3.12.14,
PyArrow 25.0.0 and the pinned build/test stack. It runs the full source suite,
builds wheel and sdist from that exact checkout, then runs the full suite against:

- The built wheel installed into a new virtual environment
- The extracted built sdist installed into another new virtual environment

Both installed suites run in separate directories without a `src` directory,
with isolated Python imports. The workflow asserts that every runtime module
resolves inside the intended environment, outside both source checkouts, and
that neither installation is editable. The sdist suite uses tests and support
files from the extracted sdist. A green workflow does not establish qualification
for other operating systems, Python patches, Arrow versions or hostile inputs.

Read the GitHub Actions result for the exact commit being used. This document is
a description of the gate, not a claim that a pending or unrun commit passed it.
The original independent local review receipts remain historical evidence and do
not stand in for remote exact-head CI.

## Preserved local candidate evidence

The original local review packet is preserved under `evidence/release-review`.
Its manifests, local-only statements and product archive hashes describe the
unchanged reviewed candidate at that earlier gate. They do not describe the later
workflow/context additions or the subsequent synchronous-read runtime repair.
The current repository inventory is
recorded separately for publication. Compare it with the remote Git tree and the
exact commit result before treating the publication gate as complete.

No package registry upload, Git tag, GitHub Release, website deployment or upstream
pull request is part of this publication. Names remain provisional; read-only
name searches are not trademark clearance.

## Initial remote failure and synchronous reads

The first remote run passed all 739 source tests but failed one installed-wheel
case after an isolated verifier worker exited with SIGABRT and the message
"terminate called without an active exception". The product returned ERROR,
never VERIFIED; the sdist suite was not reached. An unchanged diagnostic rerun
passed, so that intermittent observation is retained rather than dismissed.

The original 23-row fixture established a deterministic lifecycle issue: pinned
Arrow 25 defaults to background I/O pre-buffering even with column threading
disabled. Source/output Python-file callbacks ran on another native thread.
Explicit `pre_buffer=False` at all five reader constructions makes those callbacks
run on the worker's main thread while preserving complete 23-row/23-value checks.
A regression records actual callback thread identities; additional coverage checks
that every reader specifies the setting. Limits, protocol, terminal ERROR handling,
independent verification and publication semantics remain unchanged.

The exact native stack of the remote SIGABRT was not captured. Background I/O
has not been proven to cause that observed SIGABRT. This is an evidence-backed
removal of an independently demonstrated background-I/O lifecycle path, not proof
that the remote abort is fixed or that every possible native abort is prevented. Initial failure and diagnostic rerun receipts
are retained separately from the revised source/wheel/sdist qualification gates.
