# Public alpha and qualification scope

ParquetDecision 0.1.0a1 is a narrowly qualified trusted-local workflow for explicit
Parquet structural decisions and preserved producer metadata. Scientific semantic
compatibility, conversion superiority and user demand remain unproven.

This public source was prepared from the clean, history-free local candidate.
Runtime modules, tests, original fixtures, packaged documentation and the forty
original baseline outcomes are unchanged. The publication preparation adds the
isolated distribution CI gates and this publication context. No private development
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
publication-only workflow/context additions. The current repository inventory is
recorded separately for publication. Compare it with the remote Git tree and the
exact commit result before treating the publication gate as complete.

No package registry upload, Git tag, GitHub Release, website deployment or upstream
pull request is part of this publication. Names remain provisional; read-only
name searches are not trademark clearance.
