# Local closure versus public gates

The current CI configuration runs the source installation/full source suite and
builds distributions. It **does not execute the installed-wheel or extracted-sdist
full suites**. Those modes have separate local qualification and independent-review
receipts in this packet. A future green result from the current workflow alone
would not reproduce those installed-mode gates.

Remote CI against the actual eventual public repository head has never run.
Public Git publication, tags, registries, Sites and other public/remote writes
have never run for this candidate. Local review approval is limited to the
qualified trusted-local alpha, and is not approval or evidence for those public
release gates, broad deployment, hostile inputs or other runtimes.
