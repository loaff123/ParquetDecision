# Independent review: synchronous Parquet readers

**Decision: APPROVE the narrow runtime and regression change.** This approval does not certify publication or establish the cause of the earlier SIGABRT. Final repaired-commit qualification and artifact binding remain separate gates.

## Scope and correctness

Reviewed against public baseline `7232fe8f331c75877369320e91138e45fa7e2e4c`. The runtime diff is exactly one explicit `pre_buffer=False` argument at each of five `ParquetFile` constructions: source, preflight, schema verification, value verification and origin. The installed, pinned PyArrow 25.0.0 constructor defaults this option to `True`. Column-level `use_threads=False` alone therefore does not disable this background I/O path.

All other runtime modules and all 21 pre-existing test files are byte-identical. The 23 packaged data files and 79 historical evidence files are also byte-identical. Strict input grammar, source identity checks, independent actual-source schema/value verification, resource limits, worker protocol, terminal ERROR behavior and exclusive no-clobber publication logic are unchanged. No signal suppression, retry-to-success policy or verification weakening was introduced.

## Independently executed evidence

- Against the unchanged baseline runtime, the proposed six regression cases all failed as expected. Five detected the absent explicit setting. The real worker callback test observed reads on native thread 9 while its main thread was 7, after successful complete 23-row/23-value verification
- Against the repaired runtime, **301 focused tests passed in 18.04 seconds**, including all six new cases, complete stream/schema verification, origin, strict model/source checks, no-clobber writes, worker resource configuration, nonzero/signal ERROR handling and late-failure publication refusal
- The callback regression instruments the worker only in test code. It observes actual file read callbacks and requires a verified result plus exact row/value counters. The production bootstrap and limits remain unchanged

These checks used the qualified Linux / Python 3.12.14 / PyArrow 25.0.0 stack and ordinary existing fixtures. No remote payloads, native fuzzing or reduced-address-space native investigation was performed.

## Evidence and release boundaries

The initial remote wheel failure remains a real failed gate; its unchanged green rerun is not repair evidence. The exact abort stack is unknown. The callback observations establish removal of background Python-file I/O for this fixture, **not that this I/O caused the observed SIGABRT**, nor prevention of every native abort, hostile-input safety or improved performance.

The reviewed publication text now distinguishes historical local-only status from later remote activity, explicitly excludes the runtime repair from old manifest/hash claims, and states in PUBLICATION.md and both qualification documents that background I/O has not been proven to cause the SIGABRT. Those corrections were verified. The source, freshly installed wheel and extracted-sdist suites must still pass for the final published commit, with regenerated artifact hashes/manifests; an earlier build or historical receipt cannot bind later files.

## Reviewed file SHA256s

- source.py: `e576717a1f7c4b2c0af7a4567975526748d803ae942d3dcd18b119abc90a27c9`
- preflight.py: `c873be444c141773c17f23149c58580c877201a6dc135718a65c6afbd0299f1b`
- verify_schema.py: `078f99f195661517222252f05bd5e859582b0b3cd8f576337d5ce8d56a35afc8`
- verify_values.py: `cb55d55e2fc12705fcdbb6a9625a5fdc856511e05b4a7b5be16b1e38fb4feefa`
- origin.py: `9880fb9038f20bf6704505665858ed3393f4492ed8b81ea07edaee12385ed536`
- test_synchronous_reads.py: `3ec94ab62579068e2547b4a7b200906919a9acd7d10071c434828bc7be1f757c`
