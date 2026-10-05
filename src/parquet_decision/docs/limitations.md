# Alpha limitations

Metadata provenance is preserved. Scientific semantic compatibility is not established.

- Trusted, quiescent benign local files only; no hostile/decompression corpus,
  arbitrary caller extension objects, plugins/code, native-parser investigation,
  malicious-input sanitization or OS security sandbox claim
- Exact Linux / Python 3.12.14 / PyArrow 25.0.0 qualification only; other platforms
  and Python patches are unsupported until qualified
- Lists, maps, unions, dictionaries, extensions, dates/durations, large/fixed-size
  binary/string, float16 and negative-scale decimals are excluded; timestamp
  timezone labels must match exactly; integer/float and decimal/float mixtures refuse
- No rename inference, units/calibration conversion, deduplication, filtering,
  defaults or scientific reconciliation; archive-only retains original byte variants
- PyArrow is shared by native read/write checks; independent algorithmic schema/value
  derivation is not an independent decoder. Hashes are not signatures or authenticity
- Batch row limits cannot bound a large individual logical value. Worker RLIMIT_AS
  bounds virtual address space, not RSS, parent startup, publication or all filesystem
  activity. Parent wall watchdog cannot run after parent death
- Graceful SIGINT/SIGTERM and KeyboardInterrupt cancel/reap the owned worker and
  remove owned staging where possible. Abrupt parent kill or host shutdown may
  leave INCOMPLETE staging, or an already committed destination after final rename.
  Cleanup/reporting failures disclose that context; no recovery command or power-loss
  durability protocol exists. Source/destination parents must be trusted/quiescent
- Mandatory libc Linux renameat2(RENAME_NOREPLACE) is qualified on the actual
  destination filesystem. Unsupported capability refuses with no unsafe fallback
- Ordinary Arrow dataset read compatibility is checked. DuckDB 1.5.4's UTC-aware
  nanosecond limitation remains; it is a development-only comparison dependency
- Original synthetic evidence: explicit Arrow parity (8 successes / 2 refusals),
  close OmniMorph / paid ParquetHarmonize alternatives not executed as full apps,
  divergent early reviewer judgments and unproven user demand. No superiority,
  speed, scientific validity, market-gap or broad safety claim
- Public names are provisional, not trademark clearance. Remote CI, public source
  publication, tags and registry uploads have never run for this local candidate
