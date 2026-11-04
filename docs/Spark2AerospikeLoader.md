# Spark2AerospikeLoader.scala

Bulk-loads Chronon `_upload` Hive tables into the online store, used both as
the `main` CLI entry point and via `AerospikeKvStore.bulkPut`.
Source: `src/main/scala/com/aerospike/chronon/Spark2AerospikeLoader.scala`

## `datasetForTable` (lines 59–73)

### Summary
Infers the online dataset name from an upload table name, case-insensitively mapping stats/consistency uploads to their dedicated datasets and everything else to `<TABLE>_BATCH`.

### Walkthrough
- **Lines 61–62 — Logged stats:** `*_LOGGED_DAILY_STATS_UPLOAD` maps to `LogStatsBatchDataset` (checked first, since it also ends in `_DAILY_STATS_UPLOAD`-like suffix patterns).
- **Lines 63–64 — Daily stats:** `*_DAILY_STATS_UPLOAD` maps to `StatsBatchDataset`.
- **Lines 65–66 — Consistency:** `*_CONSISTENCY_UPLOAD` maps to `ConsistencyMetricsDataset`.
- **Lines 67–69 — Generic upload:** `*_UPLOAD` strips the qualifier and suffix, yielding `<BASENAME>_BATCH`.
- **Lines 70–72 — Fallback:** Any other name becomes `<NAME>_BATCH`.

### Review
No significant issues identified.

## `versionTsMillis` (lines 77–78)

### Summary
Computes the record version timestamp for a partition as `epochMillis(ds) + spanMillis` — end of the partition day, matching the fetcher's `batchEndTsMillis`.

### Walkthrough
- **Lines 77–78 — Compute:** Adds the partition's span to its start-of-day epoch millis.

### Review
No significant issues identified.

## `upload` (lines 129–208)

### Summary
Reads one partition of an upload table and `multiPut`s its rows into Aerospike via executor-local stores, failing the task when any chunk has failed writes. Returns a `LoadSummary` (`rowsRead` / `rowsWritten` / `rowsDropped`, counted with Spark accumulators — best-effort under task retries) and logs one INFO summary line per upload.

### Walkthrough
- **Lines 135–140 — Dataset:** Resolves the target dataset (explicit override or inferred) and rejects overrides not ending in `_BATCH`.
- **Lines 141–144 — Partition spec:** Reads `spark.chronon.partition.column` (default `ds`) and `spark.chronon.partition.format` (default `yyyy-MM-dd`) from the session — the same keys Chronon's `TableUtils` reads — builds a one-day `PartitionSpec`, and strict-parses `endDs` (`strictEndDs`, lines 222–235: parse, re-format, compare; failures name the value, the format and the conf key). `defaultTs` is the partition's end-of-day.
- **Lines 146–154 — Source read:** Fails naming the column, the conf key and the table's columns when the partition column is absent; filters `partitionColumn == endDs` and resolves the actual name of the `ts` column case-insensitively (`tsColumn`).
- **Lines 156–159 — Accumulators:** `rowsRead`, `rowsWritten`, `rowsDropped`, `servingInfoRows`.
- **Lines 161–187 — Partition loop:** In `foreachPartition`, skips empty partitions and creates an executor-local store via `kvStoreFactory` (test seam; production builds `AerospikeKvStore`). Per chunk: counts rows read and serving-info keys in the same pass, warns on dropped rows missing binary `key_bytes`/`value_bytes`, awaits `multiPut` per chunk for `loaderBatchTimeoutSeconds` and throws `RuntimeException` when any row reports `false`, failing the Spark task for retry.
- **Lines 189–207 — Post checks:** Zero selected rows throws `IllegalStateException` naming the table, column and partition unless `allowEmpty` (then a WARN); a group-by upload (a `_BATCH` dataset that is not one of the stats/consistency datasets) with no `group_by_serving_info` row logs a WARN; one INFO summary line closes the upload.

### Review
No significant issues identified.

## `putRequestFromRow` (lines 129–146)

### Summary
Builds a `PutRequest` from a row, or `None` when `key_bytes`/`value_bytes` are absent or non-binary.

### Walkthrough
- **Lines 133–134 — Extract:** Pulls `key_bytes` and `value_bytes` via `safeBytes`.
- **Lines 135–136 — Drop:** Returns `None` when either is missing/malformed (caller logs the drop).
- **Timestamp:** Uses the row's `ts` column (read under its actual name, so `TS`/`Ts` work too) coerced to `Long` when present and numeric, else `defaultTs`.
- **Line 144 — Result:** Wraps key, value, dataset, and timestamp in `Some(PutRequest)`.

### Review
No significant issues identified.

## `safeBytes` (lines 151–158)

### Summary
Returns a column's bytes or null when the column is absent from the schema or not binary, distinguishing "no column" from malformed rows.

### Walkthrough
- **Line 152 — Presence:** Checks the row schema for the field name.
- **Lines 153–156 — Extract:** Returns the value only when it is `Array[Byte]`, else null.

### Review
No significant issues identified.

## `main` (lines 269–285)

### Summary
CLI entry point: parses args into an `AerospikeConfig`, obtains a `SparkSession`, and runs `upload`.

### Walkthrough
- **Base config:** Parses `LoaderConf` and assembles the `aerospike.*` key map with CLI values or defaults.
- **Optional keys:** Folds in `key-prefix` and `max-commands-in-process` when supplied, then validates via `AerospikeConfig.fromUserConf`.
- **Session:** Builds the `SparkSession` via `getOrCreate` — deliberately never stopped, since embedded callers may share it. The partition column/format are not flags: they come from `spark.chronon.partition.column` / `spark.chronon.partition.format` on the session (set with `--conf` on `spark-submit`).
- **Run:** Delegates to `upload` with the parsed table, dataset, partition, config and the `--allow-empty` flag.

### Review
No significant issues identified.

## TLS/auth flags

The CLI mirrors every `aerospike.tls.*`/auth `userConf` key (mapped 1:1 in
`userConfFrom`, validated by `AerospikeConfig.fromUserConf`):

`--tls-enabled`, `--tls-name`, `--tls-protocols`, `--tls-ciphers`,
`--tls-truststore-file`, `--tls-truststore-password-file`,
`--tls-truststore-type`, `--tls-keystore-file`,
`--tls-keystore-password-file`, `--tls-keystore-key-password-file`,
`--tls-keystore-type`, `--tls-for-login-only`, `--aerospike-user`,
`--aerospike-password-file`, `--aerospike-password-env`, `--auth-mode`.

Keystore/truststore/password file paths, and the environment variable named by
`--aerospike-password-env`, must resolve on every executor host — the loader
does not ship keystore or credential material, and `--aerospike-password-env`
carries only the variable *name*. There is intentionally no inline
`--aerospike-password` flag (it would appear in process args and the Spark UI).
`--aerospike-hosts` accepts `host[:tlsName][:port]` seeds.
