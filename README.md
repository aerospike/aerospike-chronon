# aerospike-chronon

Aerospike KV Store for Chronon.

This artifact provides an [`ai.chronon.online.KVStore`](https://github.com/airbnb/chronon/blob/master/online/src/main/scala/ai/chronon/online/Api.scala)
implementation backed by the Aerospike Java async client, plus the
`ai.chronon.online.Api` wiring and a Spark bulk loader for `_upload` tables.

## Data model

```
BATCH datasets (e.g. MY_GB_BATCH)
  -> Aerospike set GROUPBY_BATCH
  -> record key: MY_GB_BATCH#<keyBytes>
  -> bin "v": K-ordered map { tsMillis -> valueBytes }

STREAMING datasets (e.g. MY_GB_STREAMING)
  -> Aerospike set GROUPBY_STREAMING
  -> one record per entity per day
  -> record key: MY_GB_STREAMING#<keyBytes>#<dayTs>
  -> bin "v": K-ordered map { tsMillis -> valueBytes }

METADATA / other datasets
  -> Aerospike set named by the dataset
  -> record key: DATASET#<keyBytes>
```

`keyBytes` is the raw Avro-encoded key from Chronon and is concatenated
verbatim; it is never round-tripped through a UTF-8 `String`, so distinct binary
keys cannot collide.

`multiPut` writes into the map bin with a key-ordered map policy. Same-ts writes
overwrite the previous value (last-write-wins), making retries idempotent.
`multiGet` applies a server-side map key-range filter so only versions with
`ts >= afterTsMillis` are returned.

## Configuration

`ChrononAerospikeApi` reads all settings from the `userConf` map supplied to
Chronon via `api.impl`:

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `aerospike.hosts` | yes | — | Comma-separated Aerospike host names / IPs |
| `aerospike.port` | no | `3000` | Aerospike service port |
| `aerospike.namespace` | yes | — | Target namespace |
| `aerospike.ttl.seconds` | no | `432000` (5 days) | Record TTL for feature records |
| `aerospike.maxConnsPerNode` | no | `300` | Max async+sync connections per node |
| `aerospike.maxCommandsInProcess` | no | `maxConnsPerNode` | Total in-flight async command cap across event loops; extra commands queue in the client. Minimum `5` (lower values are rejected). Values above `maxConnsPerNode` log a warning: on a skewed workload writes can fail with `NO_MORE_CONNECTIONS` again. A higher value mainly helps multi-node clusters, where the pool is per node |
| `aerospike.timeout.connect.millis` | no | `1000` | Connect timeout |
| `aerospike.timeout.socket.millis` | no | `30000` | Socket timeout |
| `aerospike.timeout.total.millis` | no | `30000` | Total command timeout |
| `aerospike.key.prefix` | no | — | Optional prefix prepended to every record key |
| `aerospike.streaming.maxLookbackDays` | no | `5` | Streaming read max lookback in days (must be <= TTL) |
| `aerospike.loader.batch.timeout.seconds` | no | `60` | Timeout per `multiPut` batch in the Spark loader |
| `chronon.log.consumer.class` | no | — | `java.util.function.Consumer[LoggableResponse]` impl |
| `chronon.stream.decoder.class` | yes for streaming | — | `StreamDecoder` impl constructor taking `GroupByServingInfoParsed` |

## Adoption

1. Add the `aerospike-chronon` jar to your Chronon runtime classpath.
2. Configure Chronon with the `Api` implementation class:
   ```
   api.impl=com.aerospike.chronon.ChrononAerospikeApi
   ```
   and the `userConf` keys above.
3. Run the standard Chronon `metadata-upload` so metadata rows land in the store.
4. For each group-by, run `Spark2AerospikeLoader` over the `_upload` table:
   ```
   --table-name team.my_gb_upload
   --end-ds 2026-10-13
   --aerospike-hosts host1,host2
   --namespace features
   --ttl 432000
   [--aerospike-port 3000]
   [--max-lookback-days 5]
   [--batch-timeout-seconds 60]
   [--dataset MY_GB_BATCH]
   ```
   The dataset is inferred from the table name when omitted.
5. Point streaming jobs at the same `Api` implementation; `multiPut` writes
   streaming tiles to the day-bucketed `GROUPBY_STREAMING` set.

## Operations requirements

- The target Aerospike namespace **must** run with `nsup-period > 0`. Record TTL
  is used for retention; without namespace supervisor running, expired records
  are never removed.
- Choose `aerospike.ttl.seconds` larger than your upload cadence plus an incident
  buffer. The default is 5 days, matching the zipline Redis/BigTable precedent.
- The `group_by_serving_info` row in a `*_BATCH` dataset is always written with
  never-expire TTL, even though it lives in `GROUPBY_BATCH`. This prevents a
  stalled batch upload from turning "stale features" into "every fetch throws".
- Writes beyond the connection pool queue in the client instead of failing: the
  async event loops cap in-flight commands to `aerospike.maxConnsPerNode`
  (total timeout still bounds the wait). Failed writes are still reported as
  `false` from `multiPut`; each failing `multiPut` logs one `warn` line per
  dataset with result codes, and `write.exceptions.<result_name>` counters
  (e.g. `write.exceptions.no_more_available_connections`) break failures down.
- Push mode (`multiPutWithNotification`) is not supported and throws an
  `UnsupportedOperationException` if enabled.

## API documentation

Scaladoc on `src/main` is the developer reference. Generate and browse it
locally:

```bash
sbt doc
open target/scala-2.12/api/index.html   # macOS; xdg-open on Linux
```

Output is gitignored — regenerate after pulling changes. See
[`docs/index.md`](docs/index.md) for the architecture overview and file guide.

## Testing

Requires JDK 17+ (the `ai.chronon` jars are compiled for Java 17) — run sbt with
`JAVA_HOME` pointing at a JDK 17 or 21.

- `sbt test` — unit tests only (`src/test`), no external services needed.
- `sbt it:test` — integration tests (`src/it`); each suite boots the Aerospike
  Community Edition rig via `docker compose up -d --wait` and tears it down
  after. Requires a running Docker daemon. Connection details can be overridden
  with `AEROSPIKE_HOST` / `AEROSPIKE_PORT` (defaults `localhost:3000`, namespace
  `test` with `nsup-period=2` for TTL coverage).

## Benchmarking

The `benchmark/` module is a manual, configurable load harness that compares
this implementation against the vendored Chronon MongoDB quickstart online
store (`put`/`get`/`serving`/`streaming` scenarios with correctness checks;
JSON + PDF reports). See [`benchmark/README.md`](benchmark/README.md).

## License

Apache 2.0 — see `LICENSE`.
