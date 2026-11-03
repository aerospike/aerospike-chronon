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

STATS datasets (CHRONON_STATS_BATCH, CHRONON_LOG_STATS_BATCH,
  CHRONON_CONSISTENCY_METRICS_STATS_BATCH)
  -> Aerospike set GROUPBY_BATCH (same as batch)
  -> timeseries keys: DATASET#<keyBytes>#<dayTs>#<seg> — one day-bucketed
     record per entity per day (same layout as streaming, seg 0 = day root)
     with segment records when the day overflows; never expires, never trimmed
  -> first-day marker: DATASET#<keyBytes>#first, long bin "first" = earliest
     day with data; never expires
  -> legacy entries written before this layout stay at DATASET#<keyBytes>
     (plain batch record) and are still read
  -> schema keys (…__keySchema / …__valueSchema): plain batch records bounded
     by retainVersions
  -> int bin "tail" on each day root: index of the last segment of that day

STREAMING datasets (e.g. MY_GB_STREAMING)
  -> Aerospike set GROUPBY_STREAMING
  -> one root record per entity per day, plus segment records when the day grows
  -> record key: MY_GB_STREAMING#<keyBytes>#<dayTs>#<seg>   (seg 0 = root)
  -> bin "v": K-ordered map { tsMillis -> valueBytes }
  -> int bin "tail" on the root: index of the last segment (0 = none)

METADATA / other datasets
  -> Aerospike set named by the dataset
  -> record key: DATASET#<keyBytes>
```

`keyBytes` is the raw Avro-encoded key from Chronon and is concatenated
verbatim; it is never round-tripped through a UTF-8 `String`, so distinct binary
keys cannot collide.

`multiPut` writes into the map bin with a key-ordered map policy. Same-ts writes
overwrite the previous value (last-write-wins), making retries idempotent per
`(key, ts)`. Batch and metadata records keep the newest
`aerospike.batch.retainVersions` versions, streaming days are bounded by the
TTL and the segment layout below, and the stats timeseries is permanent —
segmented, never trimmed, never expiring.
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
| `aerospike.ttl.seconds` | no | `432000` (5 days) | Record TTL for batch and streaming records: a positive number of seconds, `0` (never expire, `expiration = -1`) or `namespace` (the server's `default-ttl`, `expiration = 0`) |
| `aerospike.ttl.batch.seconds` | no | `ttl.seconds` | Batch-record TTL override (same value forms) |
| `aerospike.ttl.streaming.seconds` | no | `ttl.seconds` | Streaming-record TTL override (must cover `streaming.maxLookbackDays`) |
| `aerospike.maxConnsPerNode` | no | `100` | Async connection pool cap per node (the store uses only the async API; the sync pool stays at the client default with floor 0) |
| `aerospike.minConnsPerNode` | no | `16` | Async connections pre-opened per node, `<= maxConnsPerNode` (`0` for lazy growth) |
| `aerospike.maxCommandsInProcess` | no | `maxConnsPerNode` | Total in-flight async command cap across event loops; extra commands queue in the client. Minimum `5` (lower values are rejected). Values above `maxConnsPerNode` log a warning: on a skewed workload writes can fail with `NO_MORE_CONNECTIONS` again. A higher value mainly helps multi-node clusters, where the pool is per node |
| `aerospike.timeout.connect.millis` | no | `1000` | Connect timeout |
| `aerospike.timeout.socket.millis` | no | `250` | Per-attempt socket timeout (must be below each per-class total while that class retries) |
| `aerospike.timeout.read.total.millis` | no | `2000` | Total timeout for batch reads, retries included (matches Feast's batch total) |
| `aerospike.timeout.stats.read.total.millis` | no | `10000` | Total timeout for batch reads of the stats datasets (full-history reads move hundreds of MB; see Timeouts and retries) |
| `aerospike.timeout.write.total.millis` | no | `1000` | Total timeout for writes, retries included |
| `aerospike.maxRetries.read` | no | `2` | Retries of a timed-out/failed batch read (0 disables) |
| `aerospike.maxRetries.write` | no | `2` | Retries of a timed-out/failed write (0 disables) |
| `aerospike.loader.maxConnsPerNode` | no | `32` | Async pool cap per node applied to `bulkPut` loads (many executors share the cluster) |
| `aerospike.loader.minConnsPerNode` | no | `0` | Async pool floor per node applied to `bulkPut` loads |
| `aerospike.key.prefix` | no | — | Optional prefix prepended to every record key |
| `aerospike.streaming.maxLookbackDays` | no | `5` | Streaming read max lookback in days (must be <= the streaming TTL; skipped when it is never-expire) |
| `aerospike.loader.batch.timeout.seconds` | no | `60` | Timeout per `multiPut` batch in the Spark loader |
| `aerospike.batch.retainVersions` | no | `2` | Newest N versions kept on batch/metadata records and stats schema keys (see Version retention) |
| `aerospike.stats.segmentTargetKiB` | no | `512` | Byte target per stats timeseries segment; the rollover limit is derived per write as `targetKiB * 1024 / (valueBytes + 13)` (internal cap 1024 segments) |
| `aerospike.streaming.segmentMaxEntries` | no | `256` (provisional) | Soft limit of map entries per streaming segment before the tail advances |
| `aerospike.streaming.maxSegmentsPerDay` | no | `1024` | Hard cap on segments per entity-day; writes beyond it return `false` |
| `chronon.log.consumer.class` | no | — | `java.util.function.Consumer[LoggableResponse]` impl with a public no-arg constructor; validated and instantiated once at Api construction |
| `chronon.stream.decoder.class` | yes for streaming | — | `StreamDecoder` impl; class and `(GroupByServingInfoParsed)` constructor validated at Api construction, instantiated per call |

### Api wiring

Both `chronon.*.class` keys fail fast: constructing `ChrononAerospikeApi` with
a consumer class that is missing, not a `Consumer`, lacking a public no-arg
constructor or whose constructor throws raises an `IllegalArgumentException`
naming the key, the class and the reason (cause attached); a decoder class
that is missing, not a `StreamDecoder` or lacking the
`(GroupByServingInfoParsed)` constructor fails the same way. With no consumer
configured `logResponse` is a no-op. Chronon's own behaviour when logging
fails downstream is unchanged (an exception metric and a join-codec refresh).
The resolved consumer is `@transient` — after a Java serialization round trip
the Api resolves it again on first use, caches the outcome, and logs and
rethrows without repeating the lookup if it cannot be resolved.

`externalRegistry` is one `ExternalSourceRegistry` per Api instance — the same
object `buildFetcher` and `buildJavaFetcher` receive. Register factories once
with `registry.addFactory`, then call `registerJoinExternalSources` to create
handlers from join configs; calling it again is safe (existing handlers are
skipped). The registry is not serialized — registrations are redone in the
serving process after deserialization.

### Timeouts and retries

`socketTimeoutMillis` bounds a single attempt; the per-class totals
(`aerospike.timeout.read.total.millis`, `aerospike.timeout.write.total.millis`)
bound the whole command including retries. Socket must be strictly below each
total while that class has `maxRetries > 0` — with equal values a stuck node
consumes the total in one attempt and the retry budget never fires. The store
fails config validation when this invariant is violated (zero retries opt a
class out). Reproduced against the rig by pausing the server: `socket=200
total=200` ended in 201 ms and `socket=1000 total=1000` in 1,001 ms with a
single attempt, while `socket=100 total=1000 retries=2` made 3 attempts in
303 ms; a write with `maxRetries=0` ended at the first timeout in 101 ms
(flagged in doubt) and with `maxRetries=2` made 3 attempts in 302 ms. A
request whose every attempt times out returns `Failure` (reads) or `false`
flagged in doubt (writes).

The previous 30 s defaults pinned an in-flight slot for 30 s while Chronon's
callers (`MetadataStore` awaits 10 s by default) had already given up; the
defaults are socket 250 ms, read total 2000 ms and write total 1000 ms, so
three 250 ms attempts fit inside each total. A stuck node ends after about
three attempts whatever the total is, so a larger total only changes how long a
slow transfer may take.

**Large reads need a larger read total.** The total bounds one batch command,
and a read of many MB takes longer than the serving default. Measured on the
single-node Docker rig (5 keys per fetch, 50 KiB entries, full history): at 720 versions (30 days of
hourly entries, ~184 MB) a 1000 ms total failed about 1 op in 78 and 3000 ms
failed none; at 1440 versions (~368 MB, ~3 s per fetch) a 1000 ms total failed
almost every op while 2000 ms and 5000 ms failed none. Splitting the read into
smaller chunks did not help (chunk size 100 failed as often as 1000), because the
chunks run concurrently and share the node's bandwidth. Therefore:

- stats datasets get their own total, `aerospike.timeout.stats.read.total.millis`
  (default 10000 ms), applied to every batch read of a stats dataset (day roots,
  segments, the first-day marker, schema keys). The default is an extrapolation:
  about 1 s per command per 30 days of hourly 50 KiB entries on this rig, so
  60 days passed at 2000 ms (0 of 80 ops) and 10 s should cover roughly nine
  months. Raise it, or pass `afterTsMillis`, for longer histories. The
  measurements did not go beyond ~370 MB per fetch;
- reads of very large streaming records (for example thousands of events inside
  one day for the same key, tens of MB per request) take seconds even with 30 s
  timeouts, and fail at the serving default; raise
  `aerospike.timeout.read.total.millis` for such workloads or lower the batch
  size and event size (see "Server batch-queue rejections (error 152)").


Write retries are safe: version puts are idempotent per `(key, ts)` — a retried
map put overwrites the same map key, including the retention trim — and a
cross-segment duplicate a retried streaming put could leave is removed by the
read-side `ts` dedupe. The one exception is the guarded tail advance, a
compare-and-set (`tail == expected`): it keeps `maxRetries = 0` because a
retry after a successful-but-unacknowledged advance would re-issue a stale
filter, and the writer re-reads the root to learn the outcome. Any future
non-idempotent operation must do the same. `sleepBetweenRetries` stays 0 — it
is ignored in async mode.

The Spark loader does not inherit the serving timeouts: a saturating bulk load
tolerates much longer stalls. Its CLI defaults are socket 5000 ms, read total
30000 ms (the loader does not read; the config validation still needs
socket < total while retries > 0), write total 30000 ms, `max-retries-write` 2,
`max-retries-read` 0, connect 1000 ms — overridable via
`--socket-timeout-millis`, `--read-total-timeout-millis`,
`--write-total-timeout-millis`, `--max-retries-read`, `--max-retries-write`.
For comparison, Feast's Aerospike store uses read 250 ms / write 1000 ms /
batch 2000 ms / max retries 2 — but defaults its socket timeout to `None`,
leaving the 30 s client default and reproducing the one-attempt pitfall.

### Connection footprint

Every `AerospikeClient` opens one cluster-tend connection plus its pools to
each server node, and every connection consumes one of the node's file
descriptors (`proto-fd-max`, 15,000 by default). When the server's fds run
out, new connections fail with `-8`/`NO_MORE_CONNECTIONS` — for **all**
clients, not just the one that exhausted it.

The store uses only the async API, so `aerospike.minConnsPerNode` /
`aerospike.maxConnsPerNode` size the **async** pool only; the sync pool keeps
the client default cap with floor 0. Per client per node, connections are
`1 (tend) + floor` at idle and up to `1 + cap` under load — measured: a fresh
client with the default floor 16 opens a delta of 18 (was 33 when the sync
floor was also configured); floor 0 opens 2. Connections do not shrink after
a burst: a 20k-write burst grew the pool to ~330 connections at floor 16 and
~310 at floor 0, and they stayed open (the pool fills to demand and idles
there).

Sizing rule: keep `clients x (cap + 1)` below roughly half of
`proto-fd-max`. At 15,000 fds that gives, per executor:

| Clients (executors + drivers + fetcher) | Allowed `maxConnsPerNode` |
| --- | --- |
| 20 | ~375 |
| 50 | ~150 |
| 200 | ~37 |

The floor exists for cold-start latency: a brand-new serving client would
otherwise grow its pool connection-by-connection during the first reads,
paying connect round-trips on the hot path. Set
`aerospike.minConnsPerNode = 0` to trade that for lazy growth.

The `bulkPut` path runs many executors against the same cluster, so it does
not use the serving pool keys: it applies `aerospike.loader.maxConnsPerNode`
(default 32) / `aerospike.loader.minConnsPerNode` (default 0) via
`AerospikeConfig.forLoader`, together with the loader's bulk timeouts and
retries — without it, a fleet of loader executors each defaulting to the serving
cap could starve the serving clients by exhausting the node's fds. The `Spark2AerospikeLoader` CLI exposes the same numbers as
`--max-conns-per-node` / `--min-conns-per-node` (defaults 32 / 0).

## Version retention

The trim runs in the same `operate` as the put and is relative to the written
`ts` — it never removes the entry being written or anything newer:

- Group-by batch, `group_by_serving_info` and other metadata records keep the
  newest `aerospike.batch.retainVersions` versions (map index trim). A late
  write with an older `ts` that falls outside the retained set is trimmed
  immediately, so it cannot evict newer versions.
- The three stats datasets hold two key shapes. Timeseries keys (the join
  history) are **never trimmed and never expire** — permanent by design,
  stored as a segment chain (below). Schema keys (ending `__keySchema` or
  `__valueSchema`) keep `retainVersions` versions like other latest-only keys
  and also never expire.
- Streaming datasets are not trimmed by version count; their bound is the TTL
  and the day/segment structure below.
- A read without `afterTsMillis` on a latest-only key (anything non-stats,
  non-streaming, plus the stats schema keys) fetches just the newest entry
  server-side. A stats timeseries read fetches the whole chain, however much
  history it holds.

## Streaming segments

A streaming entity-day is a chain of records: segment 0 (the root) plus
segments `1..tail`. Writes go to the writer's hinted segment; when a segment
reaches `aerospike.streaming.segmentMaxEntries` the writer advances the root's
`tail` with a guarded compare-and-set (`filter tail == expected`,
`maxRetries = 0`) that also refreshes the root's expiration to `ttl + 1 day`.

- **Hard requirement — `failOnFilteredOut`:** root writes carry a filter
  expression (`tail` absent or `0`) and MUST be issued with
  `failOnFilteredOut = true`. Without it a filtered-out write silently
  "succeeds" without writing anything — data loss.
- **Retry/backstop:** a write that gets `FILTERED_OUT` (another writer moved
  the tail) or `RECORD_TOO_BIG` re-reads the authoritative `tail` from the root
  and retries, up to 5 attempts. At `aerospike.streaming.maxSegmentsPerDay`
  the write returns `false`. The per-store segment hint cache is only a hint;
  the root's `tail` is authoritative.
- **Reads:** a fetch reads the day roots first, then — only for days with
  `tail > 0` — one more batch for segments `1..tail` (cold keys with `tail = 0`
  incur no second round). Entries are merged across segments, deduplicated by
  `ts` (later segment wins) and sorted. `KEY_NOT_FOUND` segments — expired
  earlier than the root, or never written — are skipped.
- **Root lifetime:** the advance extends the root's expiration to `ttl + 1 day`
  so the root outlives replayed segments written later within the bucket's
  read window. Segment records restart their own TTL at write time; an expired
  earlier segment is skipped on read.
- **Same-millisecond collapse:** `(key, ts)` is the version id (matching
  zipline's Bigtable/Redis stores), so distinct events for one entity in the
  same millisecond collapse into one entry; read-side dedupe by `ts` preserves
  that contract.
- **`false` means dropped:** upstream Chronon writers do not retry a `false`
  from `multiPut`, so a false result — e.g. the `RECORD_TOO_BIG` backstop or
  `maxSegmentsPerDay` reached — drops the event. Write-failure log summaries
  name `aerospike.streaming.segmentMaxEntries`.
- **Metrics:** `streaming.segment.advances`, `streaming.segment.lost_races`,
  `streaming.segment.redirects`, `streaming.segment.backstop` (writes) and
  `streaming.segments_read`, `streaming.second_round.latency.millis` (reads).

### Sizing `segmentMaxEntries`

Target record size is roughly 1–128 KiB; the rule is
`segmentMaxEntries ≈ targetKiB / (avgEntryBytes + 13)` (13 bytes is the
measured per-entry overhead). Measured on the repo rig
(`aerospike-server:8.1.2.5`, single node):

- The old single-record ceiling at 1 KiB payloads was 1011 entries (the 1 MiB
  `max-record-size` limit); with segments the same day holds 2022+ entries.
- Same-day sweep (100 keys × 2000 events × 1 KiB in one day):
  `segmentMaxEntries` 64 → ~31 segments/key (~65 KiB each), write p50 ~2.8 ms;
  256 → ~8 segments (~260 KiB), write p50 ~2.4 ms;
  1024 → 2 segments (~1 MiB), write p50 ~2.6 ms.
- **Warning — low values hurt reads.** Read fan-out is `events per day /
  segmentMaxEntries` records. With total bytes fixed (~2 MB/key/day), 64 made
  the same-day reads collapse: ~95% of fetches were rejected with server error
  152 `All batch queues are full` at 32 concurrent 50-key fetches — versus ~35%
  at 256 and none at 1024. The provisional default 256 was chosen from these
  runs; it is not claimed optimal.

### Server batch-queue rejections (error 152)

A large hot-key fetch can be rejected by the server with `152 — All batch
queues are full` (on the rig: `batch-index-threads=15`,
`batch-max-buffers-per-queue=255`). The rejection surfaces as a `Failure` on
the affected requests only — never a partial `Success`; stored data is
untouched. The store does not retry reads, so the caller (Chronon) decides
whether to retry. Writes are single-record `operate` calls and are not subject
to 152. Observed load: an extreme same-day read (~100 MB per 50-key fetch on
one laptop node); the same record counts at 16× smaller values ran error-free
at the 20 ops/s target. This is single-node, indicative evidence — operators
running large hot-key fetches should size `batch-max-buffers-per-queue` and
`batch-index-threads` for their workload. With the default read total such a
fetch fails with a timeout (code 9, after ~2 s) instead of completing slowly or
being rejected with 152: at 10 keys per request (~20 MB) the same-day shape took
2.4-2.9 s at p50 with 30 s timeouts, and every request failed at 1000 ms
(`baseline-4c`, `baseline-10b-timeout-diagnosis`). Raise
`aerospike.timeout.read.total.millis` for such workloads.

## Read chunking

Every batch read a `multiGet` issues — the root reads, the stats marker/legacy
round and the segment reads — is split into chunks of at most
`aerospike.read.batchChunkSize` reads (default 1000) by one shared helper,
the chunks run concurrently and the results merge in request order. A fetch at
or below the chunk size issues exactly one batch, so the common case is
unchanged.

Why chunk at all: `batch-max-requests` (the limit this guards) was removed in
Database 6.4.0 — before 6.4 the default is 5,000 keys per node sub-batch and an
oversized call fails whole with error 151. On the repo rig (aerospike-server
8.1.2.5) the setting reads 0: single batches of 6,000, 20,000 and 50,000 reads
all succeeded (30, 72 and 140 ms), while the same reads in parallel chunks of
1,000 took 19, 53 and 108 ms (single run, single node — indicative only). Other
limits remain: `batch-max-buffers-per-queue` rejects new batches with error 152
when all queues are full (reproduced on the single-node rig).
Chunking also isolates failures: a failed chunk fails only the requests that
have a read in it — a request whose reads span chunks fails if any of them
failed — never the whole call, and it keeps every batch under the pre-6.4
default for older clusters. Metrics: `read.chunks` (distribution of chunks per
round, per dataset) and `read.chunk.failures` (per failed chunk containing a
read of that dataset).

## Metrics

All store metrics emit on environment `kv_store` with metric prefix
`aerospike.<dataset>` (`Metrics.Context("kv_store").withSuffix("aerospike").withSuffix(dataset)`).
Chronon 0.0.115 has no `Environment.KVStore` (and `Environment` is a String
alias), so the store defines the same string itself. This is a separate
namespace from the Fetcher's own `group_by.fetch.*` metrics, which cover the
same traffic from Chronon's side.

Inherited Chronon names (`Metrics.Name.*`, literal strings):

| Metric | Kind | Meaning |
|---|---|---|
| `request.count` | counter | requests received — one per `multiPut`/`multiGet` request, including invalid/rejected ones |
| `response.count` | counter | successful requests (`Success` reads, `true` writes) |
| `fetch.exception_count` | counter | failed `multiGet` requests, including invalid ones |
| `latency.millis` | distribution | `multiGet` wall time — one sample per dataset per call |
| `write.latency.millis` | distribution | `multiPut` wall time — one sample per dataset per call |

Store-specific names (`AerospikeKvStore` companion constants):

| Metric | Kind | Meaning |
|---|---|---|
| `write.exceptions` | counter | server-side write failures (a cluster call was made) |
| `write.exceptions.<result_name>` | counter | same, split per result code (`record_too_big`, `timeout`, …) |
| `write.rejected` | counter | client-side rejected requests (null key/value, bad dataset name) — failures that are NOT server errors |
| `read.chunks` | distribution | chunks per batch-read round, one sample per dataset in the round |
| `read.chunk.failures` | counter | one per failed chunk containing a read of that dataset |
| `chunk.latency.millis` | distribution | one sample per chunk per dataset it contains |
| `streaming.segment.advances` / `.lost_races` / `.redirects` / `.backstop` | counters | tail-advance races/redirects/backstops on the streaming write path |
| `streaming.segments_read` | distribution | segment records read per request that needed a second round |
| `streaming.second_round.latency.millis` | distribution | second-round wall time per request |

Deriving failure ratios: `1 - response.count / request.count` per dataset is
the failure ratio for both reads and writes; reads split failures under
`fetch.exception_count`, writes under `write.exceptions*` (server) and
`write.rejected` (client-side). Zero-read requests (e.g. a streaming fetch
whose `afterTsMillis` is in the future) count as successful requests.

## Stats timeseries

Chronon's `SummaryJob` writes the join stats history (hourly by default, up to
24 entries/day/join) into the three stats datasets. That history is
**permanent**: timeseries records never expire (`expiration = -1`) and are
never trimmed, because reads must return the full history however far back the
query range reaches. (Zipline's Redis store cannot hold stats history at all.)

Storage is the streaming layout reused for stats: one day-bucketed record per
join per day (`DATASET#<keyBytes>#<dayTs>#<seg>`, seg 0 = the day root with a
`tail` bin), in set `GROUPBY_BATCH`, plus a first-day marker record
`DATASET#<keyBytes>#first` (long bin `first` = the earliest day with data).
The guarded root write, CAS tail advance, `failOnFilteredOut` requirement and
`RECORD_TOO_BIG`/`FILTERED_OUT` retry semantics are identical to streaming.
Entries written before this layout stay at the plain legacy key
`DATASET#<keyBytes>` and are still read and merged — nothing new writes there.

Each write first lowers the marker to its day if needed (guarded write applied
only when `first` is absent or greater — an atomic min, not a read-modify-write;
a bounded in-memory cache skips the write once the day is ensured; a marker
failure fails the write, so a day can never exist that a full-history read
cannot find). Segments are sized in bytes: `aerospike.stats.segmentTargetKiB`
(default 512) is the per-day soft size target, and each write derives its
rollover limit as `targetKiB * 1024 / (valueBytes + 13)` (13 bytes is the
measured per-entry map overhead), so a segment holds ~512 KiB whatever the
entry size. An entry at or above the target still gets a segment of one; the
cap is 1024 segments **per day**, and a write past it returns `false`.

A timeseries read runs three rounds: round 0 reads the marker and the legacy
plain key; round 1 reads the day roots for `[max(dayTs(afterTsMillis), first) .. today]`
(`first .. today` when `afterTsMillis` is absent — `afterTsMillis = 0` is
clamped to `first`, never 1970), each with the range-filtered map read plus
`tail`; round 2 reads segments `1..tail` of days that have them; then
everything merges and dedupes by `ts`. Range cost is proportional to the days
requested, not to history; a full-history read costs one root read per day
plus segments. Days with no data are `KEY_NOT_FOUND` and skipped. Schema keys
are detected by the `__keySchema`/`__valueSchema` suffix and stay bounded
(`retainVersions`) latest-only records.

Measured entry sizes (a real entry is almost all KLL sketches, k = 128, about
1.5 KiB per numeric column at 10k–100k rows per bucket; smaller for fewer
rows): ~10–11 KiB for a 10-column
join (7 numeric), ~50–56 KiB for 50 columns (35 numeric), ~214–240 KiB for 200
columns (150 numeric), ~500–559 KiB for 500 columns (350 numeric). At the
hourly default (up to 24 entries/day/join) that is roughly **98 MB / 490 MB /
2.1 GB / 4.9 GB per join per stats dataset per year** (entry bytes × 24 ×
365), and a full-history read reads all of it. Storage grows without bound by
design; bounding it is an out-of-band operator task — the day-bucket layout
makes pruning straightforward (delete day keys by range).

A stray entry with a bad timestamp (e.g. epoch 0) lowers `first` permanently,
so full-history and clamped reads then enumerate every day since (the reads
still return correct data, only slower — mostly `KEY_NOT_FOUND` round trips).
The fix is to write the right `first` value directly into the marker record;
do NOT delete the marker while stores run (a running store caches that it has
ensured it and would not rewrite it).

## Tiled Flink GroupBys

Tiled Flink GroupBys are **not supported**. The Flink writer fires on every
event and emits a cumulative IR with `ts = latest event ts` and no tile key;
the fetcher merges every returned tile, so a `(key, ts)`-versioned store keeps
each cumulative update as a separate entry and the fetcher would over-count.
Airbnb's own docs warn that "you will also likely need to modify your `KVStore`
implementation while integrating Flink".

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
   [--allow-empty]
   ```
   The dataset is inferred from the table name when omitted. `--max-lookback-days`
   defaults to `0` for the loader (it writes batch data only), so a `--ttl`
   shorter than 5 days is fine; pass it only if the loaders' executors also
   serve streaming reads. `--end-ds` must be
   in the session's partition format: the loader reads
   `spark.chronon.partition.column` (default `ds`) and
   `spark.chronon.partition.format` (default `yyyy-MM-dd`) from the Spark conf —
   the same keys Chronon's `TableUtils` uses — so pass e.g.
   `--conf spark.chronon.partition.format=yyyyMMdd
   --conf spark.chronon.partition.column=dt` on `spark-submit` for
   non-default tables. A partition that selects zero rows fails the load
   (naming table, column and partition) unless `--allow-empty` is passed;
   each run logs a summary line (`read=… written=… dropped=…`), and group-by
   uploads without a `group_by_serving_info` row log a warning.
5. Point streaming jobs at the same `Api` implementation; `multiPut` writes
   streaming tiles to the day-bucketed `GROUPBY_STREAMING` set.

## Operations requirements

- TTL-bearing records need the namespace supervisor: with `nsup-period = 0` on
  the namespace, the server rejects every write that carries a TTL with result
  code 22 (`FAIL_FORBIDDEN`) — and nothing would warn you before the first
  failure. The store now reads `nsup-period`, `allow-ttl-without-nsup` and
  `default-ttl` once when it builds a client and **fails construction** naming
  the settings and the options (enable the supervisor, set
  `allow-ttl-without-nsup` for testing only, or configure TTL `0`). If the
  info request is unavailable or unparsable the check is skipped with a WARN.
- Size `aerospike.ttl.batch.seconds` above the longest tolerable upload outage
  plus the upload cadence; `aerospike.ttl.streaming.seconds` must cover the
  streaming lookback. Both default to `aerospike.ttl.seconds` (5 days, the
  zipline Redis/BigTable precedent). A batch outage longer than the batch TTL
  silently loses history: keys missing batch and streaming data read as no
  value; keys missing only batch read as streaming-only partial aggregates for
  windows longer than the lookback (no exception). Alert on Chronon's own
  signal — `group_by.degraded_counter.count` and the `checkLateBatchData`
  warning; the store adds no signal of its own.
- The `group_by_serving_info` row in a `*_BATCH` dataset is always written with
  never-expire TTL, even though it lives in `GROUPBY_BATCH`. This prevents a
  stalled batch upload from turning "stale features" into "every fetch throws".
- Writes beyond the connection pool queue in the client instead of failing: the
  async event loops cap in-flight commands to `aerospike.maxConnsPerNode`
  (the write total timeout still bounds the wait). Failed writes are still reported as
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
  Community Edition rig via `docker compose up -d --wait` and stops/removes its
  containers after (the `aerospike-chronon-it` project network is left alone so
  unrelated Docker fixtures on the machine are never disturbed).
  Requires a running Docker daemon. Connection details can be overridden
  with `AEROSPIKE_HOST` / `AEROSPIKE_PORT` (defaults `localhost:3000`, namespace
  `test` with `nsup-period=2` for TTL coverage).

## Packaging and running through Chronon

`sbt assembly` builds the online jar — a fat jar containing
`com/aerospike/chronon/` and the Aerospike Java client, with the Scala library,
Chronon, Spark, slf4j and logback left out because the host JVM supplies them:

```
target/scala-2.12/aerospike-chronon-assembly-0.0.2-SNAPSHOT.jar   # Scala 2.12
target/scala-2.13/aerospike-chronon-assembly-0.0.2-SNAPSHOT.jar   # Scala 2.13
```

Use the jar whose Scala version matches the Chronon runtime (2.12 for the
current Chronon releases). The Chronon jars (`ai.chronon` `api` and `online`)
must be on the host classpath — the jar deliberately does not carry them.
`scripts/check-assembly.sh` verifies the jar's contents (run it after
`assembly`/`+assembly`).

Point Chronon's `run.py` at it:

```
python run.py --mode=fetch \
  --online-jar=target/scala-2.12/aerospike-chronon-assembly-0.0.2-SNAPSHOT.jar \
  --online-class=com.aerospike.chronon.ChrononAerospikeApi \
  -Zaerospike.hosts=host1,host2 -Zaerospike.namespace=features ...
```

For bulk loads, submit the loader from the same jar:

```
spark-submit --class com.aerospike.chronon.Spark2AerospikeLoader \
  [--conf spark.chronon.partition.column=dt --conf spark.chronon.partition.format=yyyyMMdd] \
  target/scala-2.12/aerospike-chronon-assembly-0.0.2-SNAPSHOT.jar \
  --table-name team.my_gb_upload --end-ds 2026-10-13 \
  --aerospike-hosts host1,host2 --namespace features --ttl 432000 [--allow-empty]
```

## Cross-build and CI

The library cross-builds for Scala 2.12.18 and 2.13.12 (`crossScalaVersions`
on the root project; `ThisBuild / scalaVersion` stays 2.12.18).

- `sbt +test` — unit tests on both Scala versions.
- `sbt +assembly` — per-version online jars (run `scripts/check-assembly.sh`
  to verify them).
- `sbt +It/test` — integration tests on both versions (Docker; run
  `sbt assembly` first for `AssemblyClassLoadingSpec`, which cancels without a
  jar).

## Releasing

`.github/workflows/release.yml` publishes to Aerospike's JFrog
(`connect-maven-dev-local`) through `aerospike/shared-workflows`: it runs
`scripts/build-release-artifacts.sh <version>`, GPG-signs the output and
deploys it. Each Scala version publishes two artifacts under `com.aerospike`:

- `aerospike-chronon_<scala>` — the library jar with sources, javadoc and a POM
  that declares its dependencies.
- `aerospike-chronon-assembly_<scala>` — the online jar for `--online-jar`,
  with a POM that declares no dependencies. It ships the library's sources and
  javadoc jars, which Maven Central requires for every artifact.

The workflow never runs on pull requests or merges; it has two triggers:

`build.sbt` always carries a `-SNAPSHOT` version (for example
`0.0.1-SNAPSHOT`); published versions are derived from it:

- **Release** — pushing a semantic version tag (`vX.Y.Z` or `vX.Y.Z-suffix`).
  The artifacts carry the tag's version, and its `X.Y.Z` must match the
  `build.sbt` version without `-SNAPSHOT` or the run fails, so `v0.0.1` and
  `v0.0.1-rc1` both release from `0.0.1-SNAPSHOT`. The release also creates the
  `aerospike-chronon` release bundle and promotes it to DEV and TEST. To
  release, run `git tag v0.0.1 && git push origin v0.0.1` on the commit to
  ship, then bump `build.sbt` to the next `-SNAPSHOT`.
- **Dev build** — Actions → Release → Run workflow, choosing the branch. It
  publishes `<X.Y.Z>-dev.<run number>` (for example `0.0.1-dev.42`) with no
  release bundle. Tick **dry-run** to build and sign without uploading
  anything.

## License

Apache 2.0 — see `LICENSE`.
