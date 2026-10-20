# aerospike-chronon — Developer Documentation

An `ai.chronon.online.KVStore` implementation backed by the Aerospike async Java
client, plus the `Api` wiring and a Spark bulk loader. This page is the map;
the [generated Scaladoc](#generating-the-api-docs) is the reference, and the
[README](../README.md) remains the operations/config guide.

## Architecture

```
                    Chronon runtime (fetchers, streaming jobs)
                          │ api.impl=com.aerospike.chronon.ChrononAerospikeApi
                          ▼
               ┌──────────────────────┐
               │ ChrononAerospikeApi  │  userConf → everything; pluggable
               │  genKvStore          │  StreamDecoder + log consumer by FQN
               └─────────┬────────────┘
                         ▼
               ┌──────────────────────┐
               │   AerospikeKvStore   │  KVStore impl
               │  create  (no-op)     │
               │  multiPut  ──────────┼─► per-request MapOperation.put on bin "v"
               │  multiGet  ──────────┼─► BatchRead + server-side map key-range
               │  bulkPut   ──────────┼─► Spark2AerospikeLoader.upload
               └────┬───────────┬─────┘
                    │           │
        ┌───────────▼──┐   ┌────▼─────────────┐
        │AerospikeData │   │ AerospikeClient  │  per-JVM shared client keyed by
        │    Model     │   │     Holder       │  conn/tuning fields only;
        │ dataset →    │   │ NioEventLoops +  │  NioEventLoops + cached policy
        │ set & key    │   │ policy templates │  templates + shutdown hook
        └──────────────┘   └────┬─────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              ▼                                  ▼
   ┌─────────────────────┐          ┌──────────────────────┐
   │   AerospikeConfig   │          │ Spark2AerospikeLoader│  CLI + bulkPut path;
   │ userConf parse +    │          │ executor-local store,│
   │ validation rules    │          │ 1000-row chunks      │
   └─────────────────────┘          └──────────────────────┘
```

## Data model

Each record holds one bin `"v"`: a **key-ordered map `tsMillis → valueBytes`** —
one map entry per version.

| Dataset suffix | Aerospike set | Record key | TTL |
|---|---|---|---|
| `*_BATCH` | `GROUPBY_BATCH` | `[prefix]dataset#keyBytes` | `ttlSeconds` (never-expire for `group_by_serving_info`) |
| `*_STREAMING` | `GROUPBY_STREAMING` | `[prefix]dataset#keyBytes#dayTs` (one record per entity per UTC day) | `ttlSeconds` |
| anything else | the dataset name | `[prefix]dataset#keyBytes` | never expire |

## File guide

| File | Role |
|---|---|
| [`ChrononAerospikeApi`](../src/main/scala/com/aerospike/chronon/ChrononAerospikeApi.scala) | `api.impl` entry point; `userConf` contract; reflection-based `StreamDecoder` and log-consumer wiring |
| [`AerospikeKvStore`](../src/main/scala/com/aerospike/chronon/AerospikeKvStore.scala) | The `KVStore`: write path, batch-read fan-out, response ordering, failure→`Boolean`/`Failure` mapping, per-dataset metrics |
| [`AerospikeDataModel`](../src/main/scala/com/aerospike/chronon/AerospikeDataModel.scala) | Dataset→set mapping and record-key byte assembly |
| [`AerospikeConfig`](../src/main/scala/com/aerospike/chronon/AerospikeConfig.scala) | All `aerospike.*` keys, defaults, and validation rules |
| [`AerospikeClientHolder`](../src/main/scala/com/aerospike/chronon/AerospikeClientHolder.scala) | Shared per-JVM client + event loops + policy templates; which config fields key the cache |
| [`Spark2AerospikeLoader`](../src/main/scala/com/aerospike/chronon/Spark2AerospikeLoader.scala) | `_upload` table → store: dataset inference, version timestamps, chunked writes, CLI |

## Generating the API docs

```bash
cd aerospike-chronon
sbt doc
open target/scala-2.12/api/index.html
```

Requires JDK 17+ (same as the build). Output is gitignored; regenerate after
pulling changes — there is no published site.
