# AerospikeConfig.scala

Parsed and validated connection/tuning settings built from Chronon's
`userConf` map. Source: `src/main/scala/com/aerospike/chronon/AerospikeConfig.scala`

## `fromUserConf` (lines 251–400)

### Summary
Parses the `aerospike.*` keys of `userConf` into a validated `AerospikeConfig`, throwing `IllegalArgumentException` naming the offending key on any missing, malformed, or inconsistent value.

### Walkthrough
- **Lines 125–131 — Hosts:** Requires `aerospike.hosts`, splits on commas, trims, drops empties, and throws when no non-empty host remains.
- **Lines 133–144 — `parseInt` helper:** Strict int parser — present-but-non-numeric values throw `IllegalArgumentException`; absent keys fall back to the supplied default.
- **Lines 146–152 — `parsePositiveInt` helper:** Wraps `parseInt` and throws when the parsed value is `<= 0`.
- **Lines 154–165 — `parseLong` helper:** Strict long parser with the same naming-the-key failure behavior as `parseInt`.
- **Lines 167–168 — Namespace:** Requires `aerospike.namespace`, throwing when absent.
- **Lines 305–331 — TTL settings:** `parseTtl` accepts a positive number of seconds (`Seconds`), `0` (`NeverExpire`, written `expiration = -1`) or `namespace` (`NamespaceDefault`, written `expiration = 0`); negative, non-numeric and `Int.MaxValue`-overflowing values are rejected naming the key. `ttl.batch.seconds`/`ttl.streaming.seconds` fall back to `ttl.seconds` (default 5 days for both).
- **Lines 333–347 — Lookback validation:** Parses `streaming.maxLookbackDays` (non-negative), then rejects the combination when `days * 86400000` exceeds the streaming TTL — only when it is `Seconds` (never-expire is unbounded; `namespace` is validated against the resolved `default-ttl` at client build, or skipped if unresolvable).
- **Line 193 — Loader timeout:** Parses `loader.batch.timeout.seconds` as a positive int (default 60).
- **Lines 195–199 — In-flight cap:** Optionally parses `maxCommandsInProcess` and rejects values below `MinCommandsInProcess` (5, the client minimum).
- **Lines 303–312 — Pool bounds:** Parses `maxConnsPerNode` (positive, default 100) and `minConnsPerNode` (non-negative, default 16), rejecting `min > max`. Both are async-only pool settings — the sync pool keeps the client default cap with floor 0.
- **Lines 314–319 — Loader pool bounds:** Parses `loader.maxConnsPerNode` (positive, default 32) and `loader.minConnsPerNode` (non-negative, default 0), rejecting `min > max`. Not part of the client cache key.
- **Lines 321–345 — Result:** Assembles the `AerospikeConfig` with parsed port/timeouts as positive ints, retries as non-negative ints, `keyPrefix` filtered to non-empty, and the validated fields above. The case class itself `require`s `socket < read/write total` whenever that class has `maxRetries > 0` (a stalled attempt would consume the whole total and retries could never fire), throwing `IllegalArgumentException` naming the socket, total and retries keys.

### TTL keys

Each accepts a positive number of seconds, `0` (never expire,
`expiration = -1`) or `namespace` (the server's `default-ttl`,
`expiration = 0`); none is part of the client cache key:

| Key | Default | Applies to |
| --- | --- | --- |
| `aerospike.ttl.seconds` | `432000` (5 days) | Fallback for both classes |
| `aerospike.ttl.batch.seconds` | `ttl.seconds` | Batch records (not the stats datasets) |
| `aerospike.ttl.streaming.seconds` | `ttl.seconds` | Streaming records, their segments and roots |

Metadata, `group_by_serving_info` and the stats datasets are always
never-expire. A positive TTL requires `nsup-period > 0` on the namespace (or
`allow-ttl-without-nsup`, testing only) — the server rejects TTL-bearing
writes with code 22 otherwise, and the client build now fails fast on that
combination (see AerospikeClientHolder.md `checkNamespacePolicy`).

### Retention and streaming-segment keys

Parsed with `parsePositiveInt` (non-numeric or `<= 0` rejected naming the key);
all five are data-shape settings excluded from the client cache key, so stores
that differ only here share one `AerospikeClient`:

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.batch.retainVersions` | `2` | Newest N versions kept per record for group-by batch, `group_by_serving_info`, stats schema keys and other metadata datasets |
| `aerospike.stats.segmentTargetKiB` | `512` | Byte target per stats timeseries day segment; each write derives its entry limit as `targetKiB * 1024 / (valueBytes + 13)` so a segment stays near the target (internal cap 1024 segments per day) |
| `aerospike.streaming.segmentMaxEntries` | `256` (provisional) | Soft limit of map entries in a streaming day segment; reaching it advances the day's `tail` |
| `aerospike.streaming.maxSegmentsPerDay` | `1024` | Cap on segments per entity-day; a write past the cap returns `false` |
| `aerospike.read.batchChunkSize` | `1000` | Max `BatchRead`s per `multiGet` batch call; larger reads split into concurrent chunks with per-chunk failure isolation (see README "Read chunking") |

### Timeout and retry keys

Timeouts parse with `parsePositiveInt`, retries with `parseNonNegativeInt`
(`0` allowed; negative/non-numeric rejected naming the key). All five are
baked into the shared client's policy templates, so they ARE part of the
client cache key:

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.timeout.socket.millis` | `250` | Per-attempt socket timeout; must be below each per-class total while that class retries (validated) |
| `aerospike.timeout.read.total.millis` | `2000` | Total timeout for batch reads, retries included |
| `aerospike.timeout.stats.read.total.millis` | `10000` | Total timeout for batch reads of the stats datasets (cache-key member) |
| `aerospike.timeout.write.total.millis` | `1000` | Total timeout for writes, retries included |
| `aerospike.maxRetries.read` | `2` | Retries of a timed-out/failed batch read (`0` disables) |
| `aerospike.maxRetries.write` | `2` | Retries of a timed-out/failed write (`0` disables) |

The previous `aerospike.timeout.total.millis` key (single 30 s total) is
removed outright. See README "Timeouts and retries" for the model and the
stuck-node measurements behind the defaults. The Spark loader overrides these
with bulk-oriented defaults (socket 5000, totals 30000, write retries 2).

### Connection-pool keys

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.maxConnsPerNode` | `100` | Async pool cap per node (cache-key member) |
| `aerospike.minConnsPerNode` | `16` | Async pool floor per node, `<= max` (cache-key member) |
| `aerospike.loader.maxConnsPerNode` | `32` | Async pool cap applied by `forLoader` to bulk loads (not in cache key) |
| `aerospike.loader.minConnsPerNode` | `0` | Async pool floor applied by `forLoader` (not in cache key) |

See README "Connection footprint" for sizing against `proto-fd-max` and the
measured idle/burst numbers.

## `forLoader` (lines 353–362)

`copy` that switches a serving config to the bulk-load profile: pool floor
and cap from the `aerospike.loader.*` keys, the loader constants
`LoaderSocketTimeoutMs` (5000) / `LoaderReadTotalTimeoutMs` (30000) /
`LoaderWriteTotalTimeoutMs` (30000) / `LoaderMaxRetriesRead` (0) /
`LoaderMaxRetriesWrite` (2), and `maxCommandsInProcess = None` so the
in-flight cap falls back to the new `maxConnsPerNode`. Used by
`AerospikeKvStore.bulkPut`; the `Spark2AerospikeLoader` CLI builds its config
from its own flags (`--max-conns-per-node` / `--min-conns-per-node`,
defaults 32 / 0) instead.

### Review
No significant issues identified.

## TLS and authentication keys

TLS is opt-in and requires Enterprise Edition. `aerospike.tls.enabled` must be
`true` before any other `aerospike.tls.*` key is accepted — a stray TLS key on
a plaintext config fails at parse rather than silently flipping the wire.
When TLS is enabled every connection (seed, tend, async commands) is TLS; the
async path uses netty event loops (the direct-NIO loops cannot speak TLS), so
netty 4.2 is bundled and **shaded** into the assembly jar
(`chronon.shaded.io.netty.*`, along with `chronon.shaded.com.aerospike.client.*`),
so TLS works out of the box even on hosts carrying netty 4.1 (Spark 3.5).
Consumers of the thin `packageBin` jar instead get `io.netty` 4.2 as a normal
POM dependency: on a host carrying netty 4.1 (e.g. Spark 3.5), enabling TLS
requires either aligning the host's netty graph to 4.2+ or switching to the
assembly jar — an unaligned classpath fails with linkage errors at client
build. Two cosmetic notes:
stack traces name the shaded classes, and JVM shutdown can log a harmless
`ClassNotFoundException` for a shaded inner class (the shutdown-hook thread's
context classloader cannot see them).

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.tls.enabled` | unset | `true`/`false`; required before any `aerospike.tls.*` key |
| `aerospike.tls.name` | unset | Cluster-wide TLS name for seeds without `host:tlsName[:port]` (seed name wins; NOT mapped to `clusterName`, which also enables cluster-name validation) |
| `aerospike.tls.protocols` | client default (TLSv1.2) | Comma-separated protocol allowlist |
| `aerospike.tls.ciphers` | JVM ciphers | Comma-separated cipher allowlist |
| `aerospike.tls.truststore.file` | unset | JKS/PKCS12 CA truststore; absent = default JVM `SSLContext` (system truststore / `-Djavax.net.ssl.trustStore`) |
| `aerospike.tls.truststore.password.file` | unset | File containing the truststore password |
| `aerospike.tls.truststore.type` | `JKS` | `JKS` or `PKCS12` |
| `aerospike.tls.keystore.file` | unset | JKS/PKCS12 keystore with client cert chain + key (mTLS / PKI) |
| `aerospike.tls.keystore.password.file` | required with keystore | File containing the keystore password |
| `aerospike.tls.keystore.key.password.file` | keystore password | File containing the key password |
| `aerospike.tls.keystore.type` | `JKS` | `JKS` or `PKCS12` |
| `aerospike.tls.forLoginOnly` | `false` | TLS for login only — credentials over TLS, then CLEARTEXT data plane; requires credentials. Not transport encryption |
| `aerospike.user` | unset | Auth user |
| `aerospike.password.file` | unset | File containing the password, read on the JVM that builds the client (each Spark executor needs it) |
| `aerospike.password.env` | unset | NAME of an environment variable holding the password, read on the JVM that builds the client (each Spark executor needs it in its own env) |
| ~~`aerospike.password`~~ | removed | Inline passwords were serialised into Spark task closures and visible in logs; setting the key fails at parse naming the two keys above |
| `aerospike.auth.mode` | `internal` | `internal` \| `external` \| `external_insecure` \| `pki` |

Seeds accept the client's native `host[:tlsName][:port]` form;
`aerospike.port` is now the default for seeds that omit a port (backward
compatible).

### Validation (all fail at `fromUserConf`, naming the keys)

- Any `aerospike.tls.*` key while `tls.enabled` is absent/false; a `host:tlsName` seed with TLS off.
- Keystore without `keystore.password.file`; orphaned truststore/keystore password files.
- `user` XOR password source; `password.file` and `password.env` together; the removed inline `password` key.
- `auth.mode=external`: requires TLS + credentials. `external_insecure`: credentials only (testing). `pki`: TLS + keystore, and forbids user/password (the cert CN is the user; provisioned server-side as a `nopassword` user).
- `forLoginOnly`: requires TLS + credentials.
- `clientCacheKey` covers TLS and auth identity (stores incl. their password-file paths, protocols, ciphers, `forLoginOnly`, auth mode, user, password file path / env var name) — a TLS config can never inherit a plaintext client, and a store pointing at a *different password source* gets its own client (a wrong source fails at connect instead of silently reusing a working client). An env var cannot change inside a JVM, so keying on its name is exact; a password file rewritten at an unchanged path reuses the existing client (restart to rotate).
- Credentials are **references, never values**: the file/env is read at client build on the JVM that builds the client (a Spark executor reads *its own* file/env), so no secret is in `userConf`, the serialised `AerospikeConfig` or Spark task closures. An unset or blank env var fails closed (`IllegalStateException` naming the variable) rather than building an unauthenticated client.
- On Spark, give executors the variable via cluster-level env/secret mounts, or `spark.executorEnv.<NAME>` (Spark's default `spark.redaction.regex` redacts config names containing "password"; name the variable accordingly if you use it).

### Test fixture

One command owns the whole TLS fixture lifecycle — certificate and Java-store
initialization, both EE 8.2.0.1 servers (4533 server auth, 4534 mTLS
required), and provisioning of `chronon-user`/`chronon-pass` plus the
`chronon-client` PKI-only user on each:

```
AEROSPIKE_TLS_UID=$(id -u) AEROSPIKE_TLS_GID=$(id -g) \
  docker compose --profile tls run --rm tls-ready
```

Requires Docker Compose v5.1.3 or later. The `AEROSPIKE_TLS_UID`/`GID` vars
chown generated material to your host user; without them it stays
container-owned and may be unreadable on native-Linux Docker (Docker Desktop
maps ownership, so it works either way). `AerospikeDockerSpec` runs the same
command automatically — passing `id -u`/`id -g` itself — once per TLS suite
and removes the fixture containers afterwards.

Generated material lives in `docker/tls-certs/` (gitignored, host-readable).
A complete valid set is reused across runs — fingerprints stay stable.
Fixture certificates are valid for **30 days**; expired, partial, or
inconsistent material is rejected rather than silently rotated. To reset:
stop the TLS services (`docker compose --profile tls stop`), delete
`docker/tls-certs/`, and rerun the setup command.

Setup readiness means TLS authentication succeeded with the fixture client
certificate and both users' roles/auth modes were verified on both servers —
it deliberately does **not** perform record operations; the
`AerospikeTlsSpec` round-trips remain the write/read verification.

Container tooling: `eclipse-temurin:21-jdk` (digest-pinned) runs OpenSSL +
`keytool` for cert/store generation (`openssl`-only PKCS12 cert bags load as
zero trusted entries in the JDK, so `keytool` is mandatory), and
`aerospike/aerospike-tools` (digest-pinned) runs `asadm`/`asinfo` for
provisioning over the service network — no `docker exec`, Docker socket, or
host tools required.
