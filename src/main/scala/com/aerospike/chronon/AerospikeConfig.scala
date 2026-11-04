/*
 * Copyright (C) 2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.chronon

import com.aerospike.client.Host

/** A record TTL mode: positive seconds, never expire (`expiration = -1`) or
  * defer to the namespace default (`expiration = 0`). */
sealed trait TtlSetting {

  /** The write-policy expiration for the mode. */
  def expiration: Int
}

object TtlSetting {

  /** Records expire `seconds` after write (requires the namespace supervisor). */
  final case class Seconds(seconds: Int) extends TtlSetting { def expiration: Int = seconds }

  /** Records never expire (`expiration = -1`). */
  case object NeverExpire extends TtlSetting { def expiration: Int = -1 }

  /** The server's `default-ttl` applies (`expiration = 0`). */
  case object NamespaceDefault extends TtlSetting { def expiration: Int = 0 }
}

/** How the client authenticates with the cluster. `Pki` uses the client
  * certificate (the cert CN is the user) and forbids user/password;
  * `External` sends the password over TLS to a server-side directory;
  * `ExternalInsecure` sends it without TLS (testing only). */
sealed trait AuthModeSetting { def key: String }

object AuthModeSetting {
  case object Internal         extends AuthModeSetting { val key = "internal" }
  case object External         extends AuthModeSetting { val key = "external" }
  case object ExternalInsecure extends AuthModeSetting { val key = "external_insecure" }
  case object Pki              extends AuthModeSetting { val key = "pki" }

  val all: Seq[AuthModeSetting] = Seq(Internal, External, ExternalInsecure, Pki)

  def parse(configKey: String, value: String): AuthModeSetting =
    all.find(_.key == value.trim.toLowerCase).getOrElse(
      throw new IllegalArgumentException(
        s"Config key '$configKey' must be one of ${all.map(_.key).mkString("|")}, got: '$value'"))
}

/** TLS material for the client, present only when `aerospike.tls.enabled=true`.
  * Keystore/truststore files are JKS or PKCS12; store passwords come from
  * *files* so secrets stay out of `userConf`. */
final case class TlsSettings(tlsName: Option[String],
                             protocols: Option[Seq[String]],
                             ciphers: Option[Seq[String]],
                             truststoreFile: Option[String],
                             truststorePasswordFile: Option[String],
                             truststoreType: String,
                             keystoreFile: Option[String],
                             keystorePasswordFile: Option[String],
                             keystoreKeyPasswordFile: Option[String],
                             keystoreType: String,
                             forLoginOnly: Boolean)

/** Credentials as a *reference*, never a value: exactly one of a password
  * file or the name of an environment variable, resolved at client build on
  * the JVM that builds the client (so on a Spark executor it is the
  * executor's file/env, and no secret travels in the serialised config or
  * task closure). */
final case class Credentials(user: String, passwordFile: Option[String], passwordEnv: Option[String])

/** A parsed `host[:tlsName][:port]` seed. Kept as a plain case class (not
  * `com.aerospike.client.Host`) because the config is serialized to Spark
  * executors and `Host` is not `Serializable`. */
final case class Seed(name: String, tlsName: Option[String], port: Int)

/** Parsed and validated connection/tuning settings for the store, built by
  * [[AerospikeConfig.fromUserConf]] from Chronon's `userConf` map.
  *
  * Only some fields shape the shared client — hosts, port, pool bounds,
  * `maxCommandsInProcess`, and the three timeouts form
  * `AerospikeClientHolder`'s cache key; namespace, TTL, key prefix, lookback,
  * and loader timeout are per-store and never create a second client.
  *
  * @param hosts                     cluster seeds (required, non-empty); each
  *                                  `userConf` entry is `host[:tlsName][:port]`
  *                                  with `port` filling seeds that omit one
  * @param port                      default service port (default 3000)
  * @param namespace                 target namespace (required)
  * @param batchTtl                  TTL for batch records (default 5 days): a
  *                                  positive number of seconds, `0` = never expire
  *                                  (`expiration = -1`) or `namespace` = defer to the
  *                                  namespace default (`expiration = 0`)
  * @param streamingTtl              TTL for streaming records and their segments
  *                                  (default: the shared `aerospike.ttl.seconds`);
  *                                  also caps effective streaming read lookback
  *                                  when it resolves to a positive number of seconds
  * @param maxConnsPerNode           async connection pool cap per node (default 100);
  *                                  the sync pool is not used
  * @param minConnsPerNode           async pre-opened pool floor per node, `<= maxConnsPerNode`
  *                                  (default 16; 0 restores lazy growth)
  * @param connectTimeoutMillis      cluster-tend/seed connect timeout AND
  *                                  per-policy connect timeout (default 1000)
  * @param socketTimeoutMillis       per-attempt socket timeout (default 250); must
  *                                  be below each per-class total when that class
  *                                  retries, or a stalled attempt would consume
  *                                  the whole total and no retry could fire
  * @param readTotalTimeoutMillis    total timeout bounding a whole batch-read
  *                                  command including retries (default 2000)
  * @param statsReadTotalTimeoutMillis total timeout for batch reads of the stats
  *                                  datasets, whose history reads can move hundreds
  *                                  of MB per round (default 10000)
  * @param writeTotalTimeoutMillis   total timeout bounding a whole write command
  *                                  including retries (default 1000)
  * @param maxRetriesRead            retries of a timed-out/failed batch read
  *                                  (default 2; 0 disables)
  * @param maxRetriesWrite           retries of a timed-out/failed write
  *                                  (default 2; 0 disables)
  * @param keyPrefix                 optional prefix prepended to every record key
  * @param streamingMaxLookbackDays  max day-buckets read per streaming `multiGet`;
  *                                  must satisfy `days * 86400s <= streamingTtl`
  *                                  when the streaming TTL is positive seconds
  * @param loaderBatchTimeoutSeconds per-chunk `multiPut` await in the Spark loader
  *                                  (default 60)
  * @param loaderMaxConnsPerNode     async pool cap per node applied by
  *                                  [[AerospikeConfig.forLoader]] (default 32)
  * @param loaderMinConnsPerNode     async pool floor per node applied by
  *                                  [[AerospikeConfig.forLoader]] (default 0)
  * @param maxCommandsInProcess      total in-flight async command cap across event
  *                                  loops; minimum [[AerospikeConfig.MinCommandsInProcess]],
  *                                  defaults to `maxConnsPerNode` when unset
  * @param retainVersions            versions kept per record for group-by batch,
  *                                  serving-info and other metadata (default 2)
  * @param statsSegmentTargetKiB     byte target of a stats timeseries segment; the
  *                                  per-write entry limit is derived from it (default 512);
  *                                  retention fields do not affect the client cache key
  * @param segmentMaxEntries         streaming day record soft limit, in entries; a writer
  *                                  that reaches it advances the day's tail segment (default 256)
  * @param maxSegmentsPerDay         cap on segments per entity and day; a runaway key stays in
  *                                  the last segment and surfaces `RECORD_TOO_BIG` (default 1024)
  * @param readBatchChunkSize        maximum `BatchRead`s per `multiGet` batch call; larger
  *                                  reads are split into chunks issued concurrently (default 1000)
  */
final case class AerospikeConfig(hosts: Seq[Seed],
                                 port: Int,
                                 namespace: String,
                                 batchTtl: TtlSetting,
                                 streamingTtl: TtlSetting,
                                 maxConnsPerNode: Int,
                                 minConnsPerNode: Int,
                                 connectTimeoutMillis: Int,
                                 socketTimeoutMillis: Int,
                                 readTotalTimeoutMillis: Int,
                                 writeTotalTimeoutMillis: Int,
                                 maxRetriesRead: Int,
                                 maxRetriesWrite: Int,
                                 keyPrefix: Option[String],
                                 streamingMaxLookbackDays: Int,
                                 loaderBatchTimeoutSeconds: Int,
                                 maxCommandsInProcess: Option[Int] = None,
                                 retainVersions: Int = AerospikeConfig.DefaultRetainVersions,
                                 statsSegmentTargetKiB: Int = AerospikeConfig.DefaultStatsSegmentTargetKiB,
                                 segmentMaxEntries: Int = AerospikeConfig.DefaultSegmentMaxEntries,
                                 maxSegmentsPerDay: Int = AerospikeConfig.DefaultMaxSegmentsPerDay,
                                 readBatchChunkSize: Int = AerospikeConfig.DefaultReadBatchChunkSize,
                                 loaderMaxConnsPerNode: Int = AerospikeConfig.DefaultLoaderMaxConnsPerNode,
                                 loaderMinConnsPerNode: Int = AerospikeConfig.DefaultLoaderMinConnsPerNode,
                                 statsReadTotalTimeoutMillis: Int = AerospikeConfig.DefaultStatsReadTotalTimeoutMs,
                                 tls: Option[TlsSettings] = None,
                                 authMode: AuthModeSetting = AuthModeSetting.Internal,
                                 credentials: Option[Credentials] = None) {

  // A stalled attempt consumes socketTimeout; when it is not below the class
  // total there is no time left for a retry, so retries could never fire.
  require(!(maxRetriesRead > 0 && socketTimeoutMillis >= readTotalTimeoutMillis),
    s"Config key '${AerospikeConfig.SocketTimeoutKey}' ($socketTimeoutMillis) must be below " +
      s"'${AerospikeConfig.ReadTotalTimeoutKey}' ($readTotalTimeoutMillis) while " +
      s"'${AerospikeConfig.MaxRetriesReadKey}' is $maxRetriesRead (> 0); retries could never fire")
  require(!(maxRetriesRead > 0 && socketTimeoutMillis >= statsReadTotalTimeoutMillis),
    s"Config key '${AerospikeConfig.SocketTimeoutKey}' ($socketTimeoutMillis) must be below " +
      s"'${AerospikeConfig.StatsReadTotalTimeoutKey}' ($statsReadTotalTimeoutMillis) while " +
      s"'${AerospikeConfig.MaxRetriesReadKey}' is $maxRetriesRead (> 0); retries could never fire")
  require(!(maxRetriesWrite > 0 && socketTimeoutMillis >= writeTotalTimeoutMillis),
    s"Config key '${AerospikeConfig.SocketTimeoutKey}' ($socketTimeoutMillis) must be below " +
      s"'${AerospikeConfig.WriteTotalTimeoutKey}' ($writeTotalTimeoutMillis) while " +
      s"'${AerospikeConfig.MaxRetriesWriteKey}' is $maxRetriesWrite (> 0); retries could never fire")
}

/** `userConf` keys (`aerospike.*`), their defaults, and the parser. All
  * numeric values are parsed strictly — a malformed number throws
  * `IllegalArgumentException` naming the offending key rather than silently
  * falling back to a default.
  */
object AerospikeConfig {
  val MaxCommandsInProcessKey     = "aerospike.maxCommandsInProcess"
  /** The client rejects `maxCommandsInProcess` in 1..4 per event loop, so the
    * total cap may not be set below this floor. */
  val MinCommandsInProcess        = 5

  /** Required. Comma-separated seed hosts (e.g. `'host1,host2'`). */
  val HostsKey                    = "aerospike.hosts"
  /** Service port. Default [[DefaultPort]]. */
  val PortKey                     = "aerospike.port"
  /** Required. Target namespace. */
  val NamespaceKey                = "aerospike.namespace"
  /** Record TTL for batch and streaming records; also caps streaming lookback.
    * A positive number of seconds, `0` (never expire) or `namespace` (defer to
    * the namespace default). Default [[DefaultTtlSeconds]] (5 days). */
  val TtlSecondsKey               = "aerospike.ttl.seconds"
  /** Batch-record TTL override; falls back to [[TtlSecondsKey]]. */
  val TtlBatchSecondsKey          = "aerospike.ttl.batch.seconds"
  /** Streaming-record TTL override; falls back to [[TtlSecondsKey]]. */
  val TtlStreamingSecondsKey      = "aerospike.ttl.streaming.seconds"
  /** Async connection pool cap per node (the sync pool is unused). Default [[DefaultMaxConnsPerNode]]. */
  val MaxConnsPerNodeKey          = "aerospike.maxConnsPerNode"
  /** Async pre-opened pool floor per node, `<= maxConnsPerNode`. Default [[DefaultMinConnsPerNode]]. */
  val MinConnsPerNodeKey          = "aerospike.minConnsPerNode"
  /** Async pool cap per node applied by [[forLoader]]. Default [[DefaultLoaderMaxConnsPerNode]]. */
  val LoaderMaxConnsPerNodeKey    = "aerospike.loader.maxConnsPerNode"
  /** Async pool floor per node applied by [[forLoader]]. Default [[DefaultLoaderMinConnsPerNode]]. */
  val LoaderMinConnsPerNodeKey    = "aerospike.loader.minConnsPerNode"
  /** Cluster-tend/seed connect timeout and per-policy connect timeout. Default [[DefaultConnectTimeoutMs]]. */
  val ConnectTimeoutKey           = "aerospike.timeout.connect.millis"
  /** Per-attempt socket timeout. Default [[DefaultSocketTimeoutMs]]. */
  val SocketTimeoutKey            = "aerospike.timeout.socket.millis"
  /** Total timeout for batch reads, retries included. Default [[DefaultReadTotalTimeoutMs]]. */
  val ReadTotalTimeoutKey         = "aerospike.timeout.read.total.millis"
  /** Total timeout for batch reads of the stats datasets, retries included. Default [[DefaultStatsReadTotalTimeoutMs]]. */
  val StatsReadTotalTimeoutKey    = "aerospike.timeout.stats.read.total.millis"
  /** Total timeout for writes, retries included. Default [[DefaultWriteTotalTimeoutMs]]. */
  val WriteTotalTimeoutKey        = "aerospike.timeout.write.total.millis"
  /** Retries of a timed-out/failed batch read. Default [[DefaultMaxRetriesRead]]. */
  val MaxRetriesReadKey           = "aerospike.maxRetries.read"
  /** Retries of a timed-out/failed write. Default [[DefaultMaxRetriesWrite]]. */
  val MaxRetriesWriteKey          = "aerospike.maxRetries.write"
  /** Optional prefix prepended to every record key. */
  val KeyPrefixKey                = "aerospike.key.prefix"
  /** Max day-buckets read per streaming `multiGet`; must not exceed the TTL. Default [[DefaultStreamingMaxLookbackDays]]. */
  val StreamingMaxLookbackDaysKey = "aerospike.streaming.maxLookbackDays"
  /** Per-chunk `multiPut` await in the Spark loader. Default [[DefaultLoaderBatchTimeoutSeconds]]. */
  val LoaderBatchTimeoutSecondsKey = "aerospike.loader.batch.timeout.seconds"
  /** Versions kept per record for group-by batch, serving-info and metadata. Default [[DefaultRetainVersions]]. */
  val RetainVersionsKey           = "aerospike.batch.retainVersions"
  /** Soft size target of a stats timeseries segment in KiB; the per-write entry limit is
    * `targetKiB * 1024 / (valueBytes + 13)` so a segment stays near the target regardless
    * of entry size. Default [[DefaultStatsSegmentTargetKiB]]. */
  val StatsSegmentTargetKiBKey    = "aerospike.stats.segmentTargetKiB"
  /** Soft entry limit of a streaming day record before the tail advances. Default [[DefaultSegmentMaxEntries]]. */
  val SegmentMaxEntriesKey        = "aerospike.streaming.segmentMaxEntries"
  /** Cap on segments per entity and day. Default [[DefaultMaxSegmentsPerDay]]. */
  val MaxSegmentsPerDayKey        = "aerospike.streaming.maxSegmentsPerDay"
  /** Maximum `BatchRead`s per batch call issued by `multiGet` (root, stats and
    * segment reads alike). Default [[DefaultReadBatchChunkSize]]. */
  val ReadBatchChunkSizeKey       = "aerospike.read.batchChunkSize"

  /** Enable TLS on all connections. Required before any `aerospike.tls.*` key
    * is accepted — a stray TLS key must never silently flip the wire. */
  val TlsEnabledKey               = "aerospike.tls.enabled"
  /** Cluster-wide TLS name applied to seeds that carry no `host:tlsName[:port]`
    * name of their own (the seed-level name always wins). */
  val TlsNameKey                  = "aerospike.tls.name"
  /** Comma-separated TLS protocol allowlist (client default: TLSv1.2). */
  val TlsProtocolsKey             = "aerospike.tls.protocols"
  /** Comma-separated cipher allowlist (client default: JVM ciphers). */
  val TlsCiphersKey               = "aerospike.tls.ciphers"
  /** JKS/PKCS12 truststore file with the CA certificate(s). Absent = default
    * JVM `SSLContext` (system truststore / `-Djavax.net.ssl.trustStore`). */
  val TlsTruststoreFileKey        = "aerospike.tls.truststore.file"
  /** File containing the truststore password. */
  val TlsTruststorePasswordFileKey = "aerospike.tls.truststore.password.file"
  /** `JKS` or `PKCS12`. Default `JKS`. */
  val TlsTruststoreTypeKey        = "aerospike.tls.truststore.type"
  /** JKS/PKCS12 keystore with the client certificate chain + key (mTLS, PKI). */
  val TlsKeystoreFileKey          = "aerospike.tls.keystore.file"
  /** File containing the keystore password. */
  val TlsKeystorePasswordFileKey  = "aerospike.tls.keystore.password.file"
  /** File containing the key password; falls back to the keystore password. */
  val TlsKeystoreKeyPasswordFileKey = "aerospike.tls.keystore.key.password.file"
  /** `JKS` or `PKCS12`. Default `JKS`. */
  val TlsKeystoreTypeKey          = "aerospike.tls.keystore.type"
  /** TLS for login only: credentials over TLS, then cleartext data plane.
    * Requires credentials; NOT transport encryption. */
  val TlsForLoginOnlyKey          = "aerospike.tls.forLoginOnly"
  /** `internal` (default) | `external` | `external_insecure` | `pki`. */
  val AuthModeKey                 = "aerospike.auth.mode"
  /** Authentication user (internal/external modes; also asinfo users). */
  val UserKey                     = "aerospike.user"
  /** Removed: inline passwords were serialised into Spark task closures and
    * visible wherever `userConf` is logged. Setting it fails at parse. */
  val RemovedPasswordKey          = "aerospike.password"
  /** File containing the password, read on the JVM that builds the client. */
  val PasswordFileKey             = "aerospike.password.file"
  /** NAME of an environment variable holding the password, read on the JVM
    * that builds the client (a Spark executor needs it in its own env). */
  val PasswordEnvKey              = "aerospike.password.env"

  private val TlsKeys = Set(
    TlsNameKey, TlsProtocolsKey, TlsCiphersKey,
    TlsTruststoreFileKey, TlsTruststorePasswordFileKey, TlsTruststoreTypeKey,
    TlsKeystoreFileKey, TlsKeystorePasswordFileKey, TlsKeystoreKeyPasswordFileKey,
    TlsKeystoreTypeKey, TlsForLoginOnlyKey)

  private val StoreTypes = Set("JKS", "PKCS12")

  val DefaultPort                       = 3000
  val DefaultTtlSeconds: Long           = 5L * 24 * 3600 // 5 days
  val DefaultMaxConnsPerNode            = 100
  // Pre-opened pool floor; avoids a cold-start connect stampede under bursty
  // load. Set to 0 to restore lazy pool growth.
  val DefaultMinConnsPerNode            = 16
  val DefaultConnectTimeoutMs           = 1000
  // Below both per-class totals so a stalled attempt leaves room for retries.
  val DefaultSocketTimeoutMs            = 250
  val DefaultReadTotalTimeoutMs         = 2000
  // A full-history stats read moves ~184 MB per 5-key fetch at 30 days of hourly 50 KiB entries and takes
  // about 1 s per command (baseline-10b); the total must cover the whole round, not one chunk.
  val DefaultStatsReadTotalTimeoutMs    = 10000
  val DefaultWriteTotalTimeoutMs        = 1000
  val DefaultMaxRetriesRead             = 2
  val DefaultMaxRetriesWrite            = 2
  val DefaultStreamingMaxLookbackDays   = 5 // capped by default 5-day TTL
  val DefaultLoaderBatchTimeoutSeconds  = 60
  // Per-executor bulk-load sizing: many executors share the cluster, so each
  // gets a small async cap and no pre-opened floor. Applied by [[forLoader]]
  // and used as the loader CLI defaults.
  val DefaultLoaderMaxConnsPerNode      = 32
  val DefaultLoaderMinConnsPerNode      = 0
  val LoaderSocketTimeoutMs             = 5000
  val LoaderReadTotalTimeoutMs          = 30000 // loader does not read; keeps socket < total under validation
  val LoaderWriteTotalTimeoutMs         = 30000
  val LoaderMaxRetriesRead              = 0
  val LoaderMaxRetriesWrite             = 2
  // 2 covers a same-day rerun and out-of-order arrival of an older upload
  val DefaultRetainVersions             = 2
  // sized from the measured entry bytes (benchmark/benchmark-reports/stats-entry-size.md)
  val DefaultStatsSegmentTargetKiB      = 512
  // settled by the hot-key gate (baseline-3/4 seg sweeps + the 152 diagnosis:
  // seg64 collapses under heavy reads)
  val DefaultSegmentMaxEntries          = 256
  val DefaultMaxSegmentsPerDay          = 1024
  // under the pre-6.4 batch-max-requests default of 5000 per node sub-batch
  val DefaultReadBatchChunkSize         = 1000

  private val MillisPerDay = 86400000L

  /** Parses `userConf` into a validated [[AerospikeConfig]].
    *
    * Required keys: [[HostsKey]] (comma-separated, at least one host) and
    * [[NamespaceKey]]. Everything else has a `Default*` constant.
    *
    * Throws `IllegalArgumentException` on missing required keys, non-numeric or
    * non-positive numbers, and consistency violations:
    * `streamingMaxLookbackDays` beyond `ttlSeconds`, `minConnsPerNode >
    * maxConnsPerNode`, `maxCommandsInProcess < 5`. Every message names the
    * offending key so operators can fix config quickly.
    */
  def fromUserConf(userConf: Map[String, String]): AerospikeConfig = {
    val rawHosts = userConf.getOrElse(HostsKey, throw new IllegalArgumentException(
      s"Missing required config key: $HostsKey (e.g. 'localhost' or 'host1,host2')"))
    if (rawHosts.split(',').map(_.trim).forall(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Config key '$HostsKey' must contain at least one non-empty host, got: '$rawHosts'")
    }

    def parseInt(key: String, default: Int): Int =
      userConf.get(key) match {
        case Some(v) =>
          val trimmed = v.trim
          try {
            trimmed.toInt
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"Config key '$key' must be numeric, got: '$v'")
          }
        case None => default
      }

    def parsePositiveInt(key: String, default: Int): Int = {
      val value = parseInt(key, default)
      if (value <= 0) {
        throw new IllegalArgumentException(s"Config key '$key' must be positive, got: $value")
      }
      value
    }

    def parseNonNegativeInt(key: String, default: Int): Int = {
      val value = parseInt(key, default)
      if (value < 0) {
        throw new IllegalArgumentException(s"Config key '$key' must be non-negative, got: $value")
      }
      value
    }

    def parseLong(key: String, default: Long): Long =
      userConf.get(key) match {
        case Some(v) =>
          val trimmed = v.trim
          try {
            trimmed.toLong
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"Config key '$key' must be numeric, got: '$v'")
          }
        case None => default
      }

    val namespace = userConf.getOrElse(NamespaceKey, throw new IllegalArgumentException(
      s"Missing required config key: $NamespaceKey"))

    def parseBoolean(key: String): Option[Boolean] =
      userConf.get(key).map { v =>
        v.trim.toLowerCase match {
          case "true"  => true
          case "false" => false
          case _ => throw new IllegalArgumentException(
            s"Config key '$key' must be 'true' or 'false', got: '$v'")
        }
      }

    def parseCsv(key: String): Option[Seq[String]] =
      userConf.get(key).map { v =>
        val items = v.split(',').map(_.trim).filter(_.nonEmpty)
        if (items.isEmpty) {
          throw new IllegalArgumentException(s"Config key '$key' must list at least one value, got: '$v'")
        }
        items.toSeq
      }

    def storeType(key: String, default: String = "JKS"): String =
      userConf.get(key).map(_.trim.toUpperCase) match {
        case Some(t) if StoreTypes.contains(t) => t
        case Some(t) => throw new IllegalArgumentException(
          s"Config key '$key' must be ${StoreTypes.mkString(" or ")}, got: '$t'")
        case None => default
      }

    val port = parsePositiveInt(PortKey, DefaultPort)

    // Seeds use the client's native host[:tlsName][:port] syntax; `port` fills
    // entries that omit it.
    val hosts: Seq[Seed] =
      try {
        Host.parseHosts(
          rawHosts.split(',').map(_.trim).filter(_.nonEmpty).mkString(","), port)
          .map(h => Seed(h.name, Option(h.tlsName), h.port)).toSeq
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Config key '$HostsKey' has an invalid seed ('$rawHosts'): ${e.getMessage}")
      }

    val tlsEnabled = parseBoolean(TlsEnabledKey).getOrElse(false)
    val tlsKeysSet = TlsKeys.filter(userConf.contains)
    if (!tlsEnabled && tlsKeysSet.nonEmpty) {
      throw new IllegalArgumentException(
        s"TLS keys ${tlsKeysSet.toSeq.sorted.mkString(", ")} require '$TlsEnabledKey'='true'")
    }
    val tls: Option[TlsSettings] =
      if (!tlsEnabled) None
      else Some(TlsSettings(
        tlsName = userConf.get(TlsNameKey).map(_.trim).filter(_.nonEmpty),
        protocols = parseCsv(TlsProtocolsKey),
        ciphers = parseCsv(TlsCiphersKey),
        truststoreFile = userConf.get(TlsTruststoreFileKey).map(_.trim).filter(_.nonEmpty),
        truststorePasswordFile = userConf.get(TlsTruststorePasswordFileKey).map(_.trim).filter(_.nonEmpty),
        truststoreType = storeType(TlsTruststoreTypeKey),
        keystoreFile = userConf.get(TlsKeystoreFileKey).map(_.trim).filter(_.nonEmpty),
        keystorePasswordFile = userConf.get(TlsKeystorePasswordFileKey).map(_.trim).filter(_.nonEmpty),
        keystoreKeyPasswordFile = userConf.get(TlsKeystoreKeyPasswordFileKey).map(_.trim).filter(_.nonEmpty),
        keystoreType = storeType(TlsKeystoreTypeKey),
        forLoginOnly = parseBoolean(TlsForLoginOnlyKey).getOrElse(false)))

    tls.foreach { t =>
      if (t.keystoreFile.isDefined && t.keystorePasswordFile.isEmpty) {
        throw new IllegalArgumentException(
          s"Config key '$TlsKeystoreFileKey' requires '$TlsKeystorePasswordFileKey'")
      }
      if (t.keystoreFile.isEmpty &&
          (t.keystorePasswordFile.isDefined || t.keystoreKeyPasswordFile.isDefined)) {
        throw new IllegalArgumentException(
          s"Keystore password keys require '$TlsKeystoreFileKey'")
      }
      if (t.truststoreFile.isEmpty && t.truststorePasswordFile.isDefined) {
        throw new IllegalArgumentException(
          s"Config key '$TlsTruststorePasswordFileKey' requires '$TlsTruststoreFileKey'")
      }
    }
    if (!tlsEnabled && hosts.exists(_.tlsName.isDefined)) {
      throw new IllegalArgumentException(
        s"Config key '$HostsKey' carries a per-seed TLS name but '$TlsEnabledKey' is not 'true'")
    }

    val authMode = userConf.get(AuthModeKey).map(AuthModeSetting.parse(AuthModeKey, _))
      .getOrElse(AuthModeSetting.Internal)
    val user = userConf.get(UserKey).map(_.trim).filter(_.nonEmpty)
    if (userConf.contains(RemovedPasswordKey)) {
      throw new IllegalArgumentException(
        s"Config key '$RemovedPasswordKey' is no longer supported (inline passwords are " +
          s"serialised to executors and visible in logs); use '$PasswordFileKey' or '$PasswordEnvKey'")
    }
    val passwordFile = userConf.get(PasswordFileKey).map(_.trim).filter(_.nonEmpty)
    val passwordEnv = userConf.get(PasswordEnvKey).map(_.trim).filter(_.nonEmpty)
    if (passwordFile.isDefined && passwordEnv.isDefined) {
      throw new IllegalArgumentException(
        s"Config keys '$PasswordFileKey' and '$PasswordEnvKey' are mutually exclusive")
    }
    if (user.isDefined != (passwordFile.isDefined || passwordEnv.isDefined)) {
      throw new IllegalArgumentException(
        s"Config keys '$UserKey' and '$PasswordFileKey'/'$PasswordEnvKey' must be set together")
    }
    val credentials = user.map(u => Credentials(u, passwordFile, passwordEnv))

    authMode match {
      case AuthModeSetting.Pki =>
        if (tls.isEmpty || tls.get.keystoreFile.isEmpty) {
          throw new IllegalArgumentException(
            s"Config key '$AuthModeKey'='pki' requires '$TlsEnabledKey' and '$TlsKeystoreFileKey'")
        }
        if (credentials.isDefined) {
          throw new IllegalArgumentException(
            s"Config key '$AuthModeKey'='pki' authenticates by certificate; " +
              s"'$UserKey'/'$PasswordFileKey'/'$PasswordEnvKey' must not be set")
        }
      case AuthModeSetting.External =>
        if (tls.isEmpty) {
          throw new IllegalArgumentException(
            s"Config key '$AuthModeKey'='external' requires '$TlsEnabledKey'")
        }
        if (credentials.isEmpty) {
          throw new IllegalArgumentException(
            s"Config key '$AuthModeKey'='external' requires '$UserKey' and a password")
        }
      case AuthModeSetting.ExternalInsecure =>
        if (credentials.isEmpty) {
          throw new IllegalArgumentException(
            s"Config key '$AuthModeKey'='external_insecure' requires '$UserKey' and a password")
        }
      case AuthModeSetting.Internal => ()
    }
    if (tls.exists(_.forLoginOnly) && credentials.isEmpty) {
      throw new IllegalArgumentException(
        s"Config key '$TlsForLoginOnlyKey' requires '$UserKey' and a password " +
          "(login-only TLS only protects the credential exchange)")
    }

    def parseTtl(key: String): Option[TtlSetting] =
      userConf.get(key).map { v =>
        val trimmed = v.trim
        if (trimmed.equalsIgnoreCase("namespace")) TtlSetting.NamespaceDefault
        else {
          val seconds = try {
            trimmed.toLong
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(
                s"Config key '$key' must be a positive number of seconds, 0 (never expire) or 'namespace', got: '$v'")
          }
          if (seconds < 0) {
            throw new IllegalArgumentException(
              s"Config key '$key' must be a positive number of seconds, 0 (never expire) or 'namespace', got: $seconds")
          }
          if (seconds > Int.MaxValue) {
            throw new IllegalArgumentException(
              s"Config key '$key' exceeds Aerospike expiration limit (${Int.MaxValue} seconds): $seconds")
          }
          if (seconds == 0) TtlSetting.NeverExpire else TtlSetting.Seconds(seconds.toInt)
        }
      }

    val baseTtl = parseTtl(TtlSecondsKey).getOrElse(TtlSetting.Seconds(DefaultTtlSeconds.toInt))
    val batchTtl = parseTtl(TtlBatchSecondsKey).getOrElse(baseTtl)
    val streamingTtl = parseTtl(TtlStreamingSecondsKey).getOrElse(baseTtl)

    val streamingMaxLookbackDays = parseInt(StreamingMaxLookbackDaysKey, DefaultStreamingMaxLookbackDays)
    if (streamingMaxLookbackDays < 0) {
      throw new IllegalArgumentException(
        s"Config key '$StreamingMaxLookbackDaysKey' must be non-negative, got: $streamingMaxLookbackDays")
    }
    val maxLookbackMillis = streamingMaxLookbackDays.toLong * MillisPerDay
    streamingTtl match {
      case TtlSetting.Seconds(seconds) if maxLookbackMillis > seconds * 1000L =>
        throw new IllegalArgumentException(
          s"Config key '$StreamingMaxLookbackDaysKey' ($streamingMaxLookbackDays days) exceeds effective " +
            s"lookback allowed by '${if (userConf.contains(TtlStreamingSecondsKey)) TtlStreamingSecondsKey else TtlSecondsKey}' " +
            s"(${seconds}s). Set max lookback <= TTL or increase TTL.")
      case _ => // never-expire or namespace (resolved against the server at client build)
    }

    val loaderBatchTimeoutSeconds = parsePositiveInt(LoaderBatchTimeoutSecondsKey, DefaultLoaderBatchTimeoutSeconds)

    val maxCommandsInProcess = userConf.get(MaxCommandsInProcessKey).map(_ => parseInt(MaxCommandsInProcessKey, 0))
    maxCommandsInProcess.filter(_ < MinCommandsInProcess).foreach { v =>
      throw new IllegalArgumentException(
        s"Config key '$MaxCommandsInProcessKey' must be at least $MinCommandsInProcess (the client minimum), got: $v")
    }

    val maxConnsPerNode = parsePositiveInt(MaxConnsPerNodeKey, DefaultMaxConnsPerNode)
    val minConnsPerNode = parseInt(MinConnsPerNodeKey, DefaultMinConnsPerNode)
    if (minConnsPerNode < 0) {
      throw new IllegalArgumentException(
        s"Config key '$MinConnsPerNodeKey' must be non-negative, got: $minConnsPerNode")
    }
    if (minConnsPerNode > maxConnsPerNode) {
      throw new IllegalArgumentException(
        s"Config key '$MinConnsPerNodeKey' ($minConnsPerNode) exceeds '$MaxConnsPerNodeKey' ($maxConnsPerNode)")
    }

    val loaderMaxConnsPerNode = parsePositiveInt(LoaderMaxConnsPerNodeKey, DefaultLoaderMaxConnsPerNode)
    val loaderMinConnsPerNode = parseNonNegativeInt(LoaderMinConnsPerNodeKey, DefaultLoaderMinConnsPerNode)
    if (loaderMinConnsPerNode > loaderMaxConnsPerNode) {
      throw new IllegalArgumentException(
        s"Config key '$LoaderMinConnsPerNodeKey' ($loaderMinConnsPerNode) exceeds '$LoaderMaxConnsPerNodeKey' ($loaderMaxConnsPerNode)")
    }

    AerospikeConfig(
      hosts = hosts,
      port = port,
      namespace = namespace,
      batchTtl = batchTtl,
      streamingTtl = streamingTtl,
      maxConnsPerNode = maxConnsPerNode,
      minConnsPerNode = minConnsPerNode,
      connectTimeoutMillis = parsePositiveInt(ConnectTimeoutKey, DefaultConnectTimeoutMs),
      socketTimeoutMillis = parsePositiveInt(SocketTimeoutKey, DefaultSocketTimeoutMs),
      readTotalTimeoutMillis = parsePositiveInt(ReadTotalTimeoutKey, DefaultReadTotalTimeoutMs),
      writeTotalTimeoutMillis = parsePositiveInt(WriteTotalTimeoutKey, DefaultWriteTotalTimeoutMs),
      statsReadTotalTimeoutMillis = parsePositiveInt(StatsReadTotalTimeoutKey, DefaultStatsReadTotalTimeoutMs),
      maxRetriesRead = parseNonNegativeInt(MaxRetriesReadKey, DefaultMaxRetriesRead),
      maxRetriesWrite = parseNonNegativeInt(MaxRetriesWriteKey, DefaultMaxRetriesWrite),
      keyPrefix = userConf.get(KeyPrefixKey).filter(_.nonEmpty),
      streamingMaxLookbackDays = streamingMaxLookbackDays,
      loaderBatchTimeoutSeconds = loaderBatchTimeoutSeconds,
      maxCommandsInProcess = maxCommandsInProcess,
      retainVersions = parsePositiveInt(RetainVersionsKey, DefaultRetainVersions),
      statsSegmentTargetKiB = parsePositiveInt(StatsSegmentTargetKiBKey, DefaultStatsSegmentTargetKiB),
      segmentMaxEntries = parsePositiveInt(SegmentMaxEntriesKey, DefaultSegmentMaxEntries),
      maxSegmentsPerDay = parsePositiveInt(MaxSegmentsPerDayKey, DefaultMaxSegmentsPerDay),
      readBatchChunkSize = parsePositiveInt(ReadBatchChunkSizeKey, DefaultReadBatchChunkSize),
      loaderMaxConnsPerNode = loaderMaxConnsPerNode,
      loaderMinConnsPerNode = loaderMinConnsPerNode,
      tls = tls,
      authMode = authMode,
      credentials = credentials
    )
  }

  /** The config a bulk load should run with: pool floor/cap from the
    * `aerospike.loader.*` keys (many executors share the cluster), the loader's
    * bulk timeouts and retries, and the in-flight cap cleared so it falls back
    * to `maxConnsPerNode`. Used by `AerospikeKvStore.bulkPut`; the loader CLI
    * builds its config from its own flags instead. */
  def forLoader(config: AerospikeConfig): AerospikeConfig =
    config.copy(
      maxConnsPerNode = config.loaderMaxConnsPerNode,
      minConnsPerNode = config.loaderMinConnsPerNode,
      socketTimeoutMillis = LoaderSocketTimeoutMs,
      readTotalTimeoutMillis = LoaderReadTotalTimeoutMs,
      writeTotalTimeoutMillis = LoaderWriteTotalTimeoutMs,
      maxRetriesRead = LoaderMaxRetriesRead,
      maxRetriesWrite = LoaderMaxRetriesWrite,
      maxCommandsInProcess = None)
}
