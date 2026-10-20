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

/** Parsed and validated connection/tuning settings for the store, built by
  * [[AerospikeConfig.fromUserConf]] from Chronon's `userConf` map.
  *
  * Only some fields shape the shared client — hosts, port, pool bounds,
  * `maxCommandsInProcess`, and the three timeouts form
  * `AerospikeClientHolder`'s cache key; namespace, TTL, key prefix, lookback,
  * and loader timeout are per-store and never create a second client.
  *
  * @param hosts                     cluster seed hosts (required, non-empty)
  * @param port                      service port (default 3000)
  * @param namespace                 target namespace (required)
  * @param ttlSeconds                record TTL for feature records; must be positive
  *                                  and `<= Int.MaxValue` (default 5 days). Also caps
  *                                  effective streaming read lookback.
  * @param maxConnsPerNode           async+sync connection pool cap per node (default 300)
  * @param minConnsPerNode           pre-opened pool floor per node, `<= maxConnsPerNode`
  *                                  (default 16; 0 restores lazy growth)
  * @param connectTimeoutMillis      cluster-tend/seed connect timeout AND
  *                                  per-policy connect timeout (default 1000)
  * @param socketTimeoutMillis       per-command socket timeout (default 30000)
  * @param totalTimeoutMillis        per-command total timeout (default 30000)
  * @param keyPrefix                 optional prefix prepended to every record key
  * @param streamingMaxLookbackDays  max day-buckets read per streaming `multiGet`;
  *                                  must satisfy `days * 86400s <= ttlSeconds`
  * @param loaderBatchTimeoutSeconds per-chunk `multiPut` await in the Spark loader
  *                                  (default 60)
  * @param maxCommandsInProcess      total in-flight async command cap across event
  *                                  loops; minimum [[AerospikeConfig.MinCommandsInProcess]],
  *                                  defaults to `maxConnsPerNode` when unset
  */
final case class AerospikeConfig(hosts: Seq[String],
                                 port: Int,
                                 namespace: String,
                                 ttlSeconds: Long,
                                 maxConnsPerNode: Int,
                                 minConnsPerNode: Int,
                                 connectTimeoutMillis: Int,
                                 socketTimeoutMillis: Int,
                                 totalTimeoutMillis: Int,
                                 keyPrefix: Option[String],
                                 streamingMaxLookbackDays: Int,
                                 loaderBatchTimeoutSeconds: Int,
                                 maxCommandsInProcess: Option[Int] = None)

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
  /** Record TTL for feature records; also caps streaming lookback. Default [[DefaultTtlSeconds]] (5 days). */
  val TtlSecondsKey               = "aerospike.ttl.seconds"
  /** Async+sync connection pool cap per node. Default [[DefaultMaxConnsPerNode]]. */
  val MaxConnsPerNodeKey          = "aerospike.maxConnsPerNode"
  /** Pre-opened pool floor per node, `<= maxConnsPerNode`. Default [[DefaultMinConnsPerNode]]. */
  val MinConnsPerNodeKey          = "aerospike.minConnsPerNode"
  /** Cluster-tend/seed connect timeout and per-policy connect timeout. Default [[DefaultConnectTimeoutMs]]. */
  val ConnectTimeoutKey           = "aerospike.timeout.connect.millis"
  /** Per-command socket timeout. Default [[DefaultSocketTimeoutMs]]. */
  val SocketTimeoutKey            = "aerospike.timeout.socket.millis"
  /** Per-command total timeout. Default [[DefaultTotalTimeoutMs]]. */
  val TotalTimeoutKey             = "aerospike.timeout.total.millis"
  /** Optional prefix prepended to every record key. */
  val KeyPrefixKey                = "aerospike.key.prefix"
  /** Max day-buckets read per streaming `multiGet`; must not exceed the TTL. Default [[DefaultStreamingMaxLookbackDays]]. */
  val StreamingMaxLookbackDaysKey = "aerospike.streaming.maxLookbackDays"
  /** Per-chunk `multiPut` await in the Spark loader. Default [[DefaultLoaderBatchTimeoutSeconds]]. */
  val LoaderBatchTimeoutSecondsKey = "aerospike.loader.batch.timeout.seconds"

  val DefaultPort                       = 3000
  val DefaultTtlSeconds: Long           = 5L * 24 * 3600 // 5 days
  val DefaultMaxConnsPerNode            = 300
  // Pre-opened pool floor; avoids a cold-start connect stampede under bursty
  // load. Set to 0 to restore lazy pool growth.
  val DefaultMinConnsPerNode            = 16
  val DefaultConnectTimeoutMs           = 1000
  val DefaultSocketTimeoutMs            = 30000
  val DefaultTotalTimeoutMs             = 30000
  val DefaultStreamingMaxLookbackDays   = 5 // capped by default 5-day TTL
  val DefaultLoaderBatchTimeoutSeconds  = 60

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
    val hosts = rawHosts.split(',').map(_.trim).filter(_.nonEmpty).toSeq
    if (hosts.isEmpty) {
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

    val ttlSeconds = parseLong(TtlSecondsKey, DefaultTtlSeconds)
    if (ttlSeconds <= 0) {
      throw new IllegalArgumentException(
        s"Config key '$TtlSecondsKey' must be positive, got: $ttlSeconds")
    }
    if (ttlSeconds > Int.MaxValue) {
      throw new IllegalArgumentException(
        s"Config key '$TtlSecondsKey' exceeds Aerospike expiration limit (${Int.MaxValue} seconds): $ttlSeconds")
    }

    val streamingMaxLookbackDays = parseInt(StreamingMaxLookbackDaysKey, DefaultStreamingMaxLookbackDays)
    if (streamingMaxLookbackDays < 0) {
      throw new IllegalArgumentException(
        s"Config key '$StreamingMaxLookbackDaysKey' must be non-negative, got: $streamingMaxLookbackDays")
    }
    val maxLookbackMillis = streamingMaxLookbackDays.toLong * MillisPerDay
    val ttlMillis = ttlSeconds * 1000L
    if (maxLookbackMillis > ttlMillis) {
      throw new IllegalArgumentException(
        s"Config key '$StreamingMaxLookbackDaysKey' ($streamingMaxLookbackDays days) exceeds effective " +
          s"lookback allowed by '$TtlSecondsKey' (${ttlSeconds}s). Set max lookback <= TTL or increase TTL.")
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

    AerospikeConfig(
      hosts = hosts,
      port = parsePositiveInt(PortKey, DefaultPort),
      namespace = namespace,
      ttlSeconds = ttlSeconds,
      maxConnsPerNode = maxConnsPerNode,
      minConnsPerNode = minConnsPerNode,
      connectTimeoutMillis = parsePositiveInt(ConnectTimeoutKey, DefaultConnectTimeoutMs),
      socketTimeoutMillis = parsePositiveInt(SocketTimeoutKey, DefaultSocketTimeoutMs),
      totalTimeoutMillis = parsePositiveInt(TotalTimeoutKey, DefaultTotalTimeoutMs),
      keyPrefix = userConf.get(KeyPrefixKey).filter(_.nonEmpty),
      streamingMaxLookbackDays = streamingMaxLookbackDays,
      loaderBatchTimeoutSeconds = loaderBatchTimeoutSeconds,
      maxCommandsInProcess = maxCommandsInProcess
    )
  }
}
