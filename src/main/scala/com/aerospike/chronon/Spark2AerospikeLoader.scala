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

import ai.chronon.api.{Constants => ApiConstants, PartitionSpec}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.rogach.scallop.ScallopConf

import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}

/** Bulk-loads Chronon `_upload` Hive tables into the online store.
  *
  * Used two ways: as the `main` CLI entry point (run per group-by after
  * `metadata-upload`), and by `AerospikeKvStore.bulkPut` for in-app loads.
  * Both funnel into [[upload]].
  *
  * Source tables must expose `key_bytes` and `value_bytes` binary columns and a
  * partition column — `ds` by default, or whatever
  * `spark.chronon.partition.column` (with `spark.chronon.partition.format`)
  * configures, the same Spark conf keys Chronon's `TableUtils` reads; set them
  * with `--conf` on `spark-submit`, not loader flags. An optional `ts` column
  * overrides the record version timestamp, which otherwise defaults to the
  * partition's end-of-day ([[versionTsMillis]]), matching what the fetcher uses
  * as `batchEndTsMillis`. Rows missing either binary column are dropped with a
  * warning.
  *
  * Writes run inside `foreachPartition`: each executor constructs its own
  * [[AerospikeKvStore]] (sharing the JVM-wide client via
  * [[AerospikeClientHolder]]) and drains rows in `ChunkSize` (1000) chunks through
  * `multiPut`, awaiting each chunk for `loaderBatchTimeoutSeconds`. Any failed
  * write fails the task — retries rely on last-write-wins idempotency.
  */
object Spark2AerospikeLoader {

  /** Outcome of one [[upload]] call. `rowsRead` counts rows in the selected
    * partition, `rowsWritten` rows handed to successful `multiPut` chunks and
    * `rowsDropped` rows skipped for a missing/non-binary `key_bytes` or
    * `value_bytes`. Counts come from Spark accumulators and are best-effort
    * under task retries (a retried task re-adds its rows). */
  case class LoadSummary(rowsRead: Long, rowsWritten: Long, rowsDropped: Long)

  /** Spark conf keys Chronon itself uses for the partition column/format
    * (see `TableUtils`); read from the session, not from loader flags. */
  val PartitionColumnConfKey = "spark.chronon.partition.column"
  val PartitionFormatConfKey = "spark.chronon.partition.format"
  val DefaultPartitionColumn = "ds"
  val DefaultPartitionFormat = "yyyy-MM-dd"

  /** Rows per `multiPut` call inside an executor. */
  private val ChunkSize = 1000
  private val logger: Logger = LoggerFactory.getLogger(Spark2AerospikeLoader.getClass)

  /** Test seam: builds the per-executor KVStore. Production keeps the default. */
  private[chronon] var kvStoreFactory: AerospikeConfig => KVStore = c => new AerospikeKvStore(c)

  /** Test seam: invoked with every driver-side WARN message in addition to
    * logging. */
  private[chronon] var warnHook: String => Unit = { _ => () }

  private def warn(message: String): Unit = {
    logger.warn(message)
    warnHook(message)
  }

  /** Infers the online dataset from an upload table name:
    * `<gb>_upload` -> `<GB>_BATCH`; `*_daily_stats_upload` ->
    * `CHRONON_STATS_BATCH`; `*_logged_daily_stats_upload` -> the logged-stats
    * dataset; `*_consistency_upload` -> the consistency-metrics dataset;
    * anything else -> `<TABLE>_BATCH`. Case-insensitive. */
  def datasetForTable(tableName: String): String = {
    val upper = tableName.toUpperCase
    if (upper.endsWith("_LOGGED_DAILY_STATS_UPLOAD")) {
      ApiConstants.LogStatsBatchDataset
    } else if (upper.endsWith("_DAILY_STATS_UPLOAD")) {
      ApiConstants.StatsBatchDataset
    } else if (upper.endsWith("_CONSISTENCY_UPLOAD")) {
      ApiConstants.ConsistencyMetricsDataset
    } else if (upper.endsWith("_UPLOAD")) {
      val basename = tableName.substring(tableName.lastIndexOf('.') + 1).toUpperCase.stripSuffix("_UPLOAD")
      s"${basename}_BATCH"
    } else {
      s"${tableName.toUpperCase}_BATCH"
    }
  }

  /** Version timestamp for a loaded partition: `epochMillis(ds) + spanMillis`
    * (end of the partition day), matching the fetcher's `batchEndTsMillis`. */
  def versionTsMillis(partitionSpec: PartitionSpec, ds: String): Long =
    partitionSpec.epochMillis(ds) + partitionSpec.spanMillis

  /** Core load path: reads `table` partition `endDs` and multiPuts rows into the
    * store via executor-local clients. Both `main` and `AerospikeKvStore.bulkPut`
    * delegate here. MUST NOT stop the injected session — the caller owns it.
    *
    * The partition column and format come from the session conf keys
    * [[PartitionColumnConfKey]] (default `ds`) and [[PartitionFormatConfKey]]
    * (default `yyyy-MM-dd`) — the same keys Chronon's `TableUtils` reads, so the
    * loader always speaks the table's convention. `endDs` must be in that exact
    * format: it is parsed and re-formatted, and any drift (a `2026-1-3` under
    * `yyyy-MM-dd`, an ISO date under `yyyyMMdd`) fails fast naming the value,
    * the format and the conf key.
    *
    * A partition with zero selected rows is an error — an empty `_upload`
    * partition almost always means the upstream upload job silently wrote
    * nothing. Pass `allowEmpty = true` (the CLI's `--allow-empty`) to WARN and
    * return instead. Returns a [[LoadSummary]] (best-effort under retries). */
  def upload(spark: SparkSession,
             tableName: String,
             dataset: Option[String],
             endDs: String,
             config: AerospikeConfig,
             allowEmpty: Boolean = false): LoadSummary = {
    val targetDataset = dataset.getOrElse(datasetForTable(tableName))
    if (!targetDataset.endsWith("_BATCH")) {
      throw new IllegalArgumentException(
        s"Loader dataset override must end with _BATCH, got: '$targetDataset'. " +
          s"Omit --dataset to infer the batch dataset from the table name.")
    }
    val partitionColumn = spark.conf.get(PartitionColumnConfKey, DefaultPartitionColumn)
    val partitionFormat = spark.conf.get(PartitionFormatConfKey, DefaultPartitionFormat)
    val partitionSpec = PartitionSpec(format = partitionFormat, spanMillis = 86400000L)
    val defaultTs = strictEndDs(partitionSpec, partitionFormat, endDs) + partitionSpec.spanMillis

    val df = spark.table(tableName)
    if (!df.columns.contains(partitionColumn)) {
      throw new IllegalArgumentException(
        s"Table $tableName has no partition column '$partitionColumn' " +
          s"(conf $PartitionColumnConfKey); columns: ${df.columns.mkString(", ")}")
    }
    val filtered = df.filter(col(partitionColumn) === lit(endDs))
    val tsColumn = filtered.columns.find(_.equalsIgnoreCase("ts"))

    val rowsRead = spark.sparkContext.longAccumulator("rowsRead")
    val rowsWritten = spark.sparkContext.longAccumulator("rowsWritten")
    val rowsDropped = spark.sparkContext.longAccumulator("rowsDropped")
    val servingInfoRows = spark.sparkContext.longAccumulator("servingInfoRows")

    filtered.foreachPartition { (rows: Iterator[Row]) =>
      if (rows.hasNext) {
        val store = kvStoreFactory(config)
        val batchTimeout = Duration(config.loaderBatchTimeoutSeconds, SECONDS)
        rows.grouped(ChunkSize).foreach { chunk =>
          rowsRead.add(chunk.size)
          servingInfoRows.add(chunk.count(row => isServingInfoKey(safeBytes(row, "key_bytes"))))
          val (requests, dropped) = chunk.map { row =>
            putRequestFromRow(row, targetDataset, tsColumn, defaultTs)
          }.partition(_.isDefined) match {
            case (somes, nones) => (somes.flatten, nones.length)
          }
          if (dropped > 0) {
            rowsDropped.add(dropped)
            warn(s"Dropped $dropped rows from $tableName partition $endDs (missing or non-binary key_bytes/value_bytes)")
          }
          if (requests.nonEmpty) {
            val results = Await.result(store.multiPut(requests), batchTimeout)
            val failures = results.count(_ == false)
            if (failures > 0) {
              throw new RuntimeException(
                s"multiPut failed for $failures / ${results.size} rows in dataset $targetDataset partition $endDs")
            }
            rowsWritten.add(requests.size)
          }
        }
      }
    }

    val summary = LoadSummary(rowsRead.value, rowsWritten.value, rowsDropped.value)
    if (summary.rowsRead == 0) {
      val message = s"Selected zero rows from $tableName partition column '$partitionColumn' = '$endDs'"
      if (!allowEmpty) {
        throw new IllegalStateException(
          s"$message. The upstream upload job probably wrote nothing; pass --allow-empty to tolerate an empty partition.")
      }
      warn(s"$message (--allow-empty set; continuing)")
    }
    if (isGroupByDataset(targetDataset) && servingInfoRows.value == 0 && summary.rowsRead > 0) {
      warn(
        s"Group-by upload $tableName partition $endDs carries no '${ApiConstants.GroupByServingInfoKey}' row; " +
          "fetches will degrade until metadata-upload produces one.")
    }
    logger.info(
      s"Loaded $tableName partition $partitionColumn=$endDs into $targetDataset: " +
        s"read=${summary.rowsRead} written=${summary.rowsWritten} dropped=${summary.rowsDropped}")
    summary
  }

  /** True for group-by batch datasets — the stats/consistency datasets do not
    * carry a `group_by_serving_info` row, so the warning does not apply. */
  private def isGroupByDataset(dataset: String): Boolean =
    dataset.endsWith("_BATCH") && !Set(
      ApiConstants.StatsBatchDataset,
      ApiConstants.LogStatsBatchDataset,
      ApiConstants.ConsistencyMetricsDataset).contains(dataset)

  private def isServingInfoKey(keyBytes: Array[Byte]): Boolean =
    keyBytes != null && new String(keyBytes, UTF_8) == ApiConstants.GroupByServingInfoKey

  /** Parses `endDs` strictly: parse, re-format, compare. Any drift fails with a
    * message naming the value, the format and its conf key. */
  private def strictEndDs(partitionSpec: PartitionSpec, format: String, endDs: String): Long = {
    val millis = Try(partitionSpec.epochMillis(endDs)) match {
      case Success(m) => m
      case Failure(e) =>
        throw new IllegalArgumentException(
          s"Cannot parse end-ds '$endDs' with partition format '$format' " +
            s"(conf $PartitionFormatConfKey): ${e.getMessage}")
    }
    if (partitionSpec.at(millis) != endDs) {
      throw new IllegalArgumentException(
        s"end-ds '$endDs' is not in partition format '$format' " +
          s"(conf $PartitionFormatConfKey)")
    }
    millis
  }

  /** Builds a `PutRequest` from a row, or `None` when `key_bytes`/`value_bytes`
    * are absent or non-binary (dropped by the caller with a warning). `ts` is
    * taken from the column named `tsColumn` (the actual case of the `ts`
    * column) when present, else `defaultTs`. */
  private def putRequestFromRow(row: Row,
                                dataset: String,
                                tsColumn: Option[String],
                                defaultTs: Long): Option[PutRequest] = {
    val keyBytes = safeBytes(row, "key_bytes")
    val valueBytes = safeBytes(row, "value_bytes")
    if (keyBytes == null || valueBytes == null) {
      None
    } else {
      val ts = tsColumn.map(row.getAs[Any](_)) match {
        case Some(l: Long)   => l
        case Some(i: Int)    => i.toLong
        case Some(n: Number) => n.longValue()
        case _               => defaultTs
      }
      Some(PutRequest(keyBytes, valueBytes, dataset, Some(ts)))
    }
  }

  /** Returns the column's bytes, or null when the column is absent from the
    * schema or not binary — letting the caller distinguish "no column" from
    * malformed rows. */
  private def safeBytes(row: Row, col: String): Array[Byte] = {
    if (row.schema.fieldNames.contains(col)) {
      row.getAs[Any](col) match {
        case bytes: Array[Byte] => bytes
        case _                  => null
      }
    } else null
  }

  /** CLI entry point; obtains a session via getOrCreate and MUST NOT stop a
    * session that was already active (embedded/test callers share it). */
  def main(args: Array[String]): Unit = {
    val conf = new LoaderConf(args)
    val config = AerospikeConfig.fromUserConf(userConfFrom(conf))

    val spark = SparkSession.builder()
      .appName("Spark2AerospikeLoader")
      .getOrCreate()

    upload(spark, conf.tableName(), conf.dataset.toOption, conf.endDs(), config, conf.allowEmpty())
  }

  /** The `userConf` map the loader builds from its CLI flags; bulk-oriented
    * defaults for the timeout/retry keys differ from the serving defaults. */
  private[chronon] def userConfFromArgs(args: Seq[String]): Map[String, String] =
    userConfFrom(new LoaderConf(args))

  private def userConfFrom(conf: LoaderConf): Map[String, String] = {
    val baseConf = Map(
      AerospikeConfig.HostsKey                       -> conf.aerospikeHosts(),
      AerospikeConfig.PortKey                        -> conf.aerospikePort.getOrElse("3000"),
      AerospikeConfig.NamespaceKey                   -> conf.namespace(),
      AerospikeConfig.TtlSecondsKey                  -> conf.ttl(),
      AerospikeConfig.MaxConnsPerNodeKey             -> conf.maxConnsPerNode.getOrElse(AerospikeConfig.DefaultLoaderMaxConnsPerNode.toString),
      AerospikeConfig.MinConnsPerNodeKey             -> conf.minConnsPerNode.getOrElse(AerospikeConfig.DefaultLoaderMinConnsPerNode.toString),
      AerospikeConfig.LoaderMaxConnsPerNodeKey       -> conf.maxConnsPerNode.getOrElse(AerospikeConfig.DefaultLoaderMaxConnsPerNode.toString),
      AerospikeConfig.LoaderMinConnsPerNodeKey       -> conf.minConnsPerNode.getOrElse(AerospikeConfig.DefaultLoaderMinConnsPerNode.toString),
      AerospikeConfig.ConnectTimeoutKey              -> conf.connectTimeoutMillis.getOrElse(AerospikeConfig.DefaultConnectTimeoutMs.toString),
      AerospikeConfig.SocketTimeoutKey               -> conf.socketTimeoutMillis.getOrElse(AerospikeConfig.LoaderSocketTimeoutMs.toString),
      AerospikeConfig.ReadTotalTimeoutKey            -> conf.readTotalTimeoutMillis.getOrElse(AerospikeConfig.LoaderReadTotalTimeoutMs.toString),
      AerospikeConfig.WriteTotalTimeoutKey           -> conf.writeTotalTimeoutMillis.getOrElse(AerospikeConfig.LoaderWriteTotalTimeoutMs.toString),
      AerospikeConfig.MaxRetriesReadKey              -> conf.maxRetriesRead.getOrElse(AerospikeConfig.LoaderMaxRetriesRead.toString),
      AerospikeConfig.MaxRetriesWriteKey             -> conf.maxRetriesWrite.getOrElse(AerospikeConfig.LoaderMaxRetriesWrite.toString),
      // the loader writes batch data only: no flag means lookback 0, not the
      // serving default 5 (which would reject a --ttl under 5 days)
      AerospikeConfig.StreamingMaxLookbackDaysKey    -> conf.maxLookbackDays.getOrElse("0"),
      AerospikeConfig.LoaderBatchTimeoutSecondsKey   -> conf.batchTimeoutSeconds.getOrElse(AerospikeConfig.DefaultLoaderBatchTimeoutSeconds.toString)
    )
    val withPrefix = conf.keyPrefix.toOption.fold(baseConf)(p => baseConf + (AerospikeConfig.KeyPrefixKey -> p))
    val withInflight = conf.maxCommandsInProcess.toOption.fold(withPrefix)(v => withPrefix + (AerospikeConfig.MaxCommandsInProcessKey -> v))
    // TLS/auth flags map 1:1 onto their userConf keys; unset flags omit the key.
    val flagKeys: Seq[(org.rogach.scallop.ScallopOption[String], String)] = Seq(
      conf.tlsEnabled               -> AerospikeConfig.TlsEnabledKey,
      conf.tlsName                  -> AerospikeConfig.TlsNameKey,
      conf.tlsProtocols             -> AerospikeConfig.TlsProtocolsKey,
      conf.tlsCiphers               -> AerospikeConfig.TlsCiphersKey,
      conf.tlsTruststoreFile        -> AerospikeConfig.TlsTruststoreFileKey,
      conf.tlsTruststorePasswordFile -> AerospikeConfig.TlsTruststorePasswordFileKey,
      conf.tlsTruststoreType        -> AerospikeConfig.TlsTruststoreTypeKey,
      conf.tlsKeystoreFile          -> AerospikeConfig.TlsKeystoreFileKey,
      conf.tlsKeystorePasswordFile  -> AerospikeConfig.TlsKeystorePasswordFileKey,
      conf.tlsKeystoreKeyPasswordFile -> AerospikeConfig.TlsKeystoreKeyPasswordFileKey,
      conf.tlsKeystoreType          -> AerospikeConfig.TlsKeystoreTypeKey,
      conf.tlsForLoginOnly          -> AerospikeConfig.TlsForLoginOnlyKey,
      conf.aerospikeUser            -> AerospikeConfig.UserKey,
      conf.aerospikePasswordEnv     -> AerospikeConfig.PasswordEnvKey,
      conf.aerospikePasswordFile    -> AerospikeConfig.PasswordFileKey,
      conf.authMode                 -> AerospikeConfig.AuthModeKey)
    flagKeys.foldLeft(withInflight) { case (m, (opt, key)) =>
      opt.toOption.fold(m)(v => m + (key -> v))
    }
  }

  private class LoaderConf(args: Seq[String]) extends ScallopConf(args) {
    val tableName = opt[String](required = true, name = "table-name", descr = "Source upload table")
    val dataset = opt[String](name = "dataset", descr = "Destination online dataset (optional, inferred from table name)")
    val endDs = opt[String](required = true, name = "end-ds", descr = "Partition date, in the session's spark.chronon.partition.format")
    val allowEmpty = opt[Boolean](name = "allow-empty", default = Some(false), descr = "Tolerate a partition with zero selected rows (WARN instead of failing)")
    val aerospikeHosts = opt[String](required = true, name = "aerospike-hosts", descr = "Comma-separated Aerospike hosts")
    val aerospikePort = opt[String](name = "aerospike-port", descr = "Aerospike port", default = Some("3000"))
    val namespace = opt[String](required = true, name = "namespace", descr = "Aerospike namespace")
    val ttl = opt[String](required = true, name = "ttl", descr = "Record TTL: seconds, 0 (never expire) or 'namespace'")
    val maxConnsPerNode = opt[String](name = "max-conns-per-node", descr = "Async connection pool cap per node per executor (bulk default 32)", default = Some(AerospikeConfig.DefaultLoaderMaxConnsPerNode.toString))
    val minConnsPerNode = opt[String](name = "min-conns-per-node", descr = "Async pre-opened pool floor per node per executor (bulk default 0)", default = Some(AerospikeConfig.DefaultLoaderMinConnsPerNode.toString))
    val maxCommandsInProcess = opt[String](name = "max-commands-in-process", descr = "Total in-flight async command cap (default: max-conns-per-node, minimum 5)")
    val connectTimeoutMillis = opt[String](name = "connect-timeout-millis", descr = "Connection timeout in milliseconds", default = Some(AerospikeConfig.DefaultConnectTimeoutMs.toString))
    val socketTimeoutMillis = opt[String](name = "socket-timeout-millis", descr = "Socket timeout in milliseconds (bulk default)", default = Some(AerospikeConfig.LoaderSocketTimeoutMs.toString))
    val readTotalTimeoutMillis = opt[String](name = "read-total-timeout-millis", descr = "Total read timeout in milliseconds (bulk default)", default = Some(AerospikeConfig.LoaderReadTotalTimeoutMs.toString))
    val writeTotalTimeoutMillis = opt[String](name = "write-total-timeout-millis", descr = "Total write timeout in milliseconds (bulk default)", default = Some(AerospikeConfig.LoaderWriteTotalTimeoutMs.toString))
    val maxRetriesRead = opt[String](name = "max-retries-read", descr = "Read retries (bulk default)", default = Some(AerospikeConfig.LoaderMaxRetriesRead.toString))
    val maxRetriesWrite = opt[String](name = "max-retries-write", descr = "Write retries (bulk default)", default = Some(AerospikeConfig.LoaderMaxRetriesWrite.toString))
    val keyPrefix = opt[String](name = "key-prefix", descr = "Optional prefix prepended to all record keys")
    val maxLookbackDays = opt[String](name = "max-lookback-days", descr = "Streaming read max lookback in days (default: 0 — the loader writes batch data only)")
    val batchTimeoutSeconds = opt[String](name = "batch-timeout-seconds", descr = "Timeout per multiPut batch in seconds", default = Some(AerospikeConfig.DefaultLoaderBatchTimeoutSeconds.toString))
    // TLS/auth mirror the aerospike.* userConf keys; store files must resolve
    // on every executor host.
    val tlsEnabled = opt[String](name = "tls-enabled", descr = "Enable TLS on all Aerospike connections (true/false)")
    val tlsName = opt[String](name = "tls-name", descr = "Cluster TLS name; seed 'host:tlsName[:port]' overrides")
    val tlsProtocols = opt[String](name = "tls-protocols", descr = "Comma-separated TLS protocol allowlist")
    val tlsCiphers = opt[String](name = "tls-ciphers", descr = "Comma-separated TLS cipher allowlist")
    val tlsTruststoreFile = opt[String](name = "tls-truststore-file", descr = "JKS/PKCS12 truststore with the CA cert (default: JVM truststore)")
    val tlsTruststorePasswordFile = opt[String](name = "tls-truststore-password-file", descr = "File containing the truststore password")
    val tlsTruststoreType = opt[String](name = "tls-truststore-type", descr = "JKS or PKCS12 (default JKS)")
    val tlsKeystoreFile = opt[String](name = "tls-keystore-file", descr = "JKS/PKCS12 keystore with client cert chain + key (mTLS/PKI)")
    val tlsKeystorePasswordFile = opt[String](name = "tls-keystore-password-file", descr = "File containing the keystore password")
    val tlsKeystoreKeyPasswordFile = opt[String](name = "tls-keystore-key-password-file", descr = "File containing the key password (default: keystore password)")
    val tlsKeystoreType = opt[String](name = "tls-keystore-type", descr = "JKS or PKCS12 (default JKS)")
    val tlsForLoginOnly = opt[String](name = "tls-for-login-only", descr = "TLS for login only — cleartext data plane (true/false)")
    val aerospikeUser = opt[String](name = "aerospike-user", descr = "Aerospike user")
    val aerospikePasswordEnv = opt[String](name = "aerospike-password-env", descr = "NAME of the environment variable holding the Aerospike password, read on each executor")
    val aerospikePasswordFile = opt[String](name = "aerospike-password-file", descr = "File containing the Aerospike password, read on each executor")
    val authMode = opt[String](name = "auth-mode", descr = "internal|external|external_insecure|pki (default internal)")
    verify()
  }
}
