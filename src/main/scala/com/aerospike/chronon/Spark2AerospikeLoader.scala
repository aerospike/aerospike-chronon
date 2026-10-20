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
import ai.chronon.online.KVStore.PutRequest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.rogach.scallop.ScallopConf

import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

/** Bulk-loads Chronon `_upload` Hive tables into the online store.
  *
  * Used two ways: as the `main` CLI entry point (run per group-by after
  * `metadata-upload`), and by `AerospikeKvStore.bulkPut` for in-app loads.
  * Both funnel into [[upload]].
  *
  * Source tables must expose `key_bytes` and `value_bytes` binary columns and a
  * `ds` partition column; an optional `ts` column overrides the record version
  * timestamp, which otherwise defaults to the partition's end-of-day
  * ([[versionTsMillis]]), matching what the fetcher uses as `batchEndTsMillis`.
  * Rows missing either binary column are dropped with a warning.
  *
  * Writes run inside `foreachPartition`: each executor constructs its own
  * [[AerospikeKvStore]] (sharing the JVM-wide client via
  * [[AerospikeClientHolder]]) and drains rows in `ChunkSize` (1000) chunks through
  * `multiPut`, awaiting each chunk for `loaderBatchTimeoutSeconds`. Any failed
  * write fails the task — retries rely on last-write-wins idempotency.
  */
object Spark2AerospikeLoader {

  /** Rows per `multiPut` call inside an executor. */
  private val ChunkSize = 1000
  private val logger: Logger = LoggerFactory.getLogger(Spark2AerospikeLoader.getClass)

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
    * delegate here. MUST NOT stop the injected session — the caller owns it. */
  def upload(spark: SparkSession,
             tableName: String,
             dataset: Option[String],
             endDs: String,
             config: AerospikeConfig): Unit = {
    val targetDataset = dataset.getOrElse(datasetForTable(tableName))
    if (!targetDataset.endsWith("_BATCH")) {
      throw new IllegalArgumentException(
        s"Loader dataset override must end with _BATCH, got: '$targetDataset'. " +
          s"Omit --dataset to infer the batch dataset from the table name.")
    }
    val partitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L)
    val defaultTs = versionTsMillis(partitionSpec, endDs)

    val df = spark.table(tableName).filter(col("ds") === lit(endDs))
    val hasTs = df.columns.map(_.toLowerCase()).contains("ts")

    df.foreachPartition { (rows: Iterator[Row]) =>
      if (rows.hasNext) {
        val store = new AerospikeKvStore(config)
        val batchTimeout = Duration(config.loaderBatchTimeoutSeconds, SECONDS)
        rows.grouped(ChunkSize).foreach { chunk =>
          val (requests, dropped) = chunk.map { row =>
            putRequestFromRow(row, targetDataset, hasTs, defaultTs)
          }.partition(_.isDefined) match {
            case (somes, nones) => (somes.flatten, nones.length)
          }
          if (dropped > 0) {
            logger.warn(s"Dropped $dropped rows from $tableName partition $endDs (missing or non-binary key_bytes/value_bytes)")
          }
          if (requests.nonEmpty) {
            val results = Await.result(store.multiPut(requests), batchTimeout)
            val failures = results.count(_ == false)
            if (failures > 0) {
              throw new RuntimeException(
                s"multiPut failed for $failures / ${results.size} rows in dataset $targetDataset partition $endDs")
            }
          }
        }
      }
    }
  }

  /** Builds a `PutRequest` from a row, or `None` when `key_bytes`/`value_bytes`
    * are absent or non-binary (dropped by the caller with a warning). `ts` is
    * taken from a numeric `ts` column when present, else `defaultTs`. */
  private def putRequestFromRow(row: Row,
                                dataset: String,
                                hasTs: Boolean,
                                defaultTs: Long): Option[PutRequest] = {
    val keyBytes = safeBytes(row, "key_bytes")
    val valueBytes = safeBytes(row, "value_bytes")
    if (keyBytes == null || valueBytes == null) {
      None
    } else {
      val ts = if (hasTs) row.getAs[Any]("ts") match {
        case l: Long    => l
        case i: Int     => i.toLong
        case n: Number  => n.longValue()
        case other      => defaultTs
      } else defaultTs
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
    val baseConf = Map(
      AerospikeConfig.HostsKey                       -> conf.aerospikeHosts(),
      AerospikeConfig.PortKey                        -> conf.aerospikePort.getOrElse("3000"),
      AerospikeConfig.NamespaceKey                   -> conf.namespace(),
      AerospikeConfig.TtlSecondsKey                  -> conf.ttl(),
      AerospikeConfig.MaxConnsPerNodeKey             -> conf.maxConnsPerNode.getOrElse(AerospikeConfig.DefaultMaxConnsPerNode.toString),
      AerospikeConfig.ConnectTimeoutKey              -> conf.connectTimeoutMillis.getOrElse(AerospikeConfig.DefaultConnectTimeoutMs.toString),
      AerospikeConfig.SocketTimeoutKey               -> conf.socketTimeoutMillis.getOrElse(AerospikeConfig.DefaultSocketTimeoutMs.toString),
      AerospikeConfig.TotalTimeoutKey                -> conf.totalTimeoutMillis.getOrElse(AerospikeConfig.DefaultTotalTimeoutMs.toString),
      AerospikeConfig.StreamingMaxLookbackDaysKey    -> conf.maxLookbackDays.getOrElse(AerospikeConfig.DefaultStreamingMaxLookbackDays.toString),
      AerospikeConfig.LoaderBatchTimeoutSecondsKey   -> conf.batchTimeoutSeconds.getOrElse(AerospikeConfig.DefaultLoaderBatchTimeoutSeconds.toString)
    )
    val withPrefix = conf.keyPrefix.toOption.fold(baseConf)(p => baseConf + (AerospikeConfig.KeyPrefixKey -> p))
    val config = AerospikeConfig.fromUserConf(
      conf.maxCommandsInProcess.toOption.fold(withPrefix)(v => withPrefix + (AerospikeConfig.MaxCommandsInProcessKey -> v))
    )

    val spark = SparkSession.builder()
      .appName("Spark2AerospikeLoader")
      .getOrCreate()

    upload(spark, conf.tableName(), conf.dataset.toOption, conf.endDs(), config)
  }

  private class LoaderConf(args: Seq[String]) extends ScallopConf(args) {
    val tableName = opt[String](required = true, name = "table-name", descr = "Source upload table")
    val dataset = opt[String](name = "dataset", descr = "Destination online dataset (optional, inferred from table name)")
    val endDs = opt[String](required = true, name = "end-ds", descr = "Partition date (yyyy-MM-dd)")
    val aerospikeHosts = opt[String](required = true, name = "aerospike-hosts", descr = "Comma-separated Aerospike hosts")
    val aerospikePort = opt[String](name = "aerospike-port", descr = "Aerospike port", default = Some("3000"))
    val namespace = opt[String](required = true, name = "namespace", descr = "Aerospike namespace")
    val ttl = opt[String](required = true, name = "ttl", descr = "Record TTL in seconds")
    val maxConnsPerNode = opt[String](name = "max-conns-per-node", descr = "Connection pool cap per node", default = Some(AerospikeConfig.DefaultMaxConnsPerNode.toString))
    val maxCommandsInProcess = opt[String](name = "max-commands-in-process", descr = "Total in-flight async command cap (default: max-conns-per-node, minimum 5)")
    val connectTimeoutMillis = opt[String](name = "connect-timeout-millis", descr = "Connection timeout in milliseconds", default = Some(AerospikeConfig.DefaultConnectTimeoutMs.toString))
    val socketTimeoutMillis = opt[String](name = "socket-timeout-millis", descr = "Socket timeout in milliseconds", default = Some(AerospikeConfig.DefaultSocketTimeoutMs.toString))
    val totalTimeoutMillis = opt[String](name = "total-timeout-millis", descr = "Total operation timeout in milliseconds", default = Some(AerospikeConfig.DefaultTotalTimeoutMs.toString))
    val keyPrefix = opt[String](name = "key-prefix", descr = "Optional prefix prepended to all record keys")
    val maxLookbackDays = opt[String](name = "max-lookback-days", descr = "Streaming read max lookback in days", default = Some(AerospikeConfig.DefaultStreamingMaxLookbackDays.toString))
    val batchTimeoutSeconds = opt[String](name = "batch-timeout-seconds", descr = "Timeout per multiPut batch in seconds", default = Some(AerospikeConfig.DefaultLoaderBatchTimeoutSeconds.toString))
    verify()
  }
}
