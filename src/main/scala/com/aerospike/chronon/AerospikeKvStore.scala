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

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.{KVStore, Metrics}
import com.aerospike.client.{AerospikeException, BatchRead, Key, ResultCode}
import com.aerospike.client.cdt.{MapOperation, MapOrder, MapPolicy, MapReturnType, MapWriteFlags}
import com.aerospike.client.listener.{BatchListListener, RecordListener}
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.{Value, Operation}

import java.util.Arrays
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

/** `ai.chronon.online.KVStore` implementation backed by the Aerospike async
  * Java client.
  *
  * == Record layout ==
  *
  * Every record holds a single bin `"v"`: an Aerospike map ordered by key, of
  * `tsMillis -> valueBytes`. Each map entry is one version of the value.
  *
  *  - '''Writes''' use `MapOperation.put` against a KEY_ORDERED map policy;
  *    a write at an existing timestamp overwrites it (last-write-wins), so
  *    retries are idempotent.
  *  - '''Reads''' use `MapOperation.getByKeyRange` server-side, so only
  *    versions with `ts >= afterTsMillis` cross the wire.
  *  - '''Streaming''' datasets shard one entity into one record per UTC day
  *    (see [[AerospikeDataModel]]); a `multiGet` fans out into one `BatchRead`
  *    per day in the lookback window and merges the results.
  *
  * == Failure semantics ==
  *
  * `multiPut` never fails the batch for a single bad write: per-request results
  * are booleans in request order (`false` on Aerospike errors, launch
  * exceptions, or invalid requests such as null key/value bytes or an
  * over-63-byte set name). `multiGet` returns per-request
  * `GetResponse(Failure(ex))` on errors but maps `KEY_NOT_FOUND` to an empty
  * value list rather than a failure.
  *
  * == TTL ==
  *
  * Records expire after `config.ttlSeconds`, EXCEPT metadata datasets and the
  * `group_by_serving_info` row in batch sets, which are written with
  * `expiration = -1` (never expire) so a stalled batch upload degrades to
  * "stale features" rather than "every fetch throws". The namespace must run
  * `nsup-period > 0` for expiry to take effect.
  *
  * The underlying `AerospikeClient` is shared per JVM via
  * [[AerospikeClientHolder]]; constructing additional `AerospikeKvStore`
  * instances with equivalent configs does not open new connections.
  *
  * @param config parsed and validated connection/tuning settings
  */
class AerospikeKvStore(val config: AerospikeConfig) extends KVStore {

  import AerospikeDataModel._
  import AerospikeKvStore.WriteFailure

  private val client = AerospikeClientHolder.clientFor(config)

  /** Single bin holding the `tsMillis -> valueBytes` key-ordered map. */
  private val ValueBin: String = "v"

  /** KEY_ORDERED so server-side `getByKeyRange` on `afterTsMillis` is efficient
    * and results come back sorted by timestamp. */
  private val KeyOrderedPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT)

  /** Lazily-created Chronon metrics context per dataset name. */
  private val metricsByDataset = TrieMap.empty[String, Metrics.Context]

  /** Per-dataset metrics context (latency distributions, success/failure counters),
    * created on first use under the `GroupByUpload` environment with the dataset
    * name as suffix. */
  def metricsContextFor(dataset: String): Metrics.Context =
    metricsByDataset.getOrElseUpdate(dataset, Metrics.Context(Metrics.Environment.GroupByUpload).withSuffix(dataset))

  /** No-op: Aerospike sets are created implicitly on first write. */
  override def create(dataset: String): Unit =
    logger.debug(s"create($dataset) is a no-op for Aerospike (sets are created on write)")

  /** Asynchronously writes every request; returns per-request success flags in
    * the same order as `putRequests`.
    *
    * Each request becomes one `MapOperation.put` on bin `"v"` keyed by the
    * request timestamp (or `System.currentTimeMillis()` when absent). Requests
    * with null `keyBytes`/`valueBytes` or an invalid dataset name resolve to
    * `false` without touching the cluster.
    *
    * Metrics per dataset: `Metrics.Name.WriteLatencyMillis` (batch wall time),
    * `Metrics.Name.ResponseCount`, `Metrics.Name.RequestCount` (successes),
    * `write.exceptions` and `write.exceptions.<result_name>`
    * (per-result-code breakdown, see `AerospikeKvStore.resultName`).
    */
  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    val startNs = System.nanoTime()
    val rejected = Future.successful(Some(WriteFailure(ResultCode.PARAMETER_ERROR, inDoubt = false)))
    val results = Future.sequence(putRequests.map { req =>
      if (req.keyBytes == null || req.valueBytes == null || datasetNameError(req.dataset).isDefined) rejected
      else putOne(req)
    })
    results.map { outcomes =>
      val latencyMs = (System.nanoTime() - startNs) / 1000000
      putRequests.zip(outcomes).groupBy(_._1.dataset).foreach { case (dataset, pairs) =>
        val ctx = metricsContextFor(dataset)
        ctx.distribution(Metrics.Name.WriteLatencyMillis, latencyMs)
        pairs.foreach { case (_, outcome) =>
          ctx.increment(Metrics.Name.ResponseCount)
          outcome match {
            case None => ctx.increment(Metrics.Name.RequestCount)
            case Some(failure) =>
              ctx.increment("write.exceptions")
              ctx.increment(s"write.exceptions.${AerospikeKvStore.resultName(failure.resultCode)}")
          }
        }
        logWriteFailures(dataset, pairs.flatMap(_._2), pairs.size)
      }
      outcomes.map(_.isEmpty)
    }
  }

  /** One summary line per dataset per `multiPut`, so a failure storm cannot
    * flood the log; groups failures by result code with counts and `inDoubt`
    * tallies. */
  private def logWriteFailures(dataset: String, failures: Seq[WriteFailure], total: Int): Unit =
    if (failures.nonEmpty) {
      val summary = failures.groupBy(_.resultCode).toSeq.sortBy(-_._2.size).map { case (code, fs) =>
        s"${ResultCode.getResultString(code)} (code=$code) x${fs.size}, inDoubt=${fs.count(_.inDoubt)}"
      }.mkString("; ")
      logger.warn(s"multiPut to $dataset: ${failures.size}/$total writes failed: $summary")
    }

  /** Issues one async map-put. Returns `None` on success or `Some(WriteFailure)`
    * capturing the Aerospike result code and `inDoubt` flag — both async
    * listener failures and synchronous launch exceptions are converted to
    * `Some`, so the returned future never fails. Streaming requests are routed
    * to their day-bucketed record key. */
  private def putOne(req: PutRequest): Future[Option[WriteFailure]] = {
    val ts = req.tsMillis.getOrElse(System.currentTimeMillis())
    val tableType = getTableType(req.dataset)
    val setName = physicalSetName(req.dataset)
    val keyBytes = tableType match {
      case StreamingTable => streamingRecordKey(req.dataset, req.keyBytes, ts, config.keyPrefix)
      case _                => recordKey(req.dataset, req.keyBytes, config.keyPrefix)
    }
    val key = new Key(config.namespace, setName, keyBytes)
    val writePolicy = writePolicyFor(req.dataset, req.keyBytes)
    val op = MapOperation.put(KeyOrderedPolicy, ValueBin, Value.get(ts), Value.get(req.valueBytes))

    val promise = Promise[Option[WriteFailure]]()
    try {
      client.operate(
        null, // round-robin event loop
        new RecordListener {
          override def onSuccess(key: Key, record: com.aerospike.client.Record): Unit = promise.success(None)
          override def onFailure(e: AerospikeException): Unit =
            promise.success(Some(WriteFailure(e.getResultCode, e.getInDoubt)))
        },
        writePolicy,
        key,
        op
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to launch put for ${req.dataset}", e)
        val failure = e match {
          case ae: AerospikeException => WriteFailure(ae.getResultCode, ae.getInDoubt)
          case _                      => WriteFailure(ResultCode.CLIENT_ERROR, inDoubt = false)
        }
        promise.success(Some(failure))
    }
    promise.future
  }

  /** Validates the physical set name fits Aerospike's 63-byte set-name limit;
    * returns the error message when it doesn't. */
  private def datasetNameError(dataset: String): Option[String] = {
    val setNameBytes = physicalSetName(dataset).getBytes("UTF-8")
    if (setNameBytes.length > 63) {
      Some(s"Aerospike set name '${physicalSetName(dataset)}' exceeds 63 bytes (${setNameBytes.length})")
    } else None
  }

  /** Pre-flight validation for a `GetRequest`: null `keyBytes` or an invalid
    * dataset name produce a descriptive error used to build a `Failure`
    * response without hitting the cluster. */
  private def requestError(req: GetRequest): Option[String] = {
    if (req.keyBytes == null) Some("GetRequest.keyBytes is null")
    else datasetNameError(req.dataset)
  }

  /** Batch-reads all requests through one async `client.get`, returning a
    * `GetResponse` per request in the original order.
    *
    * Each valid request expands into one or more `BatchRead`s (one per day
    * bucket for streaming datasets; exactly one otherwise), each carrying a
    * server-side map key-range filter honouring `afterTsMillis`. Invalid
    * requests get an immediate `IllegalArgumentException` failure response.
    * Per-record `KEY_NOT_FOUND` results become empty `TimedValue` lists; other
    * per-record errors and whole-batch failures become `Failure`s on the
    * affected requests.
    *
    * Metrics per dataset: `Metrics.Name.LatencyMillis`,
    * `Metrics.Name.ResponseCount`, `Metrics.Name.RequestCount` (successes),
    * `Metrics.Name.FetchExceptions`.
    */
  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val startNs = System.nanoTime()
    if (requests.isEmpty) return Future.successful(Nil)

    val indexed = requests.zipWithIndex
    val (badIndexed, goodIndexed) = indexed.partition { case (req, _) => requestError(req).isDefined }
    val badByIndex = badIndexed.map { case (req, idx) =>
      idx -> GetResponse(req, Failure(new IllegalArgumentException(requestError(req).get)))
    }.toMap

    if (goodIndexed.isEmpty) return Future.successful((0 until requests.size).map(badByIndex))

    val taggedReads = goodIndexed.flatMap { case (req, idx) => buildBatchReads(req).map { case (br, _) => (br, req, idx) } }
    if (taggedReads.isEmpty) return Future.successful(requests.map(r => GetResponse(r, Success(Seq.empty))))

    val batchReads = taggedReads.map(_._1).asJava
    val promise = Promise[List[BatchRead]]()

    try {
      client.get(
        null,
        new BatchListListener {
          override def onSuccess(records: java.util.List[BatchRead]): Unit =
            promise.success(records.asScala.toList)
          override def onFailure(e: AerospikeException): Unit =
            promise.failure(e)
        },
        AerospikeClientHolder.policyTemplates(config).batch,
        batchReads
      )
    } catch {
      case e: Exception => promise.failure(e)
    }

    promise.future
      .map { results =>
        val latencyMs = (System.nanoTime() - startNs) / 1000000
        val readByIndex = taggedReads.zip(results).groupBy(_._1._3)
        val goodByIndex = readByIndex.map { case (idx, tagged) =>
          val request = tagged.head._1._2
          val taggedWithResults = tagged.map { case ((_, _, _), br) => (request, br) }
          val ctx = metricsContextFor(request.dataset)
          ctx.distribution(Metrics.Name.LatencyMillis, latencyMs)
          ctx.increment(Metrics.Name.ResponseCount)

          val firstError = taggedWithResults.map(_._2).find { br =>
            br.resultCode != 0 && br.resultCode != ResultCode.KEY_NOT_FOUND_ERROR
          }
          val response = firstError match {
            case Some(br) =>
              ctx.increment(Metrics.Name.FetchExceptions)
              GetResponse(request, Failure(aerospikeException(br.resultCode, br.inDoubt)))
            case None =>
              val values = taggedWithResults.flatMap { case (_, br) => extractTimedValues(br.record) }.sortBy(_.millis)
              ctx.increment(Metrics.Name.RequestCount)
              GetResponse(request, Success(values))
          }
          idx -> response
        }.toMap

        (0 until requests.size).map(i => goodByIndex.getOrElse(i, badByIndex(i)))
      }
      .recover { case ex =>
        requests.map { req =>
          metricsContextFor(req.dataset).increment(Metrics.Name.FetchExceptions)
          GetResponse(req, Failure(ex))
        }
      }
  }

  /** Expands one `GetRequest` into its `BatchRead`s, each tagged back to the
    * request. Batch/metadata datasets yield a single read; streaming datasets
    * yield one read per day bucket from `clampedStartDay` through today, where
    * the start is `afterTsMillis`'s day (the window start when absent) clamped to
    * `min(ttlSeconds, streamingMaxLookbackDays)` of lookback — expired data is
    * never read. Each read carries a map key-range op filtering entries with
    * `ts >= afterTsMillis` (all entries when absent). */
  private def buildBatchReads(req: GetRequest): Seq[(BatchRead, GetRequest)] = {
    val op = req.afterTsMillis match {
      case Some(ts) => MapOperation.getByKeyRange(ValueBin, Value.get(ts), null, MapReturnType.KEY_VALUE)
      case None     => MapOperation.getByKeyRange(ValueBin, null, null, MapReturnType.KEY_VALUE)
    }

    getTableType(req.dataset) match {
      case StreamingTable =>
        val now = System.currentTimeMillis()
        val endDay = dayTs(now)
        val effectiveLookbackMillis = math.min(
          config.ttlSeconds * 1000L,
          config.streamingMaxLookbackDays.toLong * MillisPerDay
        )
        val lookbackStartDay = dayTs(now - effectiveLookbackMillis)
        val clampedStartDay = req.afterTsMillis.map(ts => math.max(dayTs(ts), lookbackStartDay)).getOrElse(lookbackStartDay)
        (clampedStartDay to endDay by MillisPerDay).map { day =>
          val recordKeyBytes = streamingRecordKeyFromDay(req.dataset, req.keyBytes, day, config.keyPrefix)
          val key = new Key(config.namespace, physicalSetName(req.dataset), recordKeyBytes)
          (new BatchRead(key, Array(op)), req)
        }
      case _ =>
        val recordKeyBytes = recordKey(req.dataset, req.keyBytes, config.keyPrefix)
        val key = new Key(config.namespace, physicalSetName(req.dataset), recordKeyBytes)
        Seq((new BatchRead(key, Array(op)), req))
    }
  }

  /** Decodes bin `"v"` map entries into `TimedValue`s. Null/missing records and
    * bins yield empty; individual malformed entries are skipped with a warning
    * rather than failing the request. */
  private def extractTimedValues(record: com.aerospike.client.Record): Seq[KVStore.TimedValue] = {
    if (record == null) return Seq.empty
    val raw = record.getList(ValueBin)
    if (raw == null) return Seq.empty
    raw.asScala.flatMap {
      case entry: java.util.Map.Entry[_, _] =>
        try {
          val ts = entry.getKey match {
            case l: java.lang.Long => l.longValue()
            case v: Value          => v.toLong
            case other             => other.toString.toLong
          }
          val bytes = entry.getValue match {
            case b: Array[Byte] => b
            case v: Value       => v.getObject.asInstanceOf[Array[Byte]]
            case other          => other.asInstanceOf[Array[Byte]]
          }
          Some(KVStore.TimedValue(bytes, ts))
        } catch {
          case e: Exception =>
            logger.warn(s"Skipping malformed map entry in bin '$ValueBin': ${e.getMessage}")
            None
        }
      case other =>
        logger.warn(s"Skipping unexpected entry type in bin '$ValueBin': ${other.getClass.getName}")
        None
    }.toSeq
  }

  /** Clones the cached write template and sets per-record expiration:
    * `expiration = -1` (never expire) for metadata tables and the batch-set
    * `group_by_serving_info` key; `config.ttlSeconds` otherwise. */
  private def writePolicyFor(dataset: String, keyBytes: Array[Byte]): WritePolicy = {
    val base = new WritePolicy(AerospikeClientHolder.policyTemplates(config).write)
    val tableType = getTableType(dataset)
    val neverExpire = tableType match {
      case MetadataTable(_) => true
      case BatchTable       => isServingInfoKey(keyBytes)
      case StreamingTable   => false
    }
    base.expiration = if (neverExpire) -1 else config.ttlSeconds.toInt
    base
  }

  /** True when `keyBytes` is Chronon's `group_by_serving_info` key, which must
    * never expire (see [[writePolicyFor]]). Byte-wise compare — the raw key is
    * not valid UTF-8 in general. */
  private def isServingInfoKey(keyBytes: Array[Byte]): Boolean =
    Arrays.equals(keyBytes, ApiConstants.GroupByServingInfoKey.getBytes(ApiConstants.UTF8))

  /** Rebuilds an `AerospikeException` from a batch-read result code so
    * per-record failures surface with the same type callers see from the
    * client. */
  private def aerospikeException(resultCode: Int, inDoubt: Boolean): AerospikeException = {
    val msg = s"Aerospike error: ${ResultCode.getResultString(resultCode)} (code=$resultCode, inDoubt=$inDoubt)"
    new AerospikeException(resultCode, msg)
  }

  /** Loads one partition of `sourceOfflineTable` into `destinationOnlineDataSet`
    * via [[Spark2AerospikeLoader.upload]] on the ambient `SparkSession`.
    * Blocks until the load finishes; throws if any `multiPut` chunk fails. */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate()
    Spark2AerospikeLoader.upload(spark, sourceOfflineTable, Some(destinationOnlineDataSet), partition, config)
  }
}

object AerospikeKvStore {

  /** Failure detail kept internally; the public `multiPut` contract stays
    * `Seq[Boolean]`. `inDoubt` is Aerospike's may-have-applied flag — the write
    * could have succeeded despite the error (e.g. timeout), which matters
    * because retries are only safe thanks to last-write-wins idempotency. */
  private[chronon] case class WriteFailure(resultCode: Int, inDoubt: Boolean)

  /** Metric-name-safe label, e.g. -7 -> "no_more_available_connections". The
    * metrics API has no tags, so the result is encoded in the name; cardinality
    * is bounded by `ResultCode`. Unknown codes fall back to `code_<n>`. */
  private[chronon] def resultName(code: Int): String = {
    val name = ResultCode.getResultString(code).toLowerCase.replaceAll("[^a-z0-9]+", "_").stripPrefix("_").stripSuffix("_")
    if (name.nonEmpty) name else s"code_${if (code < 0) "neg_" else ""}${math.abs(code)}"
  }

  /** Builds a store from Chronon's `userConf` map; see [[AerospikeConfig]]
    * for required and optional keys. Throws `IllegalArgumentException` on
    * missing/invalid config. */
  def apply(userConf: Map[String, String]): AerospikeKvStore =
    new AerospikeKvStore(AerospikeConfig.fromUserConf(userConf))
}
