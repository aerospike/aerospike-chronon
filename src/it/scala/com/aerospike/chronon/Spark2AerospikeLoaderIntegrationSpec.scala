package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants, PartitionSpec}
import ai.chronon.online.KVStore.GetRequest
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

/** Integration coverage for the Spark bulk-load path (tasks 5.3, 5.5, 5.5a). */
class Spark2AerospikeLoaderIntegrationSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private val daily = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L)
  private val ds = "2026-10-13"
  private val timeout = 30.seconds
  private val table = "test_upload.my_gb_upload"
  private val dataset = "MY_GB_BATCH"

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("Spark2AerospikeLoaderIntegrationSpec")
    .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private def createUploadTable(): Unit = {
    import spark.implicits._
    spark.sql(s"CREATE DATABASE IF NOT EXISTS test_upload")
    Seq(
      ("user1", "ir-bytes-1", ds),
      ("user2", "ir-bytes-2", ds),
      (ApiConstants.GroupByServingInfoKey, "serving-info-bytes", ds)
    ).toDF("key_str", "value_str", "ds")
      .selectExpr("CAST(key_str AS BINARY) AS key_bytes",
                  "CAST(value_str AS BINARY) AS value_bytes",
                  "ds")
      .write
      .mode("overwrite")
      .saveAsTable(table)
  }

  private def loaderArgs(table: String = table): Array[String] = Array(
    "--table-name", table,
    "--end-ds", ds,
    "--aerospike-hosts", aerospikeHost,
    "--namespace", aerospikeNamespace,
    "--ttl", "86400",
    "--max-lookback-days", "1"
  )

  // 5.3 — load lands rows + serving-info, idempotent on rerun
  "Spark2AerospikeLoader.main" should "load an _upload table into GROUPBY_BATCH and be idempotent" in {
    createUploadTable()
    Spark2AerospikeLoader.main(loaderArgs())
    spark.sparkContext.isStopped shouldBe false // main must not stop a session it didn't create
    Spark2AerospikeLoader.main(loaderArgs()) // rerun must not duplicate versions
    spark.sparkContext.isStopped shouldBe false

    val store = newStore()
    val resp = Await.result(store.multiGet(Seq(GetRequest("user1".getBytes(UTF_8), dataset))), timeout).head
    resp.values shouldBe a[Success[_]]
    val values = resp.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "ir-bytes-1"
    values.head.millis shouldBe daily.epochMillis(ds) + daily.spanMillis

    store.getString(ApiConstants.GroupByServingInfoKey, dataset, timeout.toMillis)
      .get shouldBe "serving-info-bytes"
  }

  // 5.5 — batch-ts invariant through the store
  it should "store ts = batchEndTsMillis so the fetcher millis >= batchEndTsMillis filter selects it" in {
    createUploadTable()
    Spark2AerospikeLoader.main(loaderArgs())
    spark.sparkContext.isStopped shouldBe false

    val store = newStore()
    val batchEndTs = daily.epochMillis(ds) + daily.spanMillis
    val resp = Await.result(
      store.multiGet(Seq(GetRequest("user1".getBytes(UTF_8), dataset, Some(batchEndTs)))), timeout).head
    val values = resp.values.get
    values should not be empty
    values.maxBy(_.millis).millis shouldBe batchEndTs
  }

  // 5.5a — bulkPut delegation
  "AerospikeKvStore.bulkPut" should "delegate to the loader and land rows" in {
    createUploadTable()
    val store = newStore()
    store.bulkPut(table, dataset, ds)

    val resp = Await.result(store.multiGet(Seq(GetRequest("user2".getBytes(UTF_8), dataset))), timeout).head
    resp.values.get.map(v => new String(v.bytes, UTF_8)) should contain("ir-bytes-2")
  }
}
