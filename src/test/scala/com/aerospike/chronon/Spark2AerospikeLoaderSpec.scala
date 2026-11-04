package com.aerospike.chronon

import ai.chronon.api.PartitionSpec
import ai.chronon.online.KVStore
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import scala.concurrent.Future

class Spark2AerospikeLoaderSpec extends AnyFlatSpec with Matchers {

  private val requiredArgs = Seq(
    "--table-name", "my_gb_upload",
    "--end-ds", "2026-01-01",
    "--aerospike-hosts", "localhost",
    "--namespace", "test",
    "--ttl", "86400",
    "--max-lookback-days", "1")

  "userConfFromArgs" should "use the loader's bulk timeout defaults when no timeout flags are given" in {
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs))
    conf.socketTimeoutMillis shouldBe AerospikeConfig.LoaderSocketTimeoutMs
    conf.readTotalTimeoutMillis shouldBe AerospikeConfig.LoaderReadTotalTimeoutMs
    conf.writeTotalTimeoutMillis shouldBe AerospikeConfig.LoaderWriteTotalTimeoutMs
    conf.maxRetriesRead shouldBe AerospikeConfig.LoaderMaxRetriesRead
    conf.maxRetriesWrite shouldBe AerospikeConfig.LoaderMaxRetriesWrite
    AerospikeConfig.LoaderSocketTimeoutMs shouldBe 5000
    AerospikeConfig.LoaderWriteTotalTimeoutMs shouldBe 30000
    AerospikeConfig.LoaderMaxRetriesWrite shouldBe 2
  }

  it should "use the loader pool defaults (cap 32, floor 0) when no pool flags are given" in {
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs))
    conf.maxConnsPerNode shouldBe AerospikeConfig.DefaultLoaderMaxConnsPerNode
    conf.minConnsPerNode shouldBe AerospikeConfig.DefaultLoaderMinConnsPerNode
    conf.loaderMaxConnsPerNode shouldBe conf.maxConnsPerNode
    conf.loaderMinConnsPerNode shouldBe conf.minConnsPerNode
  }

  it should "honour explicit pool flags" in {
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs ++ Seq(
      "--max-conns-per-node", "10", "--min-conns-per-node", "3")))
    conf.maxConnsPerNode shouldBe 10
    conf.minConnsPerNode shouldBe 3
  }

  it should "parse a --ttl under the streaming lookback when --max-lookback-days is absent" in {
    // the loader writes batch data only: no flag means lookback 0, not the serving default 5
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs.dropRight(2)))
    conf.streamingMaxLookbackDays shouldBe 0
  }

  it should "still reject an explicit --max-lookback-days above the streaming TTL" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
        requiredArgs.updated(requiredArgs.length - 1, "5")))
    }
    ex.getMessage should include(AerospikeConfig.StreamingMaxLookbackDaysKey)
  }

  it should "honour the timeout flags when given" in {
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs ++ Seq(
      "--socket-timeout-millis", "2000",
      "--read-total-timeout-millis", "60000",
      "--write-total-timeout-millis", "70000",
      "--max-retries-read", "1",
      "--max-retries-write", "4")))
    conf.socketTimeoutMillis shouldBe 2000
    conf.readTotalTimeoutMillis shouldBe 60000
    conf.writeTotalTimeoutMillis shouldBe 70000
    conf.maxRetriesRead shouldBe 1
    conf.maxRetriesWrite shouldBe 4
  }

  // section 13 — conf-driven partition spec, strict end-ds, empty partitions, load summary
  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("Spark2AerospikeLoaderSpec")
    .config("spark.sql.warehouse.dir", Files.createTempDirectory("loader-spec-warehouse").toString)
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private def config = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs))

  private def withWarns[A](body: scala.collection.mutable.ListBuffer[String] => A): A = {
    val warns = scala.collection.mutable.ListBuffer.empty[String]
    Spark2AerospikeLoader.warnHook = warns += _
    try body(warns)
    finally Spark2AerospikeLoader.warnHook = { _ => () }
  }

  private def withRecordingStore[A](body: => A): A = {
    Spark2AerospikeLoader.kvStoreFactory = _ => new Spark2AerospikeLoaderSpec.RecordingStore
    Spark2AerospikeLoaderSpec.RecordingStore.drain()
    try body
    finally Spark2AerospikeLoader.kvStoreFactory = config => new AerospikeKvStore(config)
  }

  private def setPartitionConf(column: String, format: String): Unit = {
    spark.conf.set("spark.chronon.partition.column", column)
    spark.conf.set("spark.chronon.partition.format", format)
  }

  private def uploadView(viewName: String,
                         rows: Seq[(String, String, String)],
                         column: String = "ds"): Unit = {
    import spark.implicits._
    rows.toDF("key_str", "value_str", column)
      .selectExpr("CAST(key_str AS BINARY) AS key_bytes",
                  "CAST(value_str AS BINARY) AS value_bytes",
                  column)
      .createOrReplaceTempView(viewName)
  }

  private def binaryUploadView(viewName: String,
                               rows: Seq[(Array[Byte], Array[Byte], String)],
                               column: String = "ds"): Unit = {
    import spark.implicits._
    rows.toDF("key_bytes", "value_bytes", column).createOrReplaceTempView(viewName)
  }

  "upload" should "reject an end-ds that does not match spark.chronon.partition.format" in {
    setPartitionConf("ds", "yyyyMMdd")
    uploadView("strict_format_view", Seq(("k", "v", "20261013")))
    val ex = intercept[IllegalArgumentException] {
      Spark2AerospikeLoader.upload(spark, "strict_format_view", None, "2026-10-13", config)
    }
    ex.getMessage should include("2026-10-13")
    ex.getMessage should include("yyyyMMdd")
    ex.getMessage should include("spark.chronon.partition.format")
  }

  it should "reject a non-canonical end-ds under the default format" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    val ex = intercept[IllegalArgumentException] {
      Spark2AerospikeLoader.upload(spark, "strict_format_view", None, "2026-1-3", config)
    }
    ex.getMessage should include("2026-1-3")
    ex.getMessage should include("spark.chronon.partition.format")
  }

  it should "name the column and conf key when the partition column is missing" in {
    setPartitionConf("dt", "yyyy-MM-dd")
    uploadView("missing_column_view", Seq(("k", "v", "2026-10-13")), column = "ds")
    val ex = intercept[IllegalArgumentException] {
      Spark2AerospikeLoader.upload(spark, "missing_column_view", None, "2026-10-13", config)
    }
    ex.getMessage should include("dt")
    ex.getMessage should include("spark.chronon.partition.column")
    ex.getMessage should include("ds")
  }

  it should "throw on a partition with zero rows naming table, column and partition" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    uploadView("empty_view", Seq(("k", "v", "2026-10-12")))
    val ex = intercept[IllegalStateException] {
      Spark2AerospikeLoader.upload(spark, "empty_view", None, "2026-10-13", config)
    }
    ex.getMessage should include("empty_view")
    ex.getMessage should include("ds")
    ex.getMessage should include("2026-10-13")
  }

  it should "warn and return a zero summary on an empty partition with allowEmpty" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    uploadView("empty_view", Seq(("k", "v", "2026-10-12")))
    val summary = withWarns { warns =>
      val s = Spark2AerospikeLoader.upload(spark, "empty_view", None, "2026-10-13", config, allowEmpty = true)
      warns.exists(_.contains("empty_view")) shouldBe true
      s
    }
    summary shouldBe Spark2AerospikeLoader.LoadSummary(0, 0, 0)
  }

  it should "count rows read, written and dropped in the returned summary" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      binaryUploadView("summary_view", Seq(
        ("user1".getBytes(UTF_8), "v1".getBytes(UTF_8), "2026-10-13"),
        ("user2".getBytes(UTF_8), "v2".getBytes(UTF_8), "2026-10-13"),
        (null, "v3".getBytes(UTF_8), "2026-10-13"),
        ("user3".getBytes(UTF_8), null, "2026-10-13"),
        ("other".getBytes(UTF_8), "v4".getBytes(UTF_8), "2026-10-12")
      ))
      Spark2AerospikeLoaderSpec.RecordingStore.drain()
      val summary = Spark2AerospikeLoader.upload(spark, "summary_view", None, "2026-10-13", config)
      summary shouldBe Spark2AerospikeLoader.LoadSummary(rowsRead = 4, rowsWritten = 2, rowsDropped = 2)
      Spark2AerospikeLoaderSpec.RecordingStore.puts.size shouldBe 2
    }
  }

  it should "warn when a group-by upload carries no group_by_serving_info row" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      uploadView("no_serving_info_upload", Seq(("k1", "v1", "2026-10-13"), ("k2", "v2", "2026-10-13")))
      withWarns { warns =>
        Spark2AerospikeLoader.upload(spark, "no_serving_info_upload", None, "2026-10-13", config)
        warns.exists(_.contains("group_by_serving_info")) shouldBe true
      }
    }
  }

  it should "not warn about serving info for the stats datasets" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      uploadView("gb_daily_stats_upload", Seq(("k1", "v1", "2026-10-13")))
      withWarns { warns =>
        Spark2AerospikeLoader.upload(spark, "gb_daily_stats_upload", None, "2026-10-13", config)
        warns.forall(!_.contains("group_by_serving_info")) shouldBe true
      }
    }
  }

  it should "load a yyyyMMdd/dt table and stamp ts at the end of the partition day" in {
    setPartitionConf("dt", "yyyyMMdd")
    withRecordingStore {
      binaryUploadView("custom_partition_view",
        Seq(("user1".getBytes(UTF_8), "v1".getBytes(UTF_8), "20261013")), column = "dt")
      Spark2AerospikeLoaderSpec.RecordingStore.drain()
      val summary = Spark2AerospikeLoader.upload(spark, "custom_partition_view", None, "20261013", config)
      summary.rowsWritten shouldBe 1
      val req = Spark2AerospikeLoaderSpec.RecordingStore.puts.peek()
      val custom = PartitionSpec(format = "yyyyMMdd", spanMillis = 86400000L)
      req.tsMillis shouldBe Some(custom.epochMillis("20261013") + custom.spanMillis) // 2026-10-14T00:00:00Z
    }
  }

  it should "read a ts column regardless of case" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      import spark.implicits._
      Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), 1234L, "2026-10-13"))
        .toDF("key_bytes", "value_bytes", "TS", "ds")
        .createOrReplaceTempView("ts_case_view")
      Spark2AerospikeLoaderSpec.RecordingStore.drain()
      Spark2AerospikeLoader.upload(spark, "ts_case_view", None, "2026-10-13", config)
      Spark2AerospikeLoaderSpec.RecordingStore.puts.peek().tsMillis shouldBe Some(1234L)
    }
  }

  "datasetForTable" should "map _upload tables to their batch datasets, case-insensitively" in {
    import ai.chronon.api.{Constants => ApiConstants}
    Spark2AerospikeLoader.datasetForTable("gb_upload") shouldBe "GB_BATCH"
    Spark2AerospikeLoader.datasetForTable("db.gb_upload") shouldBe "GB_BATCH"
    Spark2AerospikeLoader.datasetForTable("a.b.gb_upload") shouldBe "GB_BATCH"
    Spark2AerospikeLoader.datasetForTable("GB_UPLOAD") shouldBe "GB_BATCH"
    Spark2AerospikeLoader.datasetForTable("Gb_Upload") shouldBe "GB_BATCH"
    Spark2AerospikeLoader.datasetForTable("x_daily_stats_upload") shouldBe ApiConstants.StatsBatchDataset
    Spark2AerospikeLoader.datasetForTable("x_logged_daily_stats_upload") shouldBe ApiConstants.LogStatsBatchDataset
    Spark2AerospikeLoader.datasetForTable("x_consistency_upload") shouldBe ApiConstants.ConsistencyMetricsDataset
    Spark2AerospikeLoader.datasetForTable("db.x_daily_stats_upload") shouldBe ApiConstants.StatsBatchDataset
    Spark2AerospikeLoader.datasetForTable("db.x_logged_daily_stats_upload") shouldBe ApiConstants.LogStatsBatchDataset
    Spark2AerospikeLoader.datasetForTable("db.x_consistency_upload") shouldBe ApiConstants.ConsistencyMetricsDataset
    Spark2AerospikeLoader.datasetForTable("some_table") shouldBe "SOME_TABLE_BATCH"
  }

  it should "keep the qualifier for non-_upload names (pins current behaviour)" in {
    Spark2AerospikeLoader.datasetForTable("db.some_table") shouldBe "DB.SOME_TABLE_BATCH"
  }

  "versionTsMillis" should "stamp the end of the partition day" in {
    val ymd = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L)
    Spark2AerospikeLoader.versionTsMillis(ymd, "2026-10-13") shouldBe
      java.time.LocalDate.of(2026, 10, 14).atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val compact = PartitionSpec(format = "yyyyMMdd", spanMillis = 86400000L)
    Spark2AerospikeLoader.versionTsMillis(compact, "20261013") shouldBe
      java.time.LocalDate.of(2026, 10, 14).atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    Spark2AerospikeLoader.versionTsMillis(ymd, "2024-02-29") shouldBe
      java.time.LocalDate.of(2024, 3, 1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
  }

  "upload" should "reject a dataset override not ending in _BATCH before reading the table" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    val ex = intercept[IllegalArgumentException] {
      Spark2AerospikeLoader.upload(spark, "nonexistent_table", Some("MY_GB"), "2026-10-13", config)
    }
    ex.getMessage should include("MY_GB")
    ex.getMessage should include("_BATCH")
  }

  it should "fail the load with the failing rows and dataset named when multiPut returns false" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      binaryUploadView("fail_gb_upload", Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-13")))
      Spark2AerospikeLoaderSpec.RecordingStore.result = false
      val ex = intercept[Exception] {
        Spark2AerospikeLoader.upload(spark, "fail_gb_upload", None, "2026-10-13", config)
      }
      ex shouldBe a[org.apache.spark.SparkException]
      val chain = Iterator.iterate(ex: Throwable)(_.getCause).takeWhile(_ != null).toSeq
      chain.exists(e => e.getMessage != null && e.getMessage.contains("multiPut failed for") &&
        e.getMessage.contains("FAIL_GB_BATCH")) shouldBe true
    }
  }

  it should "fail the load on batch-timeout when a multiPut never completes" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs ++ Seq("--batch-timeout-seconds", "1")))
    withRecordingStore {
      binaryUploadView("hang_gb_upload", Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-13")))
      Spark2AerospikeLoaderSpec.RecordingStore.neverCompletes = true
      val start = System.nanoTime()
      val ex = intercept[Exception] {
        Spark2AerospikeLoader.upload(spark, "hang_gb_upload", None, "2026-10-13", conf)
      }
      val elapsedMs = (System.nanoTime() - start) / 1000000
      elapsedMs should be < 30000L
      val chain = Iterator.iterate(ex: Throwable)(_.getCause).takeWhile(_ != null).toSeq
      chain.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]) shouldBe true
    }
  }

  it should "chunk a single-partition load into 1000-row multiPut calls" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      import spark.implicits._
      (1 to 2500).map(i => (s"k$i".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-13"))
        .toDF("key_bytes", "value_bytes", "ds")
        .repartition(1)
        .createOrReplaceTempView("chunked_gb_upload")
      val summary = Spark2AerospikeLoader.upload(spark, "chunked_gb_upload", None, "2026-10-13", config)
      import scala.collection.JavaConverters._
      Spark2AerospikeLoaderSpec.RecordingStore.callSizes.asScala.toSeq shouldBe Seq(1000, 1000, 500)
      summary shouldBe Spark2AerospikeLoader.LoadSummary(rowsRead = 2500, rowsWritten = 2500, rowsDropped = 0)
    }
  }

  it should "honour numeric ts columns and default others to the partition end" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    val defaultTs = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L).epochMillis("2026-10-13") + 86400000L
    withRecordingStore {
      import spark.implicits._
      def tsView(name: String, tsExpr: String): Unit =
        Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-13"))
          .toDF("key_bytes", "value_bytes", "ds")
          .selectExpr("key_bytes", "value_bytes", s"$tsExpr AS ts", "ds")
          .createOrReplaceTempView(name)

      def loadedTs(view: String): Option[Long] = {
        Spark2AerospikeLoaderSpec.RecordingStore.drain()
        Spark2AerospikeLoader.upload(spark, view, None, "2026-10-13", config)
        Spark2AerospikeLoaderSpec.RecordingStore.puts.peek().tsMillis.map(_.asInstanceOf[Long])
      }

      tsView("ts_int_view", "CAST(4321 AS INT)")
      loadedTs("ts_int_view") shouldBe Some(4321L)
      tsView("ts_double_view", "CAST(1234.0 AS DOUBLE)")
      loadedTs("ts_double_view") shouldBe Some(1234L)
      tsView("ts_null_view", "CAST(NULL AS BIGINT)")
      loadedTs("ts_null_view") shouldBe Some(defaultTs)
      tsView("ts_string_view", "'1234'")
      loadedTs("ts_string_view") shouldBe Some(defaultTs)
      tsView("ts_timestamp_view", "CAST('2026-10-13 01:02:03' AS TIMESTAMP)")
      loadedTs("ts_timestamp_view") shouldBe Some(defaultTs)
    }
  }

  it should "drop every row when key_bytes or value_bytes is not binary" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      import spark.implicits._
      Seq(("k", "v", "2026-10-13"))
        .toDF("key_bytes", "value_bytes", "ds")
        .createOrReplaceTempView("string_cols_gb_upload")
      val warnsAndSummary = withWarns { warns =>
        val s = Spark2AerospikeLoader.upload(spark, "string_cols_gb_upload", None, "2026-10-13", config)
        warns.exists(_.contains("Dropped 1 rows")) shouldBe true
        s
      }
      warnsAndSummary shouldBe Spark2AerospikeLoader.LoadSummary(rowsRead = 1, rowsWritten = 0, rowsDropped = 1)

      Seq(("k".getBytes(UTF_8), "2026-10-13"))
        .toDF("key_bytes", "ds")
        .createOrReplaceTempView("no_value_col_gb_upload")
      val summary = Spark2AerospikeLoader.upload(spark, "no_value_col_gb_upload", None, "2026-10-13", config)
      summary shouldBe Spark2AerospikeLoader.LoadSummary(rowsRead = 1, rowsWritten = 0, rowsDropped = 1)
    }
  }

  "main" should "load through the CLI without stopping the ambient session" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      binaryUploadView("main_gb_upload", Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-13")))
      spark.sparkContext // force the session before main calls getOrCreate
      Spark2AerospikeLoader.main(Array(
        "--table-name", "main_gb_upload",
        "--end-ds", "2026-10-13",
        "--aerospike-hosts", "localhost",
        "--namespace", "test",
        "--ttl", "86400",
        "--max-lookback-days", "1"))
      Spark2AerospikeLoaderSpec.RecordingStore.puts.isEmpty shouldBe false
      spark.sparkContext.isStopped shouldBe false
    }
  }

  it should "return normally on an empty partition with --allow-empty" in {
    setPartitionConf("ds", "yyyy-MM-dd")
    withRecordingStore {
      binaryUploadView("main_empty_upload", Seq(("k".getBytes(UTF_8), "v".getBytes(UTF_8), "2026-10-12")))
      spark.sparkContext
      Spark2AerospikeLoader.main(Array(
        "--table-name", "main_empty_upload",
        "--end-ds", "2026-10-13",
        "--aerospike-hosts", "localhost",
        "--namespace", "test",
        "--ttl", "86400",
        "--max-lookback-days", "1",
        "--allow-empty"))
      spark.sparkContext.isStopped shouldBe false
    }
  }

  "userConfFromArgs" should "map ttl/prefix/in-flight flags into the config" in {
    val ns = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs.updated(9, "namespace")))
    ns.batchTtl shouldBe TtlSetting.NamespaceDefault
    ns.streamingTtl shouldBe TtlSetting.NamespaceDefault
    val never = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs.updated(9, "0")))
    never.batchTtl shouldBe TtlSetting.NeverExpire
    never.streamingTtl shouldBe TtlSetting.NeverExpire
    val prefixed = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs ++ Seq("--key-prefix", "p:")))
    prefixed.keyPrefix shouldBe Some("p:")
    val capped = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs ++ Seq("--max-commands-in-process", "40")))
    capped.maxCommandsInProcess shouldBe Some(40)
  }

  it should "map TLS and auth flags into the config, omitting them when unset" in {
    val plain = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs))
    plain.tls shouldBe None
    plain.credentials shouldBe None
    plain.authMode shouldBe AuthModeSetting.Internal

    val conf = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(
      requiredArgs.updated(5, "node1:aerospike-tls:4333") ++ Seq(
      "--tls-enabled", "true",
      "--tls-name", "aerospike-tls",
      "--tls-protocols", "TLSv1.2,TLSv1.3",
      "--tls-ciphers", "TLS_AES_256_GCM_SHA384",
      "--tls-truststore-file", "/tls/ca.truststore.p12",
      "--tls-truststore-password-file", "/tls/storepass.txt",
      "--tls-truststore-type", "PKCS12",
      "--tls-keystore-file", "/tls/client.keystore.p12",
      "--tls-keystore-password-file", "/tls/storepass.txt",
      "--tls-keystore-key-password-file", "/tls/keypass.txt",
      "--tls-keystore-type", "PKCS12",
      "--aerospike-user", "u",
      "--aerospike-password-file", "/pw",
      "--auth-mode", "internal")))
    val t = conf.tls.get
    t.tlsName shouldBe Some("aerospike-tls")
    t.protocols shouldBe Some(Seq("TLSv1.2", "TLSv1.3"))
    t.truststoreFile shouldBe Some("/tls/ca.truststore.p12")
    t.truststoreType shouldBe "PKCS12"
    t.keystoreFile shouldBe Some("/tls/client.keystore.p12")
    t.keystoreKeyPasswordFile shouldBe Some("/tls/keypass.txt")
    conf.hosts.map(h => (h.name, h.tlsName, h.port)) shouldBe
      Seq(("node1", Some("aerospike-tls"), 4333))
    conf.credentials shouldBe Some(Credentials("u", Some("/pw"), None))

    // env-var credentials map as a NAME (resolved per executor), never a value
    AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs ++ Seq(
      "--aerospike-user", "u", "--aerospike-password-env", "AS_PW"))).credentials shouldBe
      Some(Credentials("u", None, Some("AS_PW")))

    // pki + forLoginOnly flags also flow through
    val pki = AerospikeConfig.fromUserConf(Spark2AerospikeLoader.userConfFromArgs(requiredArgs ++ Seq(
      "--tls-enabled", "true",
      "--tls-keystore-file", "/tls/client.keystore.p12",
      "--tls-keystore-password-file", "/tls/storepass.txt",
      "--auth-mode", "pki")))
    pki.authMode shouldBe AuthModeSetting.Pki

    // TLS flags without --tls-enabled fail fast, same as the userConf path
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(
      Spark2AerospikeLoader.userConfFromArgs(requiredArgs ++ Seq("--tls-name", "x"))))
      .getMessage should include("aerospike.tls.enabled")
  }
}

object Spark2AerospikeLoaderSpec {

  /** Records every multiPut request into a JVM-wide queue so the loader's
    * executor path can be exercised in unit tests without an Aerospike
    * server. Register via `Spark2AerospikeLoader.kvStoreFactory`. */
  class RecordingStore extends KVStore {
    override implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    override def create(dataset: String): Unit = ()
    override def multiGet(requests: scala.collection.Seq[KVStore.GetRequest]) =
      Future.successful(requests.map(r => KVStore.GetResponse(r, scala.util.Success(Seq.empty))))
    override def multiPut(requests: scala.collection.Seq[KVStore.PutRequest]) = {
      RecordingStore.callSizes.add(requests.size)
      requests.foreach(RecordingStore.puts.add)
      if (RecordingStore.neverCompletes) scala.concurrent.Promise[scala.collection.Seq[Boolean]]().future
      else Future.successful(requests.map(_ => RecordingStore.result))
    }
    override def bulkPut(t: String, d: String, p: String): Unit = ()
  }
  object RecordingStore {
    val puts = new java.util.concurrent.ConcurrentLinkedQueue[KVStore.PutRequest]()
    val callSizes = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    @volatile var result: Boolean = true
    @volatile var neverCompletes: Boolean = false
    def drain(): Unit = {
      puts.clear()
      callSizes.clear()
      result = true
      neverCompletes = false
    }
  }
}
