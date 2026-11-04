package com.aerospike.chronon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AerospikeConfigSpec extends AnyFlatSpec with Matchers {

  "AerospikeConfig.fromUserConf" should "parse a fully-specified config" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts"                       -> "a,b",
      "aerospike.port"                        -> "3100",
      "aerospike.namespace"                   -> "features",
      "aerospike.ttl.seconds"                 -> "864000",
      "aerospike.maxConnsPerNode"             -> "50",
      "aerospike.timeout.connect.millis"      -> "500",
      "aerospike.timeout.socket.millis"       -> "500",
      "aerospike.timeout.read.total.millis"   -> "7000",
      "aerospike.timeout.write.total.millis"  -> "8000",
      "aerospike.maxRetries.read"             -> "1",
      "aerospike.maxRetries.write"            -> "3",
      "aerospike.key.prefix"                  -> "team1:",
      "aerospike.streaming.maxLookbackDays"   -> "7",
      "aerospike.loader.batch.timeout.seconds" -> "120"
    ))
    conf.hosts.map(_.name) shouldBe Seq("a", "b")
    conf.hosts.map(_.port) shouldBe Seq(3100, 3100)
    conf.port shouldBe 3100
    conf.namespace shouldBe "features"
    conf.batchTtl shouldBe TtlSetting.Seconds(864000)
    conf.streamingTtl shouldBe TtlSetting.Seconds(864000)
    conf.maxConnsPerNode shouldBe 50
    conf.connectTimeoutMillis shouldBe 500
    conf.socketTimeoutMillis shouldBe 500
    conf.readTotalTimeoutMillis shouldBe 7000
    conf.writeTotalTimeoutMillis shouldBe 8000
    conf.maxRetriesRead shouldBe 1
    conf.maxRetriesWrite shouldBe 3
    conf.keyPrefix shouldBe Some("team1:")
    conf.streamingMaxLookbackDays shouldBe 7
    conf.loaderBatchTimeoutSeconds shouldBe 120
  }

  it should "trim whitespace from numeric values before parsing" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts"                        -> "localhost",
      "aerospike.namespace"                    -> "test",
      "aerospike.port"                         -> " 3100 ",
      "aerospike.ttl.seconds"                  -> " 864000 ",
      "aerospike.maxConnsPerNode"              -> " 50 ",
      "aerospike.timeout.connect.millis"       -> " 500 ",
      "aerospike.timeout.socket.millis"        -> " 500 ",
      "aerospike.timeout.read.total.millis"    -> " 7000 ",
      "aerospike.timeout.write.total.millis"   -> " 7000 ",
      "aerospike.streaming.maxLookbackDays"    -> " 7 ",
      "aerospike.loader.batch.timeout.seconds" -> " 120 "
    ))
    conf.port shouldBe 3100
    conf.batchTtl shouldBe TtlSetting.Seconds(864000)
    conf.streamingTtl shouldBe TtlSetting.Seconds(864000)
    conf.maxConnsPerNode shouldBe 50
    conf.connectTimeoutMillis shouldBe 500
    conf.socketTimeoutMillis shouldBe 500
    conf.readTotalTimeoutMillis shouldBe 7000
    conf.writeTotalTimeoutMillis shouldBe 7000
    conf.streamingMaxLookbackDays shouldBe 7
    conf.loaderBatchTimeoutSeconds shouldBe 120
  }

  it should "apply defaults for omitted optional settings" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts"     -> "localhost",
      "aerospike.namespace" -> "test"
    ))
    conf.port shouldBe AerospikeConfig.DefaultPort
    conf.batchTtl shouldBe TtlSetting.Seconds(AerospikeConfig.DefaultTtlSeconds.toInt)
    conf.streamingTtl shouldBe conf.batchTtl
    conf.keyPrefix shouldBe None
    conf.streamingMaxLookbackDays shouldBe AerospikeConfig.DefaultStreamingMaxLookbackDays
    conf.loaderBatchTimeoutSeconds shouldBe AerospikeConfig.DefaultLoaderBatchTimeoutSeconds
  }

  it should "fail fast with a descriptive error when aerospike.hosts is missing" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map("aerospike.namespace" -> "test"))
    }
    ex.getMessage should include("aerospike.hosts")
  }

  it should "fail fast when aerospike.hosts is empty or all commas" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        AerospikeConfig.HostsKey     -> ", ,",
        AerospikeConfig.NamespaceKey -> "test"
      ))
    }
    ex.getMessage should include("aerospike.hosts")
  }

  it should "fail fast with a descriptive error when aerospike.namespace is missing" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map("aerospike.hosts" -> "localhost"))
    }
    ex.getMessage should include("aerospike.namespace")
  }

  it should "include the offending key in non-numeric value errors" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"     -> "localhost",
        "aerospike.namespace" -> "test",
        "aerospike.port"      -> "not-a-number"
      ))
    }
    ex.getMessage should include("aerospike.port")
    ex.getMessage should include("not-a-number")
  }

  it should "fail fast when TTL is negative" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"       -> "localhost",
        "aerospike.namespace"   -> "test",
        "aerospike.ttl.seconds" -> "-5"
      ))
    }
    ex.getMessage should include("aerospike.ttl.seconds")
  }

  it should "fail fast when TTL exceeds Int.MaxValue" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"       -> "localhost",
        "aerospike.namespace"   -> "test",
        "aerospike.ttl.seconds" -> (Int.MaxValue.toLong + 1).toString
      ))
    }
    ex.getMessage should include("aerospike.ttl.seconds")
  }

  it should "fail fast when streaming max lookback is negative" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"                     -> "localhost",
        "aerospike.namespace"                 -> "test",
        "aerospike.streaming.maxLookbackDays" -> "-1"
      ))
    }
    ex.getMessage should include("aerospike.streaming.maxLookbackDays")
  }

  it should "fail fast when streaming max lookback exceeds TTL" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"                     -> "localhost",
        "aerospike.namespace"                 -> "test",
        "aerospike.ttl.seconds"               -> "3600",
        "aerospike.streaming.maxLookbackDays" -> "1"
      ))
    }
    ex.getMessage should include("aerospike.streaming.maxLookbackDays")
    ex.getMessage should include("aerospike.ttl.seconds")
  }

  it should "fail fast when loader batch timeout is non-positive" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"                          -> "localhost",
        "aerospike.namespace"                      -> "test",
        "aerospike.loader.batch.timeout.seconds" -> "0"
      ))
    }
    ex.getMessage should include("aerospike.loader.batch.timeout.seconds")
  }

  it should "leave maxCommandsInProcess unset by default and parse an override" in {
    val base = Map("aerospike.hosts" -> "localhost", "aerospike.namespace" -> "test")
    AerospikeConfig.fromUserConf(base).maxCommandsInProcess shouldBe None
    AerospikeConfig.fromUserConf(base + ("aerospike.maxCommandsInProcess" -> " 500 ")).maxCommandsInProcess shouldBe Some(500)
  }

  it should "reject maxCommandsInProcess below the client minimum or non-numeric" in {
    def bad(value: String): String = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"                -> "localhost",
        "aerospike.namespace"            -> "test",
        "aerospike.maxCommandsInProcess" -> value))
    }.getMessage
    bad("4") should include("aerospike.maxCommandsInProcess")
    bad("0") should include("aerospike.maxCommandsInProcess")
    bad("-3") should include("aerospike.maxCommandsInProcess")
    bad("many") should include("aerospike.maxCommandsInProcess")
    AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts" -> "localhost", "aerospike.namespace" -> "test",
      "aerospike.maxCommandsInProcess" -> "5")).maxCommandsInProcess shouldBe Some(5)
  }

  private val retentionBase = Map("aerospike.hosts" -> "localhost", "aerospike.namespace" -> "test")

  "retention config" should "default to 2 retained versions" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.retainVersions shouldBe AerospikeConfig.DefaultRetainVersions
    AerospikeConfig.DefaultRetainVersions shouldBe 2
  }

  it should "parse valid overrides" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.batch.retainVersions" -> " 5 "))
    conf.retainVersions shouldBe 5
  }

  it should "reject zero, negative and non-numeric values naming the key" in {
    for (key <- Seq("aerospike.batch.retainVersions");
         value <- Seq("0", "-1", "many")) {
      val ex = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase + (key -> value)))
      ex.getMessage should include(key)
    }
  }

  it should "not change the client cache key" in {
    val a = AerospikeConfig.fromUserConf(retentionBase)
    val b = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.batch.retainVersions" -> "9") + ("aerospike.stats.segmentTargetKiB" -> "90"))
    AerospikeClientHolder.clientCacheKey(a) shouldBe AerospikeClientHolder.clientCacheKey(b)
  }

  it should "no longer recognise aerospike.stats.retentionDays" in {
    classOf[AerospikeConfig].getDeclaredFields.map(_.getName) should not contain "statsRetentionDays"
    AerospikeConfig.fromUserConf(retentionBase + ("aerospike.stats.retentionDays" -> "7")) shouldBe a[AerospikeConfig]
  }

  it should "no longer recognise aerospike.stats.segmentMaxEntries" in {
    classOf[AerospikeConfig].getDeclaredFields.map(_.getName) should not contain "statsSegmentMaxEntries"
    AerospikeConfig.fromUserConf(retentionBase + ("aerospike.stats.segmentMaxEntries" -> "5")) shouldBe a[AerospikeConfig]
  }

  "segment config" should "default, accept overrides and reject non-positive or non-numeric values" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.segmentMaxEntries shouldBe 256
    conf.maxSegmentsPerDay shouldBe 1024
    val over = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.streaming.segmentMaxEntries" -> "50") + ("aerospike.streaming.maxSegmentsPerDay" -> "8"))
    over.segmentMaxEntries shouldBe 50
    over.maxSegmentsPerDay shouldBe 8
    for (key <- Seq("aerospike.streaming.segmentMaxEntries", "aerospike.streaming.maxSegmentsPerDay");
         value <- Seq("0", "-5", "lots")) {
      intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase + (key -> value)))
        .getMessage should include(key)
    }
    AerospikeClientHolder.clientCacheKey(conf) shouldBe AerospikeClientHolder.clientCacheKey(over)
  }

  "stats segment config" should "default to 512 KiB, accept overrides and reject non-positive or non-numeric values" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.statsSegmentTargetKiB shouldBe 512
    val over = AerospikeConfig.fromUserConf(retentionBase + ("aerospike.stats.segmentTargetKiB" -> "64"))
    over.statsSegmentTargetKiB shouldBe 64
    for (value <- Seq("0", "-5", "lots")) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + ("aerospike.stats.segmentTargetKiB" -> value)))
        .getMessage should include("aerospike.stats.segmentTargetKiB")
    }
    AerospikeClientHolder.clientCacheKey(conf) shouldBe AerospikeClientHolder.clientCacheKey(over)
  }

  "read chunk config" should "default to 1000, accept overrides and reject non-positive or non-numeric values" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.readBatchChunkSize shouldBe AerospikeConfig.DefaultReadBatchChunkSize
    AerospikeConfig.DefaultReadBatchChunkSize shouldBe 1000
    val over = AerospikeConfig.fromUserConf(retentionBase + ("aerospike.read.batchChunkSize" -> "250"))
    over.readBatchChunkSize shouldBe 250
    for (value <- Seq("0", "-5", "lots")) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + ("aerospike.read.batchChunkSize" -> value)))
        .getMessage should include("aerospike.read.batchChunkSize")
    }
    AerospikeClientHolder.clientCacheKey(conf) shouldBe AerospikeClientHolder.clientCacheKey(over)
  }

  "AerospikeClientHolder.eventLoopSizing" should "keep loops * perLoop within the total and perLoop >= 5" in {
    for (total <- Seq(5, 8, 9, 10, 99, 300, 1000, 5000); cpus <- Seq(1, 4, 10, 64)) {
      val (loops, perLoop) = AerospikeClientHolder.eventLoopSizing(total, cpus)
      loops should be >= 1
      loops should be <= cpus
      perLoop should be >= 5
      (loops * perLoop) should be <= total
      (total - loops * perLoop) should be < loops
    }
  }

  "AerospikeClientHolder.eventLoopSizing" should "use the whole total when it fits (no unnecessary cap)" in {
    AerospikeClientHolder.eventLoopSizing(300, 10) shouldBe (10, 30)
    AerospikeClientHolder.eventLoopSizing(8, 10) shouldBe (1, 8)
  }

  "AerospikeClientHolder.buildHolder" should "not let a failing close mask the original build error" in {
    var probed = 0
    AerospikeClientHolder.closeProbes = List(() => { probed += 1; throw new RuntimeException("close boom") })
    try {
      val socket = new java.net.ServerSocket(0)
      val port = socket.getLocalPort
      socket.close()
      val conf = AerospikeConfig.fromUserConf(Map(
        AerospikeConfig.HostsKey          -> "localhost",
        AerospikeConfig.PortKey           -> port.toString,
        AerospikeConfig.NamespaceKey      -> "t",
        AerospikeConfig.ConnectTimeoutKey -> "200"))
      val ex = intercept[Exception] { AerospikeClientHolder.clientFor(conf) }
      ex.getMessage should not include "close boom"
      probed shouldBe 1
    } finally AerospikeClientHolder.closeProbes = Nil
  }

  "AerospikeConfig.fromUserConf" should "fail fast when port, maxConnsPerNode, or timeouts are non-positive" in {
    def bad(key: String, value: String): Unit = {
      val ex = intercept[IllegalArgumentException] {
        AerospikeConfig.fromUserConf(Map(
          "aerospike.hosts"       -> "localhost",
          "aerospike.namespace"   -> "test",
          key                     -> value
        ))
      }
      ex.getMessage should include(key)
    }
    bad("aerospike.port", "0")
    bad("aerospike.maxConnsPerNode", "-1")
    bad("aerospike.timeout.connect.millis", "0")
    bad("aerospike.timeout.socket.millis", "-10")
    bad("aerospike.timeout.read.total.millis", "0")
    bad("aerospike.timeout.write.total.millis", "0")
  }

  "timeout and retry config" should "default to socket 250, read total 2000, write total 1000 and retries 2" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.socketTimeoutMillis shouldBe 250
    conf.readTotalTimeoutMillis shouldBe 2000
    conf.writeTotalTimeoutMillis shouldBe 1000
    conf.maxRetriesRead shouldBe 2
    conf.maxRetriesWrite shouldBe 2
  }

  it should "reject negative and non-numeric retries naming the key, and accept zero" in {
    for (key <- Seq("aerospike.maxRetries.read", "aerospike.maxRetries.write");
         value <- Seq("-1", "lots")) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + (key -> value))).getMessage should include(key)
    }
    val zero = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.maxRetries.read" -> "0") + ("aerospike.maxRetries.write" -> "0"))
    zero.maxRetriesRead shouldBe 0
    zero.maxRetriesWrite shouldBe 0
  }

  it should "fail when retries > 0 and socket is not below the per-class total, naming the keys" in {
    val readEx = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.timeout.socket.millis" -> "1000") +
      ("aerospike.timeout.read.total.millis" -> "1000"))).getMessage
    readEx should include("aerospike.timeout.socket.millis")
    readEx should include("aerospike.timeout.read.total.millis")
    readEx should include("aerospike.maxRetries.read")

    val writeEx = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.timeout.socket.millis" -> "1500") +
      ("aerospike.maxRetries.read" -> "0") +
      ("aerospike.timeout.write.total.millis" -> "1500"))).getMessage
    writeEx should include("aerospike.timeout.socket.millis")
    writeEx should include("aerospike.timeout.write.total.millis")
    writeEx should include("aerospike.maxRetries.write")
  }

  it should "allow socket >= total when that class has zero retries" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.timeout.socket.millis" -> "1000") +
      ("aerospike.timeout.read.total.millis" -> "1000") +
      ("aerospike.timeout.write.total.millis" -> "2000") +
      ("aerospike.maxRetries.read" -> "0"))
    conf.maxRetriesRead shouldBe 0
  }

  it should "no longer recognise a single aerospike.timeout.total.millis" in {
    classOf[AerospikeConfig].getDeclaredFields.map(_.getName) should not contain "totalTimeoutMillis"
    AerospikeConfig.fromUserConf(retentionBase + ("aerospike.timeout.total.millis" -> "7000")) shouldBe a[AerospikeConfig]
  }

  it should "change the client cache key when any timeout or retry field differs" in {
    val base = AerospikeConfig.fromUserConf(retentionBase)
    for (extra <- Seq(
        Map("aerospike.timeout.socket.millis" -> "300"),
        Map("aerospike.timeout.read.total.millis" -> "1100"),
        Map("aerospike.timeout.write.total.millis" -> "1100"),
        Map("aerospike.maxRetries.read" -> "1"),
        Map("aerospike.maxRetries.write" -> "0"))) {
      AerospikeClientHolder.clientCacheKey(AerospikeConfig.fromUserConf(retentionBase ++ extra)) should not be
        AerospikeClientHolder.clientCacheKey(base)
    }
    // data-shape keys still share the client
    AerospikeClientHolder.clientCacheKey(AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.batch.retainVersions" -> "9"))) shouldBe AerospikeClientHolder.clientCacheKey(base)
  }

  "AerospikeClientHolder.buildTemplates" should "apply per-class totals and retries to the write and batch templates" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.timeout.socket.millis"      -> "100",
      "aerospike.timeout.read.total.millis"  -> "400",
      "aerospike.timeout.write.total.millis" -> "900",
      "aerospike.maxRetries.read"            -> "3",
      "aerospike.maxRetries.write"           -> "0"))
    val t = AerospikeClientHolder.buildTemplates(conf)
    t.batch.connectTimeout shouldBe conf.connectTimeoutMillis
    t.batch.socketTimeout shouldBe 100
    t.batch.totalTimeout shouldBe 400
    t.batch.maxRetries shouldBe 3
    t.batch.sleepBetweenRetries shouldBe 0
    t.write.connectTimeout shouldBe conf.connectTimeoutMillis
    t.write.socketTimeout shouldBe 100
    t.write.totalTimeout shouldBe 900
    t.write.maxRetries shouldBe 0
    t.write.sleepBetweenRetries shouldBe 0
  }

  "AerospikeClientHolder.clientPolicyFor" should "set only the async pool from config and leave the sync pool at client defaults" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.minConnsPerNode" -> "5") + ("aerospike.maxConnsPerNode" -> "42"))
    val p = AerospikeClientHolder.clientPolicyFor(conf, null)
    p.asyncMinConnsPerNode shouldBe 5
    p.asyncMaxConnsPerNode shouldBe 42
    p.minConnsPerNode shouldBe 0
    p.maxConnsPerNode shouldBe new com.aerospike.client.policy.ClientPolicy().maxConnsPerNode
    p.timeout shouldBe conf.connectTimeoutMillis
  }

  "loader pool config" should "default to cap 32 and floor 0 and not change the client cache key" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.loaderMaxConnsPerNode shouldBe AerospikeConfig.DefaultLoaderMaxConnsPerNode
    AerospikeConfig.DefaultLoaderMaxConnsPerNode shouldBe 32
    conf.loaderMinConnsPerNode shouldBe AerospikeConfig.DefaultLoaderMinConnsPerNode
    AerospikeConfig.DefaultLoaderMinConnsPerNode shouldBe 0
    AerospikeClientHolder.clientCacheKey(conf) shouldBe
      AerospikeClientHolder.clientCacheKey(AerospikeConfig.fromUserConf(retentionBase +
        ("aerospike.loader.maxConnsPerNode" -> "64") +
        ("aerospike.loader.minConnsPerNode" -> "4")))
  }

  it should "reject a non-positive loader cap, a negative loader floor and floor above cap naming the keys" in {
    for ((key, value) <- Seq(
        "aerospike.loader.maxConnsPerNode" -> "0",
        "aerospike.loader.maxConnsPerNode" -> "many",
        "aerospike.loader.minConnsPerNode" -> "-1",
        "aerospike.loader.minConnsPerNode" -> "many")) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + (key -> value))).getMessage should include(key)
    }
    val ex = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.loader.maxConnsPerNode" -> "4") +
      ("aerospike.loader.minConnsPerNode" -> "8"))).getMessage
    ex should include("aerospike.loader.maxConnsPerNode")
    ex should include("aerospike.loader.minConnsPerNode")
  }

  "AerospikeConfig.forLoader" should "apply the loader pool bounds, bulk timeouts and retries, and clear the in-flight cap" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.maxConnsPerNode"        -> "300",
      "aerospike.minConnsPerNode"        -> "16",
      "aerospike.loader.maxConnsPerNode" -> "40",
      "aerospike.loader.minConnsPerNode" -> "2",
      "aerospike.timeout.socket.millis"  -> "250",
      "aerospike.maxRetries.write"       -> "2",
      "aerospike.maxCommandsInProcess"   -> "100"))
    val l = AerospikeConfig.forLoader(conf)
    l.maxConnsPerNode shouldBe 40
    l.minConnsPerNode shouldBe 2
    l.socketTimeoutMillis shouldBe AerospikeConfig.LoaderSocketTimeoutMs
    l.readTotalTimeoutMillis shouldBe AerospikeConfig.LoaderReadTotalTimeoutMs
    l.writeTotalTimeoutMillis shouldBe AerospikeConfig.LoaderWriteTotalTimeoutMs
    l.maxRetriesRead shouldBe AerospikeConfig.LoaderMaxRetriesRead
    l.maxRetriesWrite shouldBe AerospikeConfig.LoaderMaxRetriesWrite
    l.maxCommandsInProcess shouldBe None
    // serving values are not inherited
    l.maxConnsPerNode should not be conf.maxConnsPerNode
    l.socketTimeoutMillis should not be conf.socketTimeoutMillis
    // everything else is carried over
    l.namespace shouldBe conf.namespace
    l.batchTtl shouldBe conf.batchTtl
    l.streamingTtl shouldBe conf.streamingTtl
  }


  "TTL settings" should "parse positive seconds, 0 never-expire and namespace" in {
    def parse(m: Map[String, String]) = AerospikeConfig.fromUserConf(
      retentionBase ++ Map("aerospike.streaming.maxLookbackDays" -> "0") ++ m)
    parse(Map("aerospike.ttl.seconds" -> "3600")).batchTtl shouldBe TtlSetting.Seconds(3600)
    parse(Map("aerospike.ttl.seconds" -> "0")).batchTtl shouldBe TtlSetting.NeverExpire
    parse(Map("aerospike.ttl.seconds" -> "0")).streamingTtl shouldBe TtlSetting.NeverExpire
    parse(Map("aerospike.ttl.seconds" -> "namespace")).batchTtl shouldBe TtlSetting.NamespaceDefault
    parse(Map("aerospike.ttl.seconds" -> " Namespace ")).streamingTtl shouldBe TtlSetting.NamespaceDefault
    TtlSetting.Seconds(5).expiration shouldBe 5
    TtlSetting.NeverExpire.expiration shouldBe -1
    TtlSetting.NamespaceDefault.expiration shouldBe 0
  }

  it should "default both classes to ttl.seconds and allow per-class overrides" in {
    def parse(m: Map[String, String]) = AerospikeConfig.fromUserConf(
      retentionBase ++ Map("aerospike.streaming.maxLookbackDays" -> "0") ++ m)
    val both = parse(Map("aerospike.ttl.seconds" -> "600"))
    both.batchTtl shouldBe TtlSetting.Seconds(600)
    both.streamingTtl shouldBe TtlSetting.Seconds(600)
    val split = parse(Map(
      "aerospike.ttl.seconds"           -> "600",
      "aerospike.ttl.batch.seconds"     -> "1200",
      "aerospike.ttl.streaming.seconds" -> "0"))
    split.batchTtl shouldBe TtlSetting.Seconds(1200)
    split.streamingTtl shouldBe TtlSetting.NeverExpire
    val partial = parse(Map("aerospike.ttl.streaming.seconds" -> "namespace"))
    partial.batchTtl shouldBe TtlSetting.Seconds(AerospikeConfig.DefaultTtlSeconds.toInt)
    partial.streamingTtl shouldBe TtlSetting.NamespaceDefault
  }

  it should "reject garbage TTL values naming the key" in {
    for (key <- Seq("aerospike.ttl.seconds", "aerospike.ttl.batch.seconds", "aerospike.ttl.streaming.seconds");
         v <- Seq("many", "-1", (Int.MaxValue.toLong + 1).toString)) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + (key -> v))).getMessage should include(key)
    }
  }

  it should "validate the lookback against the streaming TTL only and skip it for never-expire" in {
    // streaming 0 (never expire): any lookback passes
    AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.seconds"             -> "3600",
      "aerospike.ttl.streaming.seconds"   -> "0",
      "aerospike.streaming.maxLookbackDays" -> "30"))
    // namespace mode: unresolvable at parse time, check deferred to build
    AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.streaming.seconds"   -> "namespace",
      "aerospike.streaming.maxLookbackDays" -> "30"))
    // batch TTL does not bound the lookback: tiny batch TTL still passes
    AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.batch.seconds"       -> "3600",
      "aerospike.streaming.maxLookbackDays" -> "4"))
    // streaming Seconds bounds it
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.streaming.seconds"   -> "3600",
      "aerospike.streaming.maxLookbackDays" -> "1"))).getMessage should include("aerospike.streaming.maxLookbackDays")
  }

  "AerospikeKvStore.expirationFor" should "map record classes to their TTL settings" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.streaming.maxLookbackDays" -> "0",
      "aerospike.ttl.batch.seconds"     -> "1200",
      "aerospike.ttl.streaming.seconds" -> "600"))
    AerospikeKvStore.expirationFor("my_gb_BATCH", "k".getBytes, conf) shouldBe 1200
    AerospikeKvStore.expirationFor("my_gb_STREAMING", "k".getBytes, conf) shouldBe 600
    AerospikeKvStore.expirationFor("my_meta", "k".getBytes, conf) shouldBe -1
    AerospikeKvStore.expirationFor("CHRONON_STATS_BATCH", "k".getBytes, conf) shouldBe -1
    AerospikeKvStore.expirationFor("my_gb_BATCH",
      ai.chronon.api.Constants.GroupByServingInfoKey.getBytes(ai.chronon.api.Constants.UTF8), conf) shouldBe -1
    val ns = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.streaming.maxLookbackDays" -> "0",
      "aerospike.ttl.batch.seconds" -> "namespace"))
    AerospikeKvStore.expirationFor("my_gb_BATCH", "k".getBytes, ns) shouldBe 0
    val nev = AerospikeConfig.fromUserConf(retentionBase + ("aerospike.ttl.streaming.seconds" -> "0"))
    AerospikeKvStore.expirationFor("my_gb_STREAMING", "k".getBytes, nev) shouldBe -1
  }

  "AerospikeKvStore TTL resolution" should "resolve namespace TTLs and the advance expiration" in {
    val ns = Some(AerospikeClientHolder.NamespaceSettings(Some(2), Some(false), Some(100)))
    AerospikeKvStore.resolvedTtlSeconds(TtlSetting.Seconds(5), ns) shouldBe Some(5L)
    AerospikeKvStore.resolvedTtlSeconds(TtlSetting.NeverExpire, ns) shouldBe None
    AerospikeKvStore.resolvedTtlSeconds(TtlSetting.NamespaceDefault, ns) shouldBe Some(100L)
    AerospikeKvStore.resolvedTtlSeconds(TtlSetting.NamespaceDefault,
      Some(AerospikeClientHolder.NamespaceSettings(Some(2), Some(false), Some(0)))) shouldBe None
    AerospikeKvStore.resolvedTtlSeconds(TtlSetting.NamespaceDefault, None) shouldBe None
    AerospikeKvStore.advanceExpiration(TtlSetting.Seconds(10), ns) shouldBe 10 + 86400
    AerospikeKvStore.advanceExpiration(TtlSetting.NeverExpire, ns) shouldBe -1
    AerospikeKvStore.advanceExpiration(TtlSetting.NamespaceDefault, ns) shouldBe 100 + 86400
    AerospikeKvStore.advanceExpiration(TtlSetting.NamespaceDefault, None) shouldBe -1
  }

  "AerospikeClientHolder.parseNamespaceInfo" should "parse the namespace info string tolerantly" in {
    val ns = AerospikeClientHolder.parseNamespaceInfo(
      "nsup-period=0;allow-ttl-without-nsup=false;default-ttl=86400;xdr=false").get
    ns.nsupPeriodSeconds shouldBe Some(0L)
    ns.allowTtlWithoutNsup shouldBe Some(false)
    ns.defaultTtlSeconds shouldBe Some(86400L)
    val partial = AerospikeClientHolder.parseNamespaceInfo("nsup-period=30;default-ttl=garbage").get
    partial.nsupPeriodSeconds shouldBe Some(30L)
    partial.allowTtlWithoutNsup shouldBe None
    partial.defaultTtlSeconds shouldBe None
    // a reply with no recognised keys is unresolved, not a real namespace
    AerospikeClientHolder.parseNamespaceInfo("") shouldBe None
    AerospikeClientHolder.parseNamespaceInfo("ERROR:4:unrecognized command") shouldBe None
    AerospikeClientHolder.parseNamespaceInfo("xdr=false;other=1") shouldBe None
  }

  "AerospikeClientHolder.checkNamespacePolicy" should "fail fast on nsup-period=0 without the allow flag" in {
    val positive = AerospikeConfig.fromUserConf(retentionBase)
    val noNsup = Some(AerospikeClientHolder.NamespaceSettings(Some(0), Some(false), Some(86400)))
    val ex = intercept[IllegalStateException](
      AerospikeClientHolder.checkNamespacePolicy(positive, noNsup))
    ex.getMessage should include("nsup-period")
    ex.getMessage should include("allow-ttl-without-nsup")
    // allow flag for testing
    AerospikeClientHolder.checkNamespacePolicy(positive,
      Some(AerospikeClientHolder.NamespaceSettings(Some(0), Some(true), Some(86400))))
    // nsup running
    AerospikeClientHolder.checkNamespacePolicy(positive,
      Some(AerospikeClientHolder.NamespaceSettings(Some(2), Some(false), Some(86400))))
    // unresolvable -> skip
    AerospikeClientHolder.checkNamespacePolicy(positive, None)
    // both classes never-expire: nothing written with a TTL
    val none = AerospikeConfig.fromUserConf(retentionBase + ("aerospike.ttl.seconds" -> "0"))
    AerospikeClientHolder.checkNamespacePolicy(none, noNsup)
    // namespace-default TTL resolved to 0 is never-expire
    val ns0 = AerospikeConfig.fromUserConf(retentionBase + ("aerospike.ttl.seconds" -> "namespace"))
    AerospikeClientHolder.checkNamespacePolicy(ns0,
      Some(AerospikeClientHolder.NamespaceSettings(Some(0), Some(false), Some(0))))
    // ... but resolved positive triggers the check
    intercept[IllegalStateException](
      AerospikeClientHolder.checkNamespacePolicy(ns0,
        Some(AerospikeClientHolder.NamespaceSettings(Some(0), Some(false), Some(100)))))
    // one class expiring is enough
    val batchOnly = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.batch.seconds" -> "3600", "aerospike.ttl.streaming.seconds" -> "0"))
    intercept[IllegalStateException](
      AerospikeClientHolder.checkNamespacePolicy(batchOnly, noNsup))
  }

  it should "validate the lookback against a resolved namespace TTL" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.ttl.streaming.seconds"   -> "namespace",
      "aerospike.streaming.maxLookbackDays" -> "5"))
    // resolves to 1 day -> lookback 5 exceeds it
    intercept[IllegalStateException](
      AerospikeClientHolder.checkNamespacePolicy(conf,
        Some(AerospikeClientHolder.NamespaceSettings(Some(2), Some(false), Some(86400)))))
    // resolves to never-expire -> skipped
    AerospikeClientHolder.checkNamespacePolicy(conf,
      Some(AerospikeClientHolder.NamespaceSettings(Some(2), Some(false), Some(0))))
  }

  "stats read timeout" should "default to 10000 ms, accept an override and reject non-positive values naming the key" in {
    AerospikeConfig.DefaultStatsReadTotalTimeoutMs shouldBe 10000
    AerospikeConfig.fromUserConf(retentionBase).statsReadTotalTimeoutMillis shouldBe 10000
    AerospikeConfig.fromUserConf(retentionBase + ("aerospike.timeout.stats.read.total.millis" -> " 30000 "))
      .statsReadTotalTimeoutMillis shouldBe 30000
    for (v <- Seq("0", "-1", "many")) {
      intercept[IllegalArgumentException](
        AerospikeConfig.fromUserConf(retentionBase + ("aerospike.timeout.stats.read.total.millis" -> v)))
        .getMessage should include("aerospike.timeout.stats.read.total.millis")
    }
  }

  it should "require the socket timeout to stay below it while read retries are on" in {
    val msg = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.timeout.socket.millis" -> "500",
      "aerospike.timeout.read.total.millis" -> "2000",
      "aerospike.timeout.stats.read.total.millis" -> "500"))).getMessage
    msg should include("aerospike.timeout.stats.read.total.millis")
    msg should include("aerospike.timeout.socket.millis")
    AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.timeout.socket.millis" -> "500",
      "aerospike.timeout.read.total.millis" -> "2000",
      "aerospike.timeout.stats.read.total.millis" -> "500",
      "aerospike.maxRetries.read" -> "0"))
  }

  it should "give stats batch reads their own policy template and a separate client cache key" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.timeout.read.total.millis" -> "1500",
      "aerospike.timeout.stats.read.total.millis" -> "7000"))
    val templates = AerospikeClientHolder.buildTemplates(conf)
    templates.batch.totalTimeout shouldBe 1500
    templates.statsBatch.totalTimeout shouldBe 7000
    templates.statsBatch.socketTimeout shouldBe templates.batch.socketTimeout
    templates.statsBatch.maxRetries shouldBe templates.batch.maxRetries
    val other = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.timeout.read.total.millis" -> "1500",
      "aerospike.timeout.stats.read.total.millis" -> "8000"))
    AerospikeClientHolder.clientCacheKey(other) should not be AerospikeClientHolder.clientCacheKey(conf)
  }

  // --- TLS / auth configuration ---

  private val tlsBase = retentionBase ++ Map(
    "aerospike.tls.enabled"                -> "true",
    "aerospike.tls.truststore.file"        -> "/tls/ca.truststore.p12",
    "aerospike.tls.truststore.password.file" -> "/tls/storepass.txt",
    "aerospike.tls.truststore.type"        -> "PKCS12")

  "TLS config" should "parse an enabled config with stores, name, protocols and ciphers" in {
    val conf = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.tls.name"       -> "aerospike-tls",
      "aerospike.tls.protocols"  -> "TLSv1.2, TLSv1.3",
      "aerospike.tls.ciphers"    -> "TLS_AES_256_GCM_SHA384",
      "aerospike.tls.keystore.file"            -> "/tls/client.keystore.p12",
      "aerospike.tls.keystore.password.file"   -> "/tls/storepass.txt",
      "aerospike.tls.keystore.type"            -> "pkcs12"))
    val t = conf.tls.get
    t.tlsName shouldBe Some("aerospike-tls")
    t.protocols shouldBe Some(Seq("TLSv1.2", "TLSv1.3"))
    t.ciphers shouldBe Some(Seq("TLS_AES_256_GCM_SHA384"))
    t.truststoreType shouldBe "PKCS12"
    t.keystoreFile shouldBe Some("/tls/client.keystore.p12")
    t.keystoreType shouldBe "PKCS12"
    t.forLoginOnly shouldBe false
  }

  it should "be absent by default and reject any aerospike.tls.* key without tls.enabled" in {
    AerospikeConfig.fromUserConf(retentionBase).tls shouldBe None
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.tls.name" -> "aerospike-tls"))).getMessage should include("aerospike.tls.enabled")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.tls.enabled" -> "false",
      "aerospike.tls.ciphers" -> "X"))).getMessage should include("aerospike.tls.enabled")
  }

  it should "reject a non-boolean tls.enabled naming the key" in {
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.tls.enabled" -> "yes"))).getMessage should include("aerospike.tls.enabled")
  }

  it should "reject empty protocol/cipher lists and unknown store types naming the key" in {
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.tls.protocols" -> " , ,"))).getMessage should include("aerospike.tls.protocols")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.tls.truststore.type" -> "PEM"))).getMessage should include("aerospike.tls.truststore.type")
  }

  it should "require a keystore password file with the keystore and reject orphaned store password keys" in {
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.tls.keystore.file" -> "/tls/client.keystore.p12"))).getMessage should
      include("aerospike.tls.keystore.password.file")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.tls.keystore.password.file" -> "/tls/storepass.txt"))).getMessage should
      include("aerospike.tls.keystore.file")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.tls.enabled" -> "true",
      "aerospike.tls.truststore.password.file" -> "/tls/storepass.txt"))).getMessage should
      include("aerospike.tls.truststore.file")
  }

  it should "allow tls.enabled with no stores (default JVM SSLContext)" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.tls.enabled" -> "true") + ("aerospike.tls.name" -> "aerospike-tls"))
    conf.tls.get.truststoreFile shouldBe None
  }

  "seed hosts" should "parse host[:tlsName][:port] and fill the default port" in {
    val conf = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.hosts" -> "node1:aerospike-tls:4333,node2,node3:4334"))
    conf.hosts.map(h => (h.name, h.tlsName, h.port)) shouldBe Seq(
      ("node1", Some("aerospike-tls"), 4333),
      ("node2", None, 3000),
      ("node3", None, 4334))
  }

  it should "reject a tlsName seed while TLS is disabled" in {
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.hosts" -> "node1:aerospike-tls:4333"))).getMessage should include("aerospike.hosts")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.hosts" -> "node1:aerospike-tls"))).getMessage should include("aerospike.hosts")
  }

  "auth config" should "default to internal, pair-validate credentials and accept a password file or env var" in {
    val conf = AerospikeConfig.fromUserConf(retentionBase)
    conf.authMode shouldBe AuthModeSetting.Internal
    conf.credentials shouldBe None

    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.user" -> "u"))).getMessage should include("aerospike.user")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.password.env" -> "PW"))).getMessage should include("aerospike.user")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.user" -> "u", "aerospike.password.env" -> "PW",
      "aerospike.password.file" -> "/pw"))).getMessage should include("mutually exclusive")

    val c = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.user" -> "u", "aerospike.password.file" -> "/pw"))
    c.credentials shouldBe Some(Credentials("u", Some("/pw"), None))
    val e = AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.user" -> "u", "aerospike.password.env" -> " AS_PW "))
    e.credentials shouldBe Some(Credentials("u", None, Some("AS_PW")))
  }

  it should "reject the removed inline aerospike.password key instead of silently ignoring it" in {
    val ex = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.user" -> "u", "aerospike.password" -> "p")))
    ex.getMessage should include("aerospike.password")
    ex.getMessage should include("aerospike.password.file")
    ex.getMessage should include("aerospike.password.env")
    ex.getMessage should not include "\"p\""
  }

  it should "reject unknown auth.mode naming the key and value" in {
    val ex = intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.auth.mode" -> "kerberos")))
    ex.getMessage should include("aerospike.auth.mode")
    ex.getMessage should include("kerberos")
  }

  it should "enforce the per-mode TLS and credential rules" in {
    // pki needs TLS + keystore, forbids credentials
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.auth.mode" -> "pki"))).getMessage should include("aerospike.auth.mode")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.auth.mode" -> "pki"))).getMessage should include("aerospike.tls.keystore.file")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.auth.mode" -> "pki",
      "aerospike.tls.keystore.file" -> "/tls/client.keystore.p12",
      "aerospike.tls.keystore.password.file" -> "/tls/storepass.txt",
      "aerospike.user" -> "u", "aerospike.password.env" -> "PW"))).getMessage should
      include("aerospike.auth.mode")
    val pki = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.auth.mode" -> "pki",
      "aerospike.tls.keystore.file" -> "/tls/client.keystore.p12",
      "aerospike.tls.keystore.password.file" -> "/tls/storepass.txt"))
    pki.authMode shouldBe AuthModeSetting.Pki

    // external needs TLS + credentials; external_insecure needs credentials only
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase ++ Map(
      "aerospike.auth.mode" -> "external",
      "aerospike.user" -> "u", "aerospike.password.env" -> "PW"))).getMessage should
      include("aerospike.tls.enabled")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.auth.mode" -> "external"))).getMessage should include("aerospike.user")
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(retentionBase +
      ("aerospike.auth.mode" -> "external_insecure"))).getMessage should include("aerospike.user")
  }

  "forLoginOnly" should "require credentials" in {
    intercept[IllegalArgumentException](AerospikeConfig.fromUserConf(tlsBase +
      ("aerospike.tls.forLoginOnly" -> "true"))).getMessage should
      include("aerospike.tls.forLoginOnly")
    val conf = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.tls.forLoginOnly" -> "true",
      "aerospike.user" -> "u", "aerospike.password.file" -> "/pw"))
    conf.tls.get.forLoginOnly shouldBe true
  }

  "clientCacheKey" should "change on any TLS or auth identity difference, including the password source" in {
    val plain = AerospikeConfig.fromUserConf(retentionBase)
    val tlsConf = AerospikeConfig.fromUserConf(tlsBase)
    val key = AerospikeClientHolder.clientCacheKey _
    key(tlsConf) should not be key(plain)
    for (extra <- Seq(
        Map("aerospike.tls.name" -> "other"),
        Map("aerospike.tls.ciphers" -> "X"),
        Map("aerospike.tls.truststore.file" -> "/other.p12"),
        Map("aerospike.hosts" -> "localhost:aerospike-tls:4333"),
        Map("aerospike.user" -> "u", "aerospike.password.env" -> "PW"),
        Map("aerospike.auth.mode" -> "pki",
            "aerospike.tls.keystore.file" -> "/k.p12",
            "aerospike.tls.keystore.password.file" -> "/pw"))) {
      key(AerospikeConfig.fromUserConf(tlsBase ++ extra)) should not be key(tlsConf)
    }
    // credentials are references: a different file path or env var NAME is a
    // different credential and gets its own client; the same source is shared
    def withPw(kv: (String, String)) =
      AerospikeConfig.fromUserConf(tlsBase ++ Map("aerospike.user" -> "u") + kv)
    key(withPw("aerospike.password.env" -> "PW_A")) shouldBe key(withPw("aerospike.password.env" -> "PW_A"))
    key(withPw("aerospike.password.env" -> "PW_A")) should not be key(withPw("aerospike.password.env" -> "PW_B"))
    key(withPw("aerospike.password.file" -> "/pw")) should not be key(withPw("aerospike.password.file" -> "/other-pw"))
    key(withPw("aerospike.password.file" -> "/pw")) should not be key(withPw("aerospike.password.env" -> "/pw"))
    // different user is a different client
    val v = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.user" -> "v", "aerospike.password.env" -> "PW_A"))
    key(withPw("aerospike.password.env" -> "PW_A")) should not be key(v)
    // keystore/truststore password-file paths are identity too
    key(AerospikeConfig.fromUserConf(tlsBase ++ Map("aerospike.tls.truststore.file" -> "/t.p12",
      "aerospike.tls.truststore.password.file" -> "/a"))) should not be
      key(AerospikeConfig.fromUserConf(tlsBase ++ Map("aerospike.tls.truststore.file" -> "/t.p12",
        "aerospike.tls.truststore.password.file" -> "/b")))
  }

  "AerospikeConfig.forLoader" should "carry TLS and auth fields unchanged" in {
    val conf = AerospikeConfig.fromUserConf(tlsBase ++ Map(
      "aerospike.user" -> "u", "aerospike.password.file" -> "/pw"))
    val l = AerospikeConfig.forLoader(conf)
    l.tls shouldBe conf.tls
    l.authMode shouldBe conf.authMode
    l.credentials shouldBe conf.credentials
  }

  /** A temp dir of loadable throwaway PKCS12 stores + a password file. The
    * wiring under test only requires the stores to load — real cert material
    * (and PKI auth) is exercised by the src/it TLS fixture, not here. */
  private def fixtureStoresDir(): String = {
    val dir = java.nio.file.Files.createTempDirectory("chronon-tls-stores")
    val pw = "storepass".toCharArray
    Seq("ca.truststore.p12", "client.keystore.p12").foreach { name =>
      val ks = java.security.KeyStore.getInstance("PKCS12")
      ks.load(null, pw)
      val out = java.nio.file.Files.newOutputStream(dir.resolve(name))
      try ks.store(out, pw) finally out.close()
    }
    java.nio.file.Files.write(dir.resolve("storepass.txt"),
      "storepass".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    dir.toString
  }

  "clientPolicyFor" should "wire tlsPolicy, clusterName, authMode and credentials" in {
    val certs = fixtureStoresDir()
    val realStores = retentionBase ++ Map(
      "aerospike.tls.enabled" -> "true",
      "aerospike.tls.truststore.file" -> s"$certs/ca.truststore.p12",
      "aerospike.tls.truststore.password.file" -> s"$certs/storepass.txt",
      "aerospike.tls.truststore.type" -> "PKCS12")
    val conf = AerospikeConfig.fromUserConf(realStores ++ Map(
      "aerospike.tls.name" -> "aerospike-tls",
      "aerospike.tls.protocols" -> "TLSv1.2,TLSv1.3",
      "aerospike.user" -> "u", "aerospike.password.file" -> s"$certs/storepass.txt"))
    val p = AerospikeClientHolder.clientPolicyFor(conf, null)
    p.tlsPolicy should not be null
    p.tlsPolicy.context should not be null // truststore configured -> explicit context
    p.tlsPolicy.protocols shouldBe Array("TLSv1.2", "TLSv1.3")
    p.tlsPolicy.forLoginOnly shouldBe false
    // clusterName stays unset — it would enable cluster-name validation;
    // the global TLS name lands on the seeds instead
    p.clusterName shouldBe null
    AerospikeClientHolder.seedsFor(conf).map(h => h.tlsName) shouldBe Seq("aerospike-tls")
    p.user shouldBe "u"
    p.password shouldBe "storepass" // read from the password file at build
    p.authMode shouldBe com.aerospike.client.policy.AuthMode.INTERNAL

    val noStore = AerospikeClientHolder.clientPolicyFor(
      AerospikeConfig.fromUserConf(retentionBase + ("aerospike.tls.enabled" -> "true")), null)
    noStore.tlsPolicy.context shouldBe null // default JVM SSLContext

    val pki = AerospikeClientHolder.clientPolicyFor(AerospikeConfig.fromUserConf(realStores ++ Map(
      "aerospike.auth.mode" -> "pki",
      "aerospike.tls.keystore.file" -> s"$certs/client.keystore.p12",
      "aerospike.tls.keystore.password.file" -> s"$certs/storepass.txt",
      "aerospike.tls.keystore.type" -> "PKCS12")), null)
    pki.authMode shouldBe com.aerospike.client.policy.AuthMode.PKI
    pki.user shouldBe null
  }

  "resolvePassword" should "read the env var named by the config, failing closed when unset or blank" in {
    val c = Credentials("u", None, Some("AS_PW"))
    AerospikeClientHolder.resolvePassword(c, Map("AS_PW" -> "  s3cret \n").get) shouldBe "s3cret"
    val unset = intercept[IllegalStateException](AerospikeClientHolder.resolvePassword(c, _ => None))
    unset.getMessage should include("AS_PW")
    intercept[IllegalStateException](AerospikeClientHolder.resolvePassword(c, Map("AS_PW" -> "  ").get))
  }

  it should "prefer neither source silently: a file reference reads the file, not the env" in {
    val pw = java.nio.file.Files.createTempFile("pw", ".txt")
    pw.toFile.deleteOnExit()
    java.nio.file.Files.write(pw, "from-file\n".getBytes("UTF-8"))
    AerospikeClientHolder.resolvePassword(Credentials("u", Some(pw.toString), None), _ => Some("from-env")) shouldBe "from-file"
  }

  it should "name the config key and path when the password file is unreadable" in {
    val ex = intercept[IllegalStateException](
      AerospikeClientHolder.resolvePassword(Credentials("u", Some("/no/such/pw.txt"), None), _ => None))
    ex.getMessage should include("aerospike.password.file")
    ex.getMessage should include("/no/such/pw.txt")
  }
}
