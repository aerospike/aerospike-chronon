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
      "aerospike.timeout.socket.millis"       -> "5000",
      "aerospike.timeout.total.millis"        -> "7000",
      "aerospike.key.prefix"                  -> "team1:",
      "aerospike.streaming.maxLookbackDays"   -> "7",
      "aerospike.loader.batch.timeout.seconds" -> "120"
    ))
    conf.hosts shouldBe Seq("a", "b")
    conf.port shouldBe 3100
    conf.namespace shouldBe "features"
    conf.ttlSeconds shouldBe 864000L
    conf.maxConnsPerNode shouldBe 50
    conf.connectTimeoutMillis shouldBe 500
    conf.socketTimeoutMillis shouldBe 5000
    conf.totalTimeoutMillis shouldBe 7000
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
      "aerospike.timeout.socket.millis"        -> " 5000 ",
      "aerospike.timeout.total.millis"         -> " 7000 ",
      "aerospike.streaming.maxLookbackDays"    -> " 7 ",
      "aerospike.loader.batch.timeout.seconds" -> " 120 "
    ))
    conf.port shouldBe 3100
    conf.ttlSeconds shouldBe 864000L
    conf.maxConnsPerNode shouldBe 50
    conf.connectTimeoutMillis shouldBe 500
    conf.socketTimeoutMillis shouldBe 5000
    conf.totalTimeoutMillis shouldBe 7000
    conf.streamingMaxLookbackDays shouldBe 7
    conf.loaderBatchTimeoutSeconds shouldBe 120
  }

  it should "apply defaults for omitted optional settings" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts"     -> "localhost",
      "aerospike.namespace" -> "test"
    ))
    conf.port shouldBe AerospikeConfig.DefaultPort
    conf.ttlSeconds shouldBe AerospikeConfig.DefaultTtlSeconds
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

  it should "fail fast when TTL is non-positive" in {
    val ex = intercept[IllegalArgumentException] {
      AerospikeConfig.fromUserConf(Map(
        "aerospike.hosts"       -> "localhost",
        "aerospike.namespace"   -> "test",
        "aerospike.ttl.seconds" -> "0"
      ))
    }
    ex.getMessage should include("aerospike.ttl.seconds")
    ex.getMessage should include("positive")
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
    bad("aerospike.timeout.total.millis", "0")
  }
}
