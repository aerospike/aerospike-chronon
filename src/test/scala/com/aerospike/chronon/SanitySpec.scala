package com.aerospike.chronon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanitySpec extends AnyFlatSpec with Matchers {
  "Default AerospikeConfig" should "be self-consistent (max lookback <= TTL)" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      AerospikeConfig.HostsKey     -> "localhost",
      AerospikeConfig.NamespaceKey -> "test"
    ))
    conf.ttlSeconds should be > 0L
    conf.streamingMaxLookbackDays.toLong * 86400000L should be <= conf.ttlSeconds * 1000L
  }

  "Aerospike Java client" should "be present on the classpath" in {
    val clientClass = Class.forName("com.aerospike.client.AerospikeClient")
    clientClass should not be null
  }
}
