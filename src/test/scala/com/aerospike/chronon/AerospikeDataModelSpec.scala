package com.aerospike.chronon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8

class AerospikeDataModelSpec extends AnyFlatSpec with Matchers {

  import AerospikeDataModel._

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  "getTableType" should "map *_BATCH datasets to the shared batch table" in {
    getTableType("MY_GB_BATCH") shouldBe BatchTable
    getTableType("CHRONON_STATS_BATCH") shouldBe BatchTable
  }

  it should "map *_STREAMING datasets to the shared streaming table" in {
    getTableType("MY_GB_STREAMING") shouldBe StreamingTable
  }

  it should "map any other dataset to a metadata table named by the dataset" in {
    getTableType("ZIPLINE_METADATA") shouldBe MetadataTable("ZIPLINE_METADATA")
    getTableType("ConfByKeyEndPointName") shouldBe MetadataTable("ConfByKeyEndPointName")
  }

  "physicalSetName" should "return GROUPBY_BATCH / GROUPBY_STREAMING for groupBy datasets" in {
    physicalSetName("MY_GB_BATCH") shouldBe "GROUPBY_BATCH"
    physicalSetName("MY_GB_STREAMING") shouldBe "GROUPBY_STREAMING"
  }

  it should "return the dataset name itself for metadata datasets" in {
    physicalSetName("ZIPLINE_METADATA") shouldBe "ZIPLINE_METADATA"
  }

  "recordKey" should "build DATASET#keyBytes as raw bytes" in {
    recordKey("MY_GB_BATCH", bytes("user|123")) shouldBe bytes("MY_GB_BATCH#user|123")
  }

  it should "prepend the configured key prefix" in {
    recordKey("MY_GB_BATCH", bytes("k"), Some("ns1:")) shouldBe bytes("ns1:MY_GB_BATCH#k")
  }

  it should "not collide distinct binary keyBytes (no UTF-8 String round-trip)" in {
    // 0xFF is not valid UTF-8; both of these decode to the same replacement-char
    // String, so a String-based key would alias two entities to one record
    val k1 = Array[Byte](0xFF.toByte, 0x01)
    val k2 = Array[Byte](0xFF.toByte, 0x02)
    recordKey("MY_GB_BATCH", k1) should not be recordKey("MY_GB_BATCH", k2)
    recordKey("MY_GB_BATCH", k1) shouldBe ("MY_GB_BATCH#".getBytes(UTF_8) ++ Array[Byte](0xFF.toByte, 0x01))
  }

  "dayTs" should "floor a timestamp to start-of-day millis" in {
    dayTs(86400000L + 1234L) shouldBe 86400000L
    dayTs(0L) shouldBe 0L
    dayTs(-1L) shouldBe -86400000L
    dayTs(-86400000L) shouldBe -86400000L
  }

  "streamingRecordKey" should "append the day bucket derived from tsMillis" in {
    val ts = 86400000L + 999L // second day
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), ts) shouldBe bytes("MY_GB_STREAMING#k#86400000")
  }

  it should "prepend the configured key prefix" in {
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), 1L, Some("ns1:")) shouldBe bytes("ns1:MY_GB_STREAMING#k#0")
  }

  "streamingRecordKeyFromDay" should "use the supplied day bucket directly" in {
    streamingRecordKeyFromDay("MY_GB_STREAMING", bytes("k"), 86400000L) shouldBe
      bytes("MY_GB_STREAMING#k#86400000")
    streamingRecordKeyFromDay("MY_GB_STREAMING", bytes("k"), 86400000L, Some("ns1:")) shouldBe
      bytes("ns1:MY_GB_STREAMING#k#86400000")
  }
}
