package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.api.PartitionSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Spark2AerospikeLoaderSpec extends AnyFlatSpec with Matchers {

  private val daily = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L)

  "datasetForTable" should "route *_logged_daily_stats_upload tables to the log-stats dataset" in {
    Spark2AerospikeLoader.datasetForTable("team.my_join_logged_daily_stats_upload") shouldBe
      ApiConstants.LogStatsBatchDataset
  }

  it should "route *_daily_stats_upload tables to the stats dataset" in {
    Spark2AerospikeLoader.datasetForTable("team.my_join_daily_stats_upload") shouldBe
      ApiConstants.StatsBatchDataset
  }

  it should "route *_consistency_upload tables to the consistency dataset" in {
    Spark2AerospikeLoader.datasetForTable("team.my_join_consistency_upload") shouldBe
      ApiConstants.ConsistencyMetricsDataset
  }

  it should "route *_upload tables to <BASENAME>_BATCH" in {
    Spark2AerospikeLoader.datasetForTable("team.my_gb_upload") shouldBe "MY_GB_BATCH"
  }

  it should "route any other table to <NAME>_BATCH (upstream convention)" in {
    Spark2AerospikeLoader.datasetForTable("team.some_table") shouldBe "TEAM.SOME_TABLE_BATCH"
  }

  "versionTsMillis" should "equal epochMillis(ds) + spanMillis (fetcher batchEndTsMillis)" in {
    val ds = "2026-10-13"
    Spark2AerospikeLoader.versionTsMillis(daily, ds) shouldBe
      daily.epochMillis(ds) + daily.spanMillis
    Spark2AerospikeLoader.versionTsMillis(daily, ds) shouldBe daily.epochMillis("2026-10-14")
  }
}
