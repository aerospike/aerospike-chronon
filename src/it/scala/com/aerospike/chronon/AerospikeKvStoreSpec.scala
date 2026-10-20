package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.aerospike.chronon.AerospikeDataModel.{MillisPerDay, dayTs}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class AerospikeKvStoreSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private val timeout = 30.seconds

  private def put(dataset: String, key: String, value: String, ts: Long): PutRequest =
    PutRequest(bytes(key), bytes(value), dataset, Some(ts))

  private def get(dataset: String, key: String, afterTs: Option[Long] = None): GetRequest =
    GetRequest(bytes(key), dataset, afterTs)

  // 3.1 — round trip
  "multiPut/multiGet" should "round-trip a TimedValue" in {
    val store = newStore()
    val ds = "ROUNDTRIP_GB_BATCH"
    Await.result(store.multiPut(Seq(put(ds, "k1", "v1", 100L))), timeout)
    val responses = Await.result(store.multiGet(Seq(get(ds, "k1"))), timeout)
    responses.head.values shouldBe a[Success[_]]
    val values = responses.head.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "v1"
    values.head.millis shouldBe 100L
  }

  // 3.3 — version filter
  it should "return only versions with ts >= afterTsMillis, in ts order" in {
    val store = newStore()
    val ds = "FILTER_GB_BATCH"
    val writes = Seq(put(ds, "k", "v100", 100L), put(ds, "k", "v200", 200L), put(ds, "k", "v300", 300L))
    Await.result(store.multiPut(writes), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k", Some(200L)))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(200L, 300L)
    values.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("v200", "v300")
  }

  // 3.4 — LWW same-ts rewrite
  it should "overwrite same-ts entries (last-write-wins, idempotent)" in {
    val store = newStore()
    val ds = "LWW_GB_BATCH"
    Await.result(store.multiPut(Seq(put(ds, "k", "v1", 100L))), timeout)
    Await.result(store.multiPut(Seq(put(ds, "k", "v2", 100L))), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "v2"
  }

  // spec scenario — two groupBys share GROUPBY_BATCH under equal keyBytes
  it should "keep same-keyBytes records from different datasets disjoint" in {
    val store = newStore()
    Await.result(store.multiPut(Seq(
      put("GB_ONE_BATCH", "shared-key", "v-one", 100L),
      put("GB_TWO_BATCH", "shared-key", "v-two", 100L))), timeout)
    val resps = Await.result(store.multiGet(Seq(
      get("GB_ONE_BATCH", "shared-key"), get("GB_TWO_BATCH", "shared-key"))), timeout)
    new String(resps(0).values.get.head.bytes, UTF_8) shouldBe "v-one"
    new String(resps(1).values.get.head.bytes, UTF_8) shouldBe "v-two"
  }

  // 3.6 — day-spanning streaming read
  it should "merge tiles across day-bucketed streaming records" in {
    val store = newStore()
    val ds = "SPAN_GB_STREAMING"
    // Use timestamps relative to now so the configurable lookback window includes them.
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(ds, "k", "tileA", day1 + 100L),
      put(ds, "k", "tileB", day2 + 200L),
      put(ds, "k", "tileC", day2 + 300L))), timeout)
    // afterTsMillis inside day 1 -> tiles from both day records, ts-ordered
    val values = Await.result(store.multiGet(Seq(get(ds, "k", Some(day1 + 50L)))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(day1 + 100L, day2 + 200L, day2 + 300L)
    values.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("tileA", "tileB", "tileC")
  }

  it should "return the full lookback window for streaming reads without afterTsMillis" in {
    val store = newStore()
    val ds = "NOAFTER_GB_STREAMING"
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(ds, "k", "tileA", day1 + 100L),
      put(ds, "k", "tileB", day2 + 200L))), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(day1 + 100L, day2 + 200L)
  }

  it should "answer duplicate requests in one batch independently" in {
    val store = newStore()
    val ds = "DUP_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    Await.result(store.multiPut(Seq(put(ds, "k", "t", day + 100L))), timeout)
    val req = get(ds, "k")
    val resps = Await.result(store.multiGet(Seq(req, req, get(ds, "other"))), timeout)
    resps should have size 3
    resps(0).values.get.map(_.millis) shouldBe Seq(day + 100L)
    resps(1).values.get.map(_.millis) shouldBe Seq(day + 100L)
    resps(2).values shouldBe Success(Seq.empty)
  }

  // 3.8 — partial failure + absent key contract
  it should "return false only at the rejected index and never fail the Future" in {
    val store = newStore()
    val ds = "PARTIAL_GB_BATCH"
    // a request the server must reject: null value bytes
    val results = Await.result(store.multiPut(Seq(
      put(ds, "good", "v", 100L),
      PutRequest(bytes("bad"), null, ds, Some(100L)),
      put(ds, "good2", "v", 100L))), timeout)
    results shouldBe Seq(true, false, true)
  }

  it should "return Success(Seq.empty) for a missing record" in {
    val store = newStore()
    val resp = Await.result(store.multiGet(Seq(get("MISSING_GB_BATCH", "nope"))), timeout).head
    resp.values shouldBe Success(Seq.empty)
  }

  it should "surface per-request Failures in GetResponse.values, never a failed Future" in {
    val store = newStore()
    // >63-char set name is rejected by the server — a real per-request error path
    val badDs = "X" * 80
    val resps = Await.result(
      store.multiGet(Seq(get("OK_GB_BATCH", "k"), get(badDs, "k2"))), timeout)
    resps should have size 2
    resps(0).values shouldBe Success(Seq.empty)
    resps(1).values shouldBe a[Failure[_]]
    resps(1).request.dataset shouldBe badDs
  }

  // 3.9a — concurrency / shared client
  it should "complete 100 concurrent multiGets through one shared client" in {
    val store1 = newStore()
    val store2 = newStore()
    val before = AerospikeClientHolder.constructionCount.get()
    val futures = (1 to 100).map(i => store1.multiGet(Seq(get("CONC_GB_BATCH", s"k$i"))))
    val all = Await.result(scala.concurrent.Future.sequence(futures), timeout)
    all should have size 100
    AerospikeClientHolder.constructionCount.get() - before should be <= 1
    store1 should not be null
    store2 should not be null
  }

  // pool exhaustion (NO_MORE_CONNECTIONS, -7) must be absorbed by client-side queueing
  it should "queue a burst larger than the connection pool instead of failing writes" in {
    val store = newStore(testUserConf + (AerospikeConfig.MaxConnsPerNodeKey -> "8", AerospikeConfig.MinConnsPerNodeKey -> "0"))
    val ds = "BURST_GB_BATCH"
    val burst = (1 to 2000).map(i => put(ds, s"k$i", "v", 100L))
    val results = Await.result(store.multiPut(burst), timeout)
    results.count(!_) shouldBe 0
    val resp = Await.result(store.multiGet(Seq(get(ds, "k1"), get(ds, "k2000"))), timeout)
    resp.map(_.values.get.size) shouldBe Seq(1, 1)
  }

  // 3.10 — TTL exception for group_by_serving_info
  it should "keep group_by_serving_info alive while normal batch rows expire" in {
    // enabled once a short-TTL namespace config is wired; requires nsup-period > 0
    val store = newStore(testUserConf +
      (AerospikeConfig.TtlSecondsKey -> "5") +
      (AerospikeConfig.StreamingMaxLookbackDaysKey -> "0"))
    val ds = "TTL_GB_BATCH"
    Await.result(store.multiPut(Seq(
      put(ds, "k", "v", 100L),
      put(ds, ApiConstants.GroupByServingInfoKey, "serving-info", 100L))), timeout)

    // poll for expiry instead of a fixed long sleep
    val deadline = System.currentTimeMillis() + 20000
    var expired = false
    while (System.currentTimeMillis() < deadline && !expired) {
      val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
      expired = values.isEmpty
      if (!expired) Thread.sleep(500)
    }
    expired shouldBe true

    store.getString(ApiConstants.GroupByServingInfoKey, ds, timeout.toMillis) shouldBe a[Success[_]]
  }

  // 3.12 — push mode contract guard (passes via trait default)
  "multiPutWithNotification" should "throw UnsupportedOperationException naming the impl class" in {
    val store = newStore()
    val ex = intercept[UnsupportedOperationException] {
      store.multiPutWithNotification(Seq.empty, "topic")
    }
    ex.getMessage should include(classOf[AerospikeKvStore].getName)
  }

  // 3.12a — per-dataset metrics contexts
  // Upstream Metrics.Context has no dataset tag; per spec the dataset is carried
  // in the metric-name prefix via Context.withSuffix (see design.md).
  "metrics" should "produce per-dataset Metrics.Contexts" in {
    val store = newStore()
    store.metricsContextFor("GB_ONE_BATCH").suffix should include("GB_ONE_BATCH")
    store.metricsContextFor("GB_TWO_BATCH").suffix should include("GB_TWO_BATCH")
    store.metricsContextFor("GB_ONE_BATCH") should not be store.metricsContextFor("GB_TWO_BATCH")
  }
}
