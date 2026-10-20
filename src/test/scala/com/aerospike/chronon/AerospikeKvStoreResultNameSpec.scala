package com.aerospike.chronon

import com.aerospike.client.ResultCode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AerospikeKvStoreResultNameSpec extends AnyFlatSpec with Matchers {

  "resultName" should "turn NO_MORE_CONNECTIONS into a metric-safe label" in {
    AerospikeKvStore.resultName(ResultCode.NO_MORE_CONNECTIONS) shouldBe "no_more_available_connections"
  }

  it should "produce only [a-z0-9_] labels for every client and server code" in {
    val codes = (-20 to 0) ++ (1 to 30)
    codes.foreach { c =>
      AerospikeKvStore.resultName(c) should fullyMatch regex "[a-z0-9]+(_[a-z0-9]+)*"
    }
  }
}
