package com.aerospike.chronon

import ai.chronon.online.{GroupByServingInfoParsed, LoggableResponse, Mutation, StreamDecoder}
import ai.chronon.api.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.function.Consumer

class RecordingConsumer extends Consumer[LoggableResponse] {
  override def accept(t: LoggableResponse): Unit = RecordingConsumer.seen.add(t)
}
object RecordingConsumer {
  val seen = new ConcurrentLinkedQueue[LoggableResponse]()
}

class TestStreamDecoder(gbsip: GroupByServingInfoParsed) extends StreamDecoder {
  override def decode(bytes: Array[Byte]): Mutation = Mutation()
  override def schema: StructType = null
}

class ChrononAerospikeApiSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private def baseConf = testUserConf

  "genKvStore" should "return a configured AerospikeKvStore" in {
    val api = new ChrononAerospikeApi(baseConf)
    api.genKvStore shouldBe a[AerospikeKvStore]
  }

  "logResponse" should "be a no-op when no consumer class is configured" in {
    val api = new ChrononAerospikeApi(baseConf)
    noException should be thrownBy
      api.logResponse(LoggableResponse(Array.empty, Array.empty, "j", 0L, "h"))
  }

  it should "forward responses to the consumer named by chronon.log.consumer.class" in {
    RecordingConsumer.seen.clear()
    val api = new ChrononAerospikeApi(baseConf + ("chronon.log.consumer.class" -> classOf[RecordingConsumer].getName))
    val resp = LoggableResponse(Array(1), Array(2), "join", 42L, "hash")
    api.logResponse(resp)
    RecordingConsumer.seen should contain(resp)
  }

  "streamDecoder" should "instantiate the class named by chronon.stream.decoder.class" in {
    val api = new ChrononAerospikeApi(baseConf + ("chronon.stream.decoder.class" -> classOf[TestStreamDecoder].getName))
    api.streamDecoder(null) shouldBe a[TestStreamDecoder]
  }

  it should "fail descriptively when chronon.stream.decoder.class is unset" in {
    val api = new ChrononAerospikeApi(baseConf)
    val ex = intercept[Exception] { api.streamDecoder(null) }
    ex.getMessage should include("chronon.stream.decoder.class")
  }

  "ChrononAerospikeApi" should "survive Java serialization" in {
    val api = new ChrononAerospikeApi(baseConf)
    val bos = new ByteArrayOutputStream()
    new ObjectOutputStream(bos).writeObject(api)
    val roundTripped = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray)).readObject()
    roundTripped shouldBe a[ChrononAerospikeApi]
  }
}
