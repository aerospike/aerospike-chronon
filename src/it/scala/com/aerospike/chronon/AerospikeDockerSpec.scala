package com.aerospike.chronon

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.sys.process.Process
import scala.util.Try

/** Brings the docker-compose Aerospike rig up once per suite and exposes the
  * shared `userConf` the store under test is built from. */
trait AerospikeDockerSpec extends BeforeAndAfterAll { this: Suite =>

  val aerospikeHost: String = sys.env.getOrElse("AEROSPIKE_HOST", "localhost")
  val aerospikePort: Int    = sys.env.get("AEROSPIKE_PORT").map(_.toInt).getOrElse(3000)
  val aerospikeNamespace    = "test"

  def testUserConf: Map[String, String] = Map(
    AerospikeConfig.HostsKey                    -> aerospikeHost,
    AerospikeConfig.PortKey                     -> aerospikePort.toString,
    AerospikeConfig.NamespaceKey                -> aerospikeNamespace,
    AerospikeConfig.TtlSecondsKey               -> "86400",
    AerospikeConfig.StreamingMaxLookbackDaysKey -> "1"
  )

  def newStore(conf: Map[String, String] = testUserConf): AerospikeKvStore =
    AerospikeKvStore(conf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val up = Process("docker compose up -d --wait", new java.io.File(".").getAbsoluteFile)
    require(up.! == 0, "docker compose up -d --wait failed; is the rig defined in docker-compose.yml?")
    // Suites may share a forked JVM; drop any cached client from a previous container.
    AerospikeClientHolder.clear()
  }

  override def afterAll(): Unit = {
    Try(Process("docker compose down").!)
    super.afterAll()
  }
}
