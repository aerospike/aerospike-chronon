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

  /** Suites that manage their own containers (e.g. the TLS fixture specs)
    * override to false — the plaintext CE rig is not brought up for them. */
  protected def needsBaseRig: Boolean = true

  /** Suites using [[withTlsServers]] override to true — the EE TLS fixtures
    * are brought up once for the whole suite and removed afterwards. */
  protected def needsTlsRig: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (needsBaseRig) {
      val up = Process("docker compose up -d --wait", new java.io.File(".").getAbsoluteFile)
      require(up.! == 0, "docker compose up -d --wait failed; is the rig defined in docker-compose.yml?")
    }
    if (needsTlsRig) {
      val compose = new java.io.File(".").getAbsoluteFile
      val up = Try(Process(
        Seq("sh", "-c",
          "AEROSPIKE_TLS_UID=$(id -u) AEROSPIKE_TLS_GID=$(id -g) " +
          "docker compose --profile tls run --rm tls-ready"), compose).!)
      up.foreach { rc =>
        if (rc != 0) throw new org.scalatest.exceptions.TestCanceledException("docker compose tls-ready failed", 0)
      }
      if (up.isFailure) throw new org.scalatest.exceptions.TestCanceledException(s"docker unavailable: ${up.failed.get.getMessage}", 0)
    }
    // Suites may share a forked JVM; drop any cached client from a previous container.
    AerospikeClientHolder.clear()
  }

  override def afterAll(): Unit = {
    // stop + rm, not `down`: the rig's network stays up so its deletion cannot
    // churn other stopped containers' network state (chronon-benchmark-aerospike).
    val compose = new java.io.File(".").getAbsoluteFile
    if (needsBaseRig) {
      Try(Process("docker compose stop --timeout 1", compose).!)
      Try(Process("docker compose rm -f", compose).!)
    }
    if (needsTlsRig) {
      tlsContainers.foreach(c => Try(Process(s"docker rm -f $c").!))
    }
    super.afterAll()
  }

  val nsup0Port: Int           = 3100
  val nsup0Container: String   = "aerospike-chronon-test-nosup"

  val tlsPort: Int                 = 4533
  val mtlsPort: Int                = 4534
  val tlsServerName: String        = "aerospike-tls"
  val tlsContainers: Seq[String]   = Seq("aerospike-chronon-test-tls", "aerospike-chronon-test-tls-mtls")
  val tlsCertsDir: String          = "docker/tls-certs"
  val tlsUser: String              = "chronon-user"
  val tlsPassword: String          = "chronon-pass"
  /** The client cert CN, provisioned as a `nopassword` PKI user. */
  val tlsPkiUser: String           = "chronon-client"

  /** Runs `body` against the EE TLS fixtures (`tls` on [[tlsPort]], mTLS
    * required on [[mtlsPort]]). The suite lifecycle owns setup — `beforeAll`
    * runs `docker compose --profile tls run --rm tls-ready` once (certificate
    * init, both servers, user provisioning) and `afterAll` removes the
    * fixture containers — so tests share the same fixtures instead of paying
    * the full compose lifecycle per test. Clears cached clients after each
    * body so a store built under one fixture identity can't leak into the
    * next test. */
  def withTlsServers[T](body: => T): T =
    try body
    finally AerospikeClientHolder.clear()

  /** Runs `body` against the profile-gated `nsup0` server (NSUP_PERIOD=0, port
    * [[nsup0Port]]), removing the container afterwards. Cancels the test when
    * Docker or the service is unavailable, the same way a missing rig cancels
    * the other ITs. */
  def withNsup0Server[T](body: => T): T = {
    val compose = new java.io.File(".").getAbsoluteFile
    val up = Try(Process(Seq("docker", "compose", "--profile", "nsup0", "up", "-d", "--wait", "aerospike-nosup"), compose).!)
    up.foreach { rc =>
      if (rc != 0) throw new org.scalatest.exceptions.TestCanceledException("docker compose nsup0 up failed", 0)
    }
    if (up.isFailure) throw new org.scalatest.exceptions.TestCanceledException(s"docker unavailable: ${up.failed.get.getMessage}", 0)
    try body
    finally {
      Try(Process(s"docker rm -f $nsup0Container").!)
      AerospikeClientHolder.clear()
    }
  }
}
