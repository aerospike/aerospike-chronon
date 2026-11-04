package com.aerospike.chronon

import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import com.aerospike.client.AerospikeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

/** Opt-in TLS integration tests against the profile-gated EE fixtures:
  * `aerospike-tls` on 4533 (server auth only) and `aerospike-tls-mtls` on 4534
  * (client cert required). Covers round-trip, fail-closed negatives, mTLS and
  * PKI — see design.md. */
class AerospikeTlsSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  // This suite needs only the EE TLS fixtures, not the plaintext CE rig —
  // skip the trait's `docker compose up` so it also runs when port 3000 is
  // taken by another project's rig.
  override protected def needsBaseRig: Boolean = false
  override protected def needsTlsRig: Boolean = true

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private val timeout = 30.seconds
  private def cert(name: String): String = s"$tlsCertsDir/$name"

  private def roundTrip(conf: Map[String, String]): Unit = {
    val store = newStore(conf)
    val ds = "TLS_ROUNDTRIP_GB_BATCH"
    Await.result(store.multiPut(Seq(PutRequest(bytes("k1"), bytes("v1"), ds, Some(100L)))), timeout)
    val responses = Await.result(store.multiGet(Seq(GetRequest(bytes("k1"), ds, None))), timeout)
    responses.head.values shouldBe a[Success[_]]
    new String(responses.head.values.get.head.bytes, UTF_8) shouldBe "v1"
  }

  private def pwFile(content: String): String = {
    val f = Files.createTempFile("chronon-pw", ".txt")
    f.toFile.deleteOnExit()
    Files.write(f, s"$content\n".getBytes(UTF_8))
    f.toString
  }
  private lazy val goodPwFile = pwFile(tlsPassword)
  private lazy val badPwFile = pwFile("not-the-password")
  private val goodPwEnv = "CHRONON_TEST_AS_PW"
  private val badPwEnv = "CHRONON_TEST_AS_PW_BAD"

  private def base(port: Int): Map[String, String] = testUserConf ++ Map(
    AerospikeConfig.HostsKey -> s"127.0.0.1:$port",
    AerospikeConfig.TlsEnabledKey -> "true",
    AerospikeConfig.TlsNameKey -> tlsServerName,
    AerospikeConfig.TlsTruststoreFileKey -> cert("ca.truststore.p12"),
    AerospikeConfig.TlsTruststorePasswordFileKey -> cert("storepass.txt"),
    AerospikeConfig.TlsTruststoreTypeKey -> "PKCS12",
    // the fixtures run `security {}` — every connection must authenticate
    AerospikeConfig.UserKey -> tlsUser,
    AerospikeConfig.PasswordFileKey -> goodPwFile)

  private def withKeystore(conf: Map[String, String], store: String): Map[String, String] =
    conf ++ Map(
      AerospikeConfig.TlsKeystoreFileKey -> cert(store),
      AerospikeConfig.TlsKeystorePasswordFileKey -> cert("storepass.txt"),
      AerospikeConfig.TlsKeystoreTypeKey -> "PKCS12")

  "TLS store" should "round-trip over TLS with a truststore and cluster tls name" in withTlsServers {
    roundTrip(base(tlsPort))
  }

  it should "round-trip with a per-seed tls name instead of the cluster name" in withTlsServers {
    val conf = base(tlsPort) - AerospikeConfig.TlsNameKey +
      (AerospikeConfig.HostsKey -> s"127.0.0.1:$tlsServerName:$tlsPort")
    roundTrip(conf)
  }

  it should "fail closed on the wrong CA" in withTlsServers {
    val conf = base(tlsPort) +
      (AerospikeConfig.TlsTruststoreFileKey -> cert("other-ca.truststore.p12"))
    intercept[AerospikeException](roundTrip(conf))
  }

  it should "fail closed on a wrong tls name" in withTlsServers {
    intercept[AerospikeException](roundTrip(base(tlsPort) + (AerospikeConfig.TlsNameKey -> "wrong-name")))
  }

  it should "fail closed when a plaintext config hits the TLS port (no silent fallback)" in withTlsServers {
    val conf = testUserConf ++ Map(
      AerospikeConfig.HostsKey -> s"127.0.0.1:$tlsPort",
      AerospikeConfig.ConnectTimeoutKey -> "1000")
    intercept[AerospikeException](roundTrip(conf))
  }

  it should "authenticate with user + password env var over TLS" in withTlsServers {
    roundTrip(base(tlsPort) - AerospikeConfig.PasswordFileKey + (AerospikeConfig.PasswordEnvKey -> goodPwEnv))
  }

  it should "reject a wrong password file with an auth failure" in withTlsServers {
    intercept[AerospikeException](roundTrip(base(tlsPort) + (AerospikeConfig.PasswordFileKey -> badPwFile)))
  }

  it should "reject a wrong password env var with an auth failure" in withTlsServers {
    intercept[AerospikeException](roundTrip(
      base(tlsPort) - AerospikeConfig.PasswordFileKey + (AerospikeConfig.PasswordEnvKey -> badPwEnv)))
  }

  it should "fail closed when the named env var is not set on this host" in withTlsServers {
    val ex = intercept[IllegalStateException](roundTrip(
      base(tlsPort) - AerospikeConfig.PasswordFileKey + (AerospikeConfig.PasswordEnvKey -> "CHRONON_TEST_UNSET_VAR")))
    ex.getMessage should include("CHRONON_TEST_UNSET_VAR")
  }

  it should "not let a store inherit a client built from a different password file" in withTlsServers {
    // store A builds a working client; store B's wrong password must fail at
    // connect rather than silently reusing A's client
    roundTrip(base(tlsPort))
    intercept[AerospikeException](roundTrip(base(tlsPort) + (AerospikeConfig.PasswordFileKey -> badPwFile)))
  }

  it should "not let a store inherit a client built from a different password env var" in withTlsServers {
    val noPw = base(tlsPort) - AerospikeConfig.PasswordFileKey
    roundTrip(noPw + (AerospikeConfig.PasswordEnvKey -> goodPwEnv))
    intercept[AerospikeException](roundTrip(noPw + (AerospikeConfig.PasswordEnvKey -> badPwEnv)))
  }

  "mTLS" should "succeed with a trusted client cert and fail without one" in withTlsServers {
    roundTrip(withKeystore(base(mtlsPort), "client.keystore.p12"))
    intercept[AerospikeException](roundTrip(base(mtlsPort)))
    intercept[AerospikeException](roundTrip(withKeystore(base(mtlsPort), "untrusted.keystore.p12")))
  }

  "PKI auth" should "authenticate by client certificate with no password" in withTlsServers {
    val conf = withKeystore(base(mtlsPort), "client.keystore.p12") -
      AerospikeConfig.UserKey - AerospikeConfig.PasswordFileKey +
      (AerospikeConfig.AuthModeKey -> "pki")
    roundTrip(conf)
  }

  it should "fail for a cert CN with no provisioned user" in withTlsServers {
    val conf = withKeystore(base(mtlsPort), "other-user.keystore.p12") -
      AerospikeConfig.UserKey - AerospikeConfig.PasswordFileKey +
      (AerospikeConfig.AuthModeKey -> "pki")
    intercept[AerospikeException](roundTrip(conf))
  }
}
