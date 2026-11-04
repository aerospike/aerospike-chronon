package com.aerospike.chronon

import ai.chronon.online.{Api, KVStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration._

/** Loads the online assembly jar the way Chronon's `Driver` does — a child
  * `URLClassLoader` with the Chronon classes on the parent loader (they are on
  * the `it` classpath here, as on the host JVM), `loadClass` +
  * `getConstructors.apply(0).newInstance(userConf)` — and checks the result is
  * an `Api` whose KV store reads and writes. The child loader is child-first
  * for `com.aerospike.*` so the test proves the class really comes from the
  * jar (the parent classpath also carries it). Cancels when the jar is absent:
  * run `sbt assembly` (or `sbt +assembly`) first. */
class AssemblyClassLoadingSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  /** Child-first for `com.aerospike.*`, parent-first for everything else —
    * mirroring how the Driver isolates the online jar's Aerospike client from
    * the host classpath. */
  private class ChildFirstLoader(urls: Array[java.net.URL], parent: ClassLoader)
      extends URLClassLoader(urls, parent) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      if (name.startsWith("com.aerospike.")) {
        try {
          val cls = findClass(name)
          if (resolve) resolveClass(cls)
          return cls
        } catch { case _: ClassNotFoundException => () }
      }
      super.loadClass(name, resolve)
    }
  }

  private def assemblyJar: Option[File] = {
    val binVer = scala.util.Properties.versionNumberString.split('.').take(2).mkString(".")
    val dir = new File(s"target/scala-$binVer")
    Option(dir.listFiles()).toList.flatten
      .filter(f => f.getName.startsWith("aerospike-chronon-assembly-") && f.getName.endsWith(".jar"))
      .sortBy(_.lastModified()).lastOption
  }

  "the assembly jar" should "load ChrononAerospikeApi like the Driver and read/write" in {
    val jar = assemblyJar.getOrElse(
      cancel(s"no assembly jar under target/scala-*; run `sbt assembly` first"))

    val parent = Thread.currentThread().getContextClassLoader // Chronon is here, as on the host JVM
    val child = new ChildFirstLoader(Array(jar.toURI.toURL), parent)
    try {
      val cls = child.loadClass("com.aerospike.chronon.ChrononAerospikeApi")
      cls.getClassLoader shouldBe child // really from the jar, not the it classpath
      cls.getProtectionDomain.getCodeSource.getLocation.getPath should endWith(jar.getName)

      // negative control: the same child-first loader over an empty dir cannot
      // resolve the class itself and falls back to the parent — proving the
      // assertion above distinguishes the two origins
      val empty = new ChildFirstLoader(Array(Files.createTempDirectory("empty-loader").toUri.toURL), parent)
      empty.loadClass("com.aerospike.chronon.ChrononAerospikeApi").getClassLoader should not be empty

      val ctors = cls.getConstructors
      ctors should have length 1
      val api = ctors(0).newInstance(testUserConf).asInstanceOf[Api]
      val store: KVStore = api.genKvStore

      val req = KVStore.PutRequest("asm-key".getBytes(UTF_8), "asm-value".getBytes(UTF_8), "TEST_DATASET")
      Await.result(store.multiPut(Seq(req)), 30.seconds) shouldBe Seq(true)
      val resp = Await.result(
        store.multiGet(Seq(KVStore.GetRequest("asm-key".getBytes(UTF_8), "TEST_DATASET"))), 30.seconds).head
      new String(resp.values.get.head.bytes, UTF_8) shouldBe "asm-value"

      // The shaded TLS stack must resolve inside the jar — the client's
      // netty-based event loops only work when both the client and netty were
      // relocated consistently.
      child.loadClass("chronon.shaded.com.aerospike.client.async.NettyEventLoops")
      child.loadClass("chronon.shaded.io.netty.channel.MultiThreadIoEventLoopGroup")
      child.loadClass("chronon.shaded.io.netty.channel.nio.NioIoHandler")
    } finally child.close()
  }
}
