/*
 * Copyright (C) 2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.chronon

import com.aerospike.client.{AerospikeClient, Host, Info}
import com.aerospike.client.async.{EventLoops, EventPolicy, NettyEventLoops, NioEventLoops}
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import com.aerospike.client.policy.{AuthMode, BatchPolicy, ClientPolicy, TlsPolicy, WritePolicy}

import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.security.KeyStore
import java.util.concurrent.ConcurrentHashMap
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.util.Try

/** One shared Aerospike client per JVM, keyed by config. Lazy; async ops run on
  * dedicated `NioEventLoops` owned by the client. Policy templates are cached
  * and cloned so hot paths do not allocate a new policy object per call.
  *
  * The cache key (`clientCacheKey`) covers only the fields that physically
  * shape the client — hosts, port, connection-pool bounds, in-flight cap, and
  * timeouts — so two `AerospikeKvStore`s that differ only in namespace, TTL,
  * key prefix, lookback, or loader timeout share one client and one set of
  * event loops. In a fetcher JVM this keeps a single connection pool per
  * cluster regardless of how many stores/datasets are instantiated.
  *
  * Each built client registers a shutdown hook closing the client then the
  * event loops; [[clear]] removes hooks and closes everything, which the
  * integration suites use so each test talks to its own freshly-started
  * container.
  */
object AerospikeClientHolder {

  /** A constructed client plus the event loops it was built with (closed
    * together). */
  private case class Holder(client: AerospikeClient, eventLoops: EventLoops)

  /** Cloned-from-client-defaults policies for the two hot paths. Callers that
    * need per-request mutations (e.g. record TTL) MUST copy the template —
    * see `AerospikeKvStore.writePolicyFor`. */
  case class PolicyTemplates(write: WritePolicy, batch: BatchPolicy, statsBatch: BatchPolicy)

  private val logger = LoggerFactory.getLogger(getClass)

  /** Returns (event loop count, maxCommandsInProcess per loop) for a total in-flight cap.
    * The client rejects 1..4 per loop, so a small total gets fewer loops to stay valid. */
  private[chronon] def eventLoopSizing(totalInFlight: Int, cpus: Int): (Int, Int) = {
    val min = AerospikeConfig.MinCommandsInProcess
    val loops = math.max(1, math.min(cpus, totalInFlight / min))
    (loops, math.max(min, totalInFlight / loops))
  }

  private val clients = new ConcurrentHashMap[String, Holder]()
  private val templates = new ConcurrentHashMap[String, PolicyTemplates]()
  private val shutdownHooks = new ConcurrentHashMap[String, Thread]()

  /** Cache key covering only the fields that affect client and policy construction.
    * Two configs that differ only in namespace, TTL, key prefix, lookback, or loader
    * timeout share one client and event loops. */
  private[chronon] def clientCacheKey(config: AerospikeConfig): String = {
    val seeds = config.hosts.map(s => s"${s.name}:${s.tlsName.getOrElse("")}:${s.port}")
      .sorted.mkString(",")
    val tls = config.tls.map { t =>
      s"tls(${t.tlsName.getOrElse("")}|${t.protocols.getOrElse(Nil).mkString("+")}|" +
        s"${t.ciphers.getOrElse(Nil).mkString("+")}|${t.truststoreFile.getOrElse("")}:" +
        s"${t.truststoreType}@${t.truststorePasswordFile.getOrElse("")}|" +
        s"${t.keystoreFile.getOrElse("")}:${t.keystoreType}" +
        s"@${t.keystorePasswordFile.getOrElse("")}@${t.keystoreKeyPasswordFile.getOrElse("")}|" +
        s"${t.forLoginOnly})"
    }.getOrElse("plain")
    // Credentials are references, so the key carries the SOURCE (file path or
    // env var name), never a secret. A different source is a different
    // credential and must get its own client rather than silently inheriting
    // one built from another. An env var cannot change within a JVM, so keying
    // on its name is exact; a file rewritten in place is the one case that
    // reuses the existing client (restart to rotate).
    val auth = s"${config.authMode.key}:${config.credentials.map(_.user).getOrElse("")}" +
      s"@${config.credentials.flatMap(_.passwordFile).getOrElse("")}" +
      s"@env:${config.credentials.flatMap(_.passwordEnv).getOrElse("")}"
    s"$seeds:${config.port}:${config.maxConnsPerNode}:${config.minConnsPerNode}:" +
      s"${config.maxCommandsInProcess.getOrElse(0)}:" +
      s"${config.connectTimeoutMillis}:${config.socketTimeoutMillis}:" +
      s"${config.readTotalTimeoutMillis}:${config.statsReadTotalTimeoutMillis}:${config.writeTotalTimeoutMillis}:" +
      s"${config.maxRetriesRead}:${config.maxRetriesWrite}:$tls:$auth"
  }

  /** Test/observability hook: how many real clients have been constructed in
    * this JVM. Asserts the sharing guarantee — N stores → 1 client. */
  val constructionCount: AtomicInteger = new AtomicInteger(0)

  /** Test-only hook: extra closers run (guarded) on a failed `buildHolder`, so
    * tests can verify a failing close does not mask the original error. */
  @volatile private[chronon] var closeProbes: List[() => Any] = Nil

  /** Builds a client + event loops and registers its shutdown hook.
    *
    * `maxCommandsInProcess` (defaulting to `maxConnsPerNode`) is the total
    * in-flight async cap, spread across event loops sized by
    * [[eventLoopSizing]]; commands beyond the cap queue in the client rather
    * than failing. A cap above `maxConnsPerNode` only logs a warning — it can
    * still exhaust the pool under skewed single-node load.
    *
    * `ClientPolicy.timeout` is mapped to `connectTimeoutMillis` because it is
    * the cluster-tend / seed-connection timeout, not a per-transaction timeout
    * (those live on the policy templates). */
  private def buildHolder(config: AerospikeConfig): Holder = {
    config.maxCommandsInProcess.filter(_ > config.maxConnsPerNode).foreach { v =>
      logger.warn(s"${AerospikeConfig.MaxCommandsInProcessKey}=$v exceeds ${AerospikeConfig.MaxConnsPerNodeKey}=" +
        s"${config.maxConnsPerNode}; on a skewed workload (all commands to one node) writes can still fail with " +
        "NO_MORE_CONNECTIONS. Raise the connection pool or lower the cap unless the cluster has several nodes.")
    }
    val totalInFlight = config.maxCommandsInProcess.getOrElse(config.maxConnsPerNode)
    val (loopCount, perLoop) = eventLoopSizing(
      totalInFlight, Runtime.getRuntime.availableProcessors())
    logger.info(s"Building Aerospike client for ${config.hosts.mkString(",")}:${config.port}: " +
      s"asyncMinConnsPerNode=${config.minConnsPerNode} asyncMaxConnsPerNode=${config.maxConnsPerNode} " +
      s"maxCommandsInProcess=$totalInFlight eventLoops=$loopCount perLoop=$perLoop")
    var eventLoops: EventLoops = null
    var client: AerospikeClient = null
    try {
      val eventPolicy = new EventPolicy()
      eventPolicy.maxCommandsInProcess = perLoop
      // The direct-NIO loops cannot speak TLS ("TLS not supported in direct
      // NIO event loops"); TLS configs switch to netty loops. Netty comes from
      // the host classpath (Spark/Chronon both ship it) — it is not bundled
      // with the jdk8 client.
      eventLoops =
        if (config.tls.isDefined) {
          try new NettyEventLoops(eventPolicy,
            new MultiThreadIoEventLoopGroup(loopCount, NioIoHandler.newFactory()))
          catch {
            case e: NoClassDefFoundError =>
              throw new IllegalStateException(
                "TLS requires netty 4.2+ on the classpath (io.netty), which the " +
                  "aerospike client does not bundle: " + e.getMessage, e)
          }
        } else new NioEventLoops(eventPolicy, loopCount)
      val clientPolicy = clientPolicyFor(config, eventLoops)

      client = new AerospikeClient(clientPolicy, seedsFor(config): _*)
      constructionCount.incrementAndGet()
      checkNamespacePolicy(config, resolveNamespace(client, config))
    } catch {
      case t: Throwable =>
        logger.info(s"Aerospike client build for ${config.hosts.mkString(",")}:${config.port} failed: ${t.getMessage}")
        // guarded: a failing close must never mask the original error
        Try(Option(client).foreach(_.close()))
        Try(Option(eventLoops).foreach(_.close()))
        closeProbes.foreach(p => Try(p()))
        throw t
    }

    val hook = sys.addShutdownHook {
      Try(client.close())
      Try(eventLoops.close())
    }
    shutdownHooks.put(clientCacheKey(config), hook)
    Holder(client, eventLoops)
  }

  /** The `ClientPolicy` behind every built client. The store only uses the
    * async API, so only the async pool is configured: `asyncMinConnsPerNode` /
    * `asyncMaxConnsPerNode` come from `aerospike.minConnsPerNode` /
    * `aerospike.maxConnsPerNode`; the sync floor is 0 and the sync cap stays at
    * the client default. `ClientPolicy.timeout` is the cluster-tend / seed
    * connect timeout (per-transaction timeouts live on the policy templates). */
  private[chronon] def clientPolicyFor(config: AerospikeConfig, eventLoops: EventLoops): ClientPolicy = {
    val clientPolicy = new ClientPolicy()
    clientPolicy.eventLoops = eventLoops
    clientPolicy.asyncMaxConnsPerNode = config.maxConnsPerNode
    clientPolicy.asyncMinConnsPerNode = config.minConnsPerNode
    clientPolicy.minConnsPerNode = 0
    clientPolicy.timeout = config.connectTimeoutMillis

    config.tls.foreach { t =>
      val tlsPolicy = new TlsPolicy()
      t.protocols.foreach(p => tlsPolicy.protocols = p.toArray)
      t.ciphers.foreach(c => tlsPolicy.ciphers = c.toArray)
      tlsPolicy.forLoginOnly = t.forLoginOnly
      // Only build an explicit SSLContext when store files are configured;
      // otherwise the client's null-context default (system truststore) applies.
      if (t.truststoreFile.isDefined || t.keystoreFile.isDefined) {
        tlsPolicy.context = buildSslContext(t)
      }
      // NOTE: tlsName is applied per-host in seedsFor, not via
      // clientPolicy.clusterName — setting clusterName also enables
      // cluster-name validation, which would reject clusters whose
      // `cluster-name` differs from the TLS name.
      clientPolicy.tlsPolicy = tlsPolicy
    }
    config.authMode match {
      case AuthModeSetting.External         => clientPolicy.authMode = AuthMode.EXTERNAL
      case AuthModeSetting.ExternalInsecure => clientPolicy.authMode = AuthMode.EXTERNAL_INSECURE
      case AuthModeSetting.Pki              => clientPolicy.authMode = AuthMode.PKI
      case AuthModeSetting.Internal         => ()
    }
    config.credentials.foreach { c =>
      clientPolicy.user = c.user
      clientPolicy.password = resolvePassword(c)
    }
    clientPolicy
  }

  /** Resolves a credential reference on the JVM building the client (never at
    * config parse, which on Spark runs on the driver): the trimmed contents of
    * the password file, or the named environment variable. A missing or blank
    * env var fails closed rather than building an unauthenticated client. */
  private def readFile(key: String, path: String): Array[Byte] =
    try Files.readAllBytes(Paths.get(path))
    catch {
      case e: java.io.IOException =>
        throw new IllegalStateException(s"Could not read file '$path' configured by '$key': ${e.getMessage}", e)
    }

  private def openFile(key: String, path: String): java.io.InputStream =
    try Files.newInputStream(Paths.get(path))
    catch {
      case e: java.io.IOException =>
        throw new IllegalStateException(s"Could not read file '$path' configured by '$key': ${e.getMessage}", e)
    }

  private[chronon] def resolvePassword(c: Credentials,
                                       env: String => Option[String] = n => Option(System.getenv(n))): String =
    c.passwordFile.map(f => new String(readFile(AerospikeConfig.PasswordFileKey, f), UTF_8).trim)
      .orElse(c.passwordEnv.map { n =>
        env(n).map(_.trim).filter(_.nonEmpty).getOrElse(throw new IllegalStateException(
          s"Environment variable '$n' named by '${AerospikeConfig.PasswordEnvKey}' is not set or empty on this host"))
      }).orNull

  private def readStorePassword(key: String, file: Option[String]): Array[Char] =
    file.map(f => new String(readFile(key, f), UTF_8).trim.toCharArray)
      .orNull

  /** Builds an `SSLContext` from the configured truststore and/or keystore.
    * With only a truststore, default key managers apply; with only a keystore
    * (unusual), default trust managers apply. */
  private[chronon] def buildSslContext(t: TlsSettings): SSLContext = {
    val trustManagers = t.truststoreFile.map { f =>
      val ks = KeyStore.getInstance(t.truststoreType)
      val in = openFile(AerospikeConfig.TlsTruststoreFileKey, f)
      try ks.load(in, readStorePassword(AerospikeConfig.TlsTruststorePasswordFileKey, t.truststorePasswordFile))
      finally in.close()
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(ks)
      tmf.getTrustManagers
    }.orNull
    val keyManagers = t.keystoreFile.map { f =>
      val storePassword = readStorePassword(AerospikeConfig.TlsKeystorePasswordFileKey, t.keystorePasswordFile)
      val keyPassword = t.keystoreKeyPasswordFile
        .map(kf => new String(readFile(AerospikeConfig.TlsKeystoreKeyPasswordFileKey, kf), UTF_8).trim.toCharArray)
        .getOrElse(storePassword)
      val ks = KeyStore.getInstance(t.keystoreType)
      val in = openFile(AerospikeConfig.TlsKeystoreFileKey, f)
      try ks.load(in, storePassword)
      finally in.close()
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(ks, keyPassword)
      kmf.getKeyManagers
    }.orNull
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(keyManagers, trustManagers, null)
    ctx
  }

  /** The `Host[]` passed to the client: per-seed `tlsName` wins, the global
    * `aerospike.tls.name` covers nameless seeds. */
  private[chronon] def seedsFor(config: AerospikeConfig): Array[Host] =
    config.hosts.map { s =>
      val tlsName = s.tlsName.orElse(config.tls.flatMap(_.tlsName)).orNull
      new Host(s.name, tlsName, s.port)
    }.toArray

  /** Returns the shared client for `config`, building it (with event loops and
    * a shutdown hook) on first use per cache key. Thread-safe and idempotent. */
  def clientFor(config: AerospikeConfig): AerospikeClient =
    clients.computeIfAbsent(clientCacheKey(config), _ => buildHolder(config)).client

  /** Namespace settings reported by `info namespace/<ns>`: the supervisor
    * period, whether TTL-bearing writes are allowed without it, and the
    * namespace default TTL. Fields are `None` when the server did not report
    * them (older servers, restricted info access, unparsable values). */
  private[chronon] final case class NamespaceSettings(nsupPeriodSeconds: Option[Long],
                                                      allowTtlWithoutNsup: Option[Boolean],
                                                      defaultTtlSeconds: Option[Long])

  /** Tolerant parser for the `namespace/<ns>` info response (`k=v;k=v;...`):
    * missing keys, `ERROR:...` responses and unparsable values yield `None`
    * fields, never an exception. */
  private[chronon] def parseNamespaceInfo(response: String): Option[NamespaceSettings] = {
    if (response == null) return None
    val fields = response
      .split(";")
      .flatMap { kv =>
        kv.split("=", 2) match {
          case Array(k, v) => Some(k.trim -> v.trim)
          case _           => None
        }
      }
      .toMap
    def long(key: String): Option[Long] = fields.get(key).flatMap(v => Try(v.toLong).toOption)
    val settings = NamespaceSettings(
      long("nsup-period"),
      fields.get("allow-ttl-without-nsup").flatMap(v =>
        if (v.equalsIgnoreCase("true")) Some(true) else if (v.equalsIgnoreCase("false")) Some(false) else None),
      long("default-ttl"))
    // a reply with no recognised keys (an ERROR response, a restricted info
    // command, an empty string) counts as unresolved, not as a real namespace
    if (settings.nsupPeriodSeconds.isEmpty && settings.allowTtlWithoutNsup.isEmpty &&
        settings.defaultTtlSeconds.isEmpty) None
    else Some(settings)
  }

  // successful resolutions only, per client (cache key) + namespace
  private val namespaceSettings = new ConcurrentHashMap[(String, String), NamespaceSettings]()

  /** Test seam: how `namespace/<ns>` is fetched for a client; the default runs
    * `Info.request` on the client's first node. */
  private[chronon] var namespaceInfoSource: (AerospikeClient, String) => String =
    (client, ns) =>
      client.getNodes.headOption
        .map(node => Info.request(null, node, s"namespace/$ns"))
        .orNull

  /** The settings for `config`'s namespace on its shared client. A cached
    * successful resolution is returned; otherwise the info request runs now
    * against the existing client — a different namespace on a shared client,
    * or a first resolution that failed transiently at build time. Failures
    * are never cached: the next store construction retries. */
  private[chronon] def namespaceSettingsFor(config: AerospikeConfig): Option[NamespaceSettings] =
    Option(namespaceSettings.get((clientCacheKey(config), config.namespace)))
      .orElse(resolveNamespace(clientFor(config), config))

  /** One `info namespace/<ns>` request against a node of the client; caches a
    * successful result per client+namespace. Any failure logs a WARN and
    * resolves to None (the startup check is then skipped) — it never fails
    * the build and is never cached, so a later store retries it. */
  private def resolveNamespace(client: AerospikeClient, config: AerospikeConfig): Option[NamespaceSettings] = {
    val key = (clientCacheKey(config), config.namespace)
    Option(namespaceSettings.get(key)).orElse {
      val settings = Try(parseNamespaceInfo(namespaceInfoSource(client, config.namespace))).getOrElse(None)
      settings match {
        case Some(ns) =>
          namespaceSettings.put(key, ns)
          logger.info(s"Aerospike namespace '${config.namespace}': nsup-period=${ns.nsupPeriodSeconds.getOrElse("?")}s " +
            s"allow-ttl-without-nsup=${ns.allowTtlWithoutNsup.getOrElse("?")} default-ttl=${ns.defaultTtlSeconds.getOrElse("?")}s; " +
            s"record TTLs: batch=${config.batchTtl} streaming=${config.streamingTtl}")
          if (config.streamingTtl == TtlSetting.NeverExpire) {
            logger.warn(s"${AerospikeConfig.TtlStreamingSecondsKey} is never-expire: one record per key per day " +
              "accumulates in GROUPBY_STREAMING until the entity stops writing.")
          }
        case None =>
          logger.warn(s"Could not read namespace '${config.namespace}' settings (nsup-period, " +
            "allow-ttl-without-nsup, default-ttl): skipping the supervisor check and 'namespace' TTL resolution")
      }
      settings
    }
  }

  /** The startup namespace check (spec "Namespace supervisor"): throws
    * `IllegalStateException` naming `nsup-period`, `allow-ttl-without-nsup` and
    * the options when any record class would be written with a TTL (positive
    * seconds, or `namespace` resolved to a positive `default-ttl`) while
    * `nsup-period` is 0 and the allow flag is false — the server would reject
    * every such write with code 22. Also validates `maxLookbackDays` against a
    * resolved `namespace` streaming TTL. Skipped entirely when the namespace
    * settings could not be resolved. */
  private[chronon] def checkNamespacePolicy(config: AerospikeConfig,
                                            ns: Option[NamespaceSettings]): Unit = {
    def resolvedSeconds(ttl: TtlSetting): Option[Long] = ttl match {
      case TtlSetting.Seconds(s)       => Some(s.toLong)
      case TtlSetting.NeverExpire      => None
      case TtlSetting.NamespaceDefault => ns.flatMap(_.defaultTtlSeconds).filter(_ > 0)
    }
    val ttlInUse = resolvedSeconds(config.batchTtl).nonEmpty || resolvedSeconds(config.streamingTtl).nonEmpty
    for {
      settings <- ns
      nsup <- settings.nsupPeriodSeconds
      if nsup == 0 && !settings.allowTtlWithoutNsup.contains(true) && ttlInUse
    } {
      throw new IllegalStateException(
        s"Namespace '${config.namespace}' has nsup-period=0 and allow-ttl-without-nsup=false, but this store " +
          s"writes TTL-bearing records (batch=${config.batchTtl}, streaming=${config.streamingTtl}); the server " +
          "would reject every such write (code 22). Enable the supervisor (nsup-period > 0), set " +
          "allow-ttl-without-nsup (testing only), or use TTL 0 (never expire).")
    }
    resolvedSeconds(config.streamingTtl).foreach { seconds =>
      val lookbackMillis = config.streamingMaxLookbackDays.toLong * 86400000L
      if (lookbackMillis > seconds * 1000L) {
        throw new IllegalStateException(
          s"Config key '${AerospikeConfig.StreamingMaxLookbackDaysKey}' (${config.streamingMaxLookbackDays} days) " +
            s"exceeds the streaming TTL resolved from namespace '${config.namespace}' (${seconds}s). " +
            "Set max lookback <= TTL or increase TTL.")
      }
    }
  }

  /** Cached policy templates derived from the client's defaults. Callers that need
    * per-call mutations (e.g. per-record TTL) must clone the returned template. */
  def policyTemplates(config: AerospikeConfig): PolicyTemplates =
    templates.computeIfAbsent(clientCacheKey(config), _ => buildTemplates(config))

  /** Test-only hook: close and drop all cached clients/templates. Used between
    * integration suites so each suite talks to its freshly-started container. */
  def clear(): Unit = {
    val holders = clients.values().asScala.toList
    val hooks = shutdownHooks.values().asScala.toList
    clients.clear()
    templates.clear()
    shutdownHooks.clear()
    namespaceSettings.clear()
    holders.foreach { h =>
      Try(h.client.close())
      Try(h.eventLoops.close())
    }
    hooks.foreach { hook =>
      Try(Runtime.getRuntime.removeShutdownHook(hook))
    }
  }

  /** Test-only hook: like `clear` but only for `config`'s cache key — the
    * other cached clients keep running. */
  private[chronon] def evict(config: AerospikeConfig): Unit = {
    val key = clientCacheKey(config)
    val holder = clients.remove(key)
    templates.remove(key)
    Option(shutdownHooks.remove(key)).foreach { hook =>
      Try(Runtime.getRuntime.removeShutdownHook(hook))
    }
    namespaceSettings.keys().asScala.filter(_._1 == key).foreach(namespaceSettings.remove)
    Option(holder).foreach { h =>
      Try(h.client.close())
      Try(h.eventLoops.close())
    }
  }

  /** Derives write/batch templates from the client's policy defaults (plain
    * `WritePolicy`/`BatchPolicy`) then overrides connect, socket, the
    * per-class total timeout and per-class `maxRetries` from `config`.
    * `sleepBetweenRetries` stays at the client default 0 — it is ignored in
    * async mode anyway. */
  private[chronon] def buildTemplates(config: AerospikeConfig): PolicyTemplates = {
    val write = new WritePolicy()
    write.connectTimeout = config.connectTimeoutMillis
    write.socketTimeout = config.socketTimeoutMillis
    write.totalTimeout = config.writeTotalTimeoutMillis
    write.maxRetries = config.maxRetriesWrite

    val batch = new BatchPolicy()
    batch.connectTimeout = config.connectTimeoutMillis
    batch.socketTimeout = config.socketTimeoutMillis
    batch.totalTimeout = config.readTotalTimeoutMillis
    batch.maxRetries = config.maxRetriesRead

    val statsBatch = new BatchPolicy()
    statsBatch.connectTimeout = batch.connectTimeout
    statsBatch.socketTimeout = batch.socketTimeout
    statsBatch.totalTimeout = config.statsReadTotalTimeoutMillis
    statsBatch.maxRetries = batch.maxRetries

    PolicyTemplates(write, batch, statsBatch)
  }
}
