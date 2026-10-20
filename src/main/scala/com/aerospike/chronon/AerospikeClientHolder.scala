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

import com.aerospike.client.{AerospikeClient, Host}
import com.aerospike.client.async.{EventLoops, NioEventLoops, EventPolicy}
import com.aerospike.client.policy.{BatchPolicy, ClientPolicy, WritePolicy}

import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
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
  case class PolicyTemplates(write: WritePolicy, batch: BatchPolicy)

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
  private[chronon] def clientCacheKey(config: AerospikeConfig): String =
    s"${config.hosts.sorted.mkString(",")}:${config.port}:${config.maxConnsPerNode}:${config.minConnsPerNode}:" +
      s"${config.maxCommandsInProcess.getOrElse(0)}:" +
      s"${config.connectTimeoutMillis}:${config.socketTimeoutMillis}:${config.totalTimeoutMillis}"

  /** Test/observability hook: how many real clients have been constructed in
    * this JVM. Asserts the sharing guarantee — N stores → 1 client. */
  val constructionCount: AtomicInteger = new AtomicInteger(0)

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
    val (loopCount, perLoop) = eventLoopSizing(
      config.maxCommandsInProcess.getOrElse(config.maxConnsPerNode), Runtime.getRuntime.availableProcessors())
    val eventPolicy = new EventPolicy()
    eventPolicy.maxCommandsInProcess = perLoop
    val eventLoops = new NioEventLoops(eventPolicy, loopCount)
    val clientPolicy = new ClientPolicy()
    clientPolicy.eventLoops = eventLoops
    clientPolicy.maxConnsPerNode = config.maxConnsPerNode
    clientPolicy.asyncMaxConnsPerNode = config.maxConnsPerNode
    clientPolicy.minConnsPerNode = config.minConnsPerNode
    clientPolicy.asyncMinConnsPerNode = config.minConnsPerNode
    // ClientPolicy.timeout is the cluster tend / initial seed-connection timeout,
    // not a per-transaction timeout. Map the user-facing connect timeout here.
    clientPolicy.timeout = config.connectTimeoutMillis

    val hosts = config.hosts.map(new Host(_, config.port)).toArray
    val client = new AerospikeClient(clientPolicy, hosts: _*)
    constructionCount.incrementAndGet()

    val hook = sys.addShutdownHook {
      Try(client.close())
      Try(eventLoops.close())
    }
    shutdownHooks.put(clientCacheKey(config), hook)
    Holder(client, eventLoops)
  }

  /** Returns the shared client for `config`, building it (with event loops and
    * a shutdown hook) on first use per cache key. Thread-safe and idempotent. */
  def clientFor(config: AerospikeConfig): AerospikeClient =
    clients.computeIfAbsent(clientCacheKey(config), _ => buildHolder(config)).client

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
    holders.foreach { h =>
      Try(h.client.close())
      Try(h.eventLoops.close())
    }
    hooks.foreach { hook =>
      Try(Runtime.getRuntime.removeShutdownHook(hook))
    }
  }

  /** Derives write/batch templates from the client's own defaults (picking up
    * its timeouts/retry settings) then overrides connect/socket/total timeouts
    * from `config`. */
  private def buildTemplates(config: AerospikeConfig): PolicyTemplates = {
    val client = clientFor(config)

    val write = new WritePolicy(client.writePolicyDefault)
    write.connectTimeout = config.connectTimeoutMillis
    write.socketTimeout = config.socketTimeoutMillis
    write.totalTimeout = config.totalTimeoutMillis

    val batch = new BatchPolicy(client.batchPolicyDefault)
    batch.connectTimeout = config.connectTimeoutMillis
    batch.socketTimeout = config.socketTimeoutMillis
    batch.totalTimeout = config.totalTimeoutMillis

    PolicyTemplates(write, batch)
  }
}
