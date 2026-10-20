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

import ai.chronon.online.{Api, ExternalSourceRegistry, GroupByServingInfoParsed, KVStore, LoggableResponse, StreamDecoder}

import java.util.function.Consumer
import scala.util.control.NonFatal

/** Chronon `ai.chronon.online.Api` implementation for Aerospike; the class to
  * point `api.impl` at. Everything is driven by `userConf` — the same map
  * Chronon passes to fetchers, the Spark loader, and streaming jobs.
  *
  * Provides the shared [[AerospikeKvStore]] plus two pluggable hooks:
  *
  *  - `chronon.stream.decoder.class` (required for streaming) — FQN of a
  *    `StreamDecoder` with a `(GroupByServingInfoParsed)` constructor, loaded
  *    by reflection.
  *  - `chronon.log.consumer.class` (optional) — FQN of a
  *    `java.util.function.Consumer[LoggableResponse]` instantiated once and
  *    cached; [[logResponse]] is a no-op without it.
  *
  * The KV store is `@transient lazy`: deserialized instances rebuild the store
  * (and its shared client) on first use rather than shipping connections.
  */
class ChrononAerospikeApi(userConf: Map[String, String]) extends Api(userConf) {

  @transient private lazy val kvStore: KVStore = AerospikeKvStore(userConf)

  /** The singleton [[AerospikeKvStore]] for this `userConf`. */
  override def genKvStore: KVStore = kvStore

  /** Instantiates the `chronon.stream.decoder.class` impl. Throws
    * `IllegalArgumentException` if the key is absent or the class can't be
    * constructed (missing class, wrong constructor, init failure). */
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = {
    val className = userConf.getOrElse(
      ChrononAerospikeApi.StreamDecoderClassKey,
      throw new IllegalArgumentException(s"Missing required config key: ${ChrononAerospikeApi.StreamDecoderClassKey}")
    )
    instantiateStreamDecoder(className, groupByServingInfoParsed)
  }

  /** A fresh empty registry; no external sources are registered by default. */
  override def externalRegistry: ExternalSourceRegistry = new ExternalSourceRegistry()

  /** No-op unless `chronon.log.consumer.class` is configured, in which case the
    * response is forwarded to the (cached) consumer instance. */
  override def logResponse(resp: LoggableResponse): Unit = {
    userConf.get(ChrononAerospikeApi.LogConsumerClassKey).foreach { className =>
      logConsumerFor(className).accept(resp)
    }
  }

  @transient private lazy val logConsumerFor: String => Consumer[LoggableResponse] = {
    val cache = scala.collection.concurrent.TrieMap.empty[String, Consumer[LoggableResponse]]
    className =>
      cache.getOrElseUpdate(
        className, {
          Class
            .forName(className)
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[Consumer[LoggableResponse]]
        }
      )
  }

  private def instantiateStreamDecoder(className: String, gbsip: GroupByServingInfoParsed): StreamDecoder = {
    try {
      Class
        .forName(className)
        .getConstructor(classOf[GroupByServingInfoParsed])
        .newInstance(gbsip)
        .asInstanceOf[StreamDecoder]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unable to instantiate stream decoder class '$className' from config key ${ChrononAerospikeApi.StreamDecoderClassKey}",
          e
        )
    }
  }
}

object ChrononAerospikeApi {
  /** `userConf` key: optional `Consumer[LoggableResponse]` FQN for response logging. */
  val LogConsumerClassKey: String = "chronon.log.consumer.class"

  /** `userConf` key: `StreamDecoder` FQN (ctor takes `GroupByServingInfoParsed`);
    * required by [[ChrononAerospikeApi.streamDecoder]]. */
  val StreamDecoderClassKey: String = "chronon.stream.decoder.class"
}
