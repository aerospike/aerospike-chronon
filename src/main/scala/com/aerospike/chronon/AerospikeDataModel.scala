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

/** Physical layout of Chronon online data in Aerospike.
  *
  * Chronon addresses data by logical `dataset` name; this object maps datasets to
  * Aerospike sets and record keys:
  *
  * {{{
  *   dataset "*_BATCH"      -> set "GROUPBY_BATCH",     key = [prefix]dataset#keyBytes
  *   dataset "*_STREAMING"  -> set "GROUPBY_STREAMING", key = [prefix]dataset#keyBytes#dayTs
  *   anything else          -> set named by the dataset, key = [prefix]dataset#keyBytes
  * }}}
  *
  * All batch group-bys share the `GROUPBY_BATCH` set (and likewise for streaming),
  * so the Aerospike set count stays small regardless of how many group-bys exist;
  * the dataset name is carried inside the record key instead.
  *
  * Record keys are raw byte arrays, never Strings: `keyBytes` is Avro-encoded
  * binary and is concatenated verbatim. Round-tripping through UTF-8 would map
  * distinct byte sequences to the same replacement-character string and collide
  * records, so [[recordKey]] and friends return `Array[Byte]` directly.
  */
object AerospikeDataModel {

  /** Classification of a Chronon dataset; determines the physical Aerospike set. */
  sealed trait TableType {

    /** The Aerospike set records of this table type land in. */
    def setName: String
  }

  /** Batch group-by uploads (`*_BATCH` datasets) share this set. */
  case object BatchTable extends TableType { override val setName: String = "GROUPBY_BATCH" }

  /** Streaming group-by data (`*_STREAMING` datasets) shares this set; records are
    * day-bucketed so TTL and read lookback operate on bounded key ranges. */
  case object StreamingTable extends TableType { override val setName: String = "GROUPBY_STREAMING" }

  /** Metadata and miscellaneous datasets get their own set named after the dataset. */
  final case class MetadataTable(dataset: String) extends TableType { override def setName: String = dataset }

  /** Byte-level separator between record-key segments (`#`, ASCII 35). */
  val KeySeparator: Byte = '#'.toByte

  /** Milliseconds in a day; the streaming bucket granularity. */
  val MillisPerDay: Long = 86400000L

  /** Classifies `dataset` by name suffix: `_BATCH` -> [[BatchTable]],
    * `_STREAMING` -> [[StreamingTable]], anything else -> [[MetadataTable]].
    */
  def getTableType(dataset: String): TableType = {
    if (dataset.endsWith("_BATCH")) BatchTable
    else if (dataset.endsWith("_STREAMING")) StreamingTable
    else MetadataTable(dataset)
  }

  /** The Aerospike set the dataset maps to. Batch and streaming datasets share
    * sets, so this is NOT unique per dataset — the dataset name is carried in
    * the record key.
    */
  def physicalSetName(dataset: String): String = getTableType(dataset).setName

  /** Start-of-day epoch millis for a version timestamp; the suffix used in
    * streaming day-bucket record keys. */
  def dayTs(tsMillis: Long): Long = Math.floorDiv(tsMillis, MillisPerDay) * MillisPerDay

  /** Byte-level key assembly shared by all key builders. Allocation is sized
    * exactly once and filled via `arraycopy` — no intermediate Strings or
    * concatenation, preserving arbitrary binary `keyBytes`. */
  private def concat(prefix: Option[String], dataset: String, keyBytes: Array[Byte], suffix: Option[Array[Byte]] = None): Array[Byte] = {
    val prefixBytes = prefix.map(_.getBytes("UTF-8")).getOrElse(Array.empty)
    val datasetBytes = dataset.getBytes("UTF-8")
    val suffixLen = suffix.map(_.length).getOrElse(0)
    val totalLen = prefixBytes.length + datasetBytes.length + 1 + keyBytes.length + (if (suffixLen > 0) 1 + suffixLen else 0)
    val out = new Array[Byte](totalLen)
    var pos = 0
    System.arraycopy(prefixBytes, 0, out, pos, prefixBytes.length); pos += prefixBytes.length
    System.arraycopy(datasetBytes, 0, out, pos, datasetBytes.length); pos += datasetBytes.length
    out(pos) = KeySeparator; pos += 1
    System.arraycopy(keyBytes, 0, out, pos, keyBytes.length); pos += keyBytes.length
    suffix.foreach { s =>
      out(pos) = KeySeparator; pos += 1
      System.arraycopy(s, 0, out, pos, s.length); pos += s.length
    }
    out
  }

  /** Batch/metadata record key as raw bytes: `[keyPrefix]dataset#keyBytes`.
    *
    * `keyBytes` is Avro-encoded binary and MUST be concatenated verbatim — never
    * round-tripped through a UTF-8 String (distinct binaries can decode to the
    * same replacement-char string and collide).
    *
    * @param dataset  logical Chronon dataset name (embedded in the key)
    * @param keyBytes raw Avro-encoded entity key from Chronon
    * @param keyPrefix optional tenant/environment prefix, prepended unseparated
    * @return the raw record key bytes for `new Key(namespace, set, _)`
    */
  def recordKey(dataset: String, keyBytes: Array[Byte], keyPrefix: Option[String] = None): Array[Byte] =
    concat(keyPrefix, dataset, keyBytes)

  /** Streaming record key: `[keyPrefix]dataset#keyBytes#dayTs` where the day
    * bucket is derived from `tsMillis` via [[dayTs]].
    *
    * Streaming writes shard one entity into one record per UTC day, so a
    * `multiGet` lookback reads at most `maxLookbackDays` records rather than one
    * ever-growing map.
    */
  def streamingRecordKey(dataset: String,
                         keyBytes: Array[Byte],
                         tsMillis: Long,
                         keyPrefix: Option[String] = None): Array[Byte] =
    streamingRecordKeyFromDay(dataset, keyBytes, dayTs(tsMillis), keyPrefix)

  /** Streaming record key using an already-computed start-of-day bucket;
    * `dayTsValue` must be a [[dayTs]] result (encoded as its decimal string). */
  def streamingRecordKeyFromDay(dataset: String,
                                keyBytes: Array[Byte],
                                dayTsValue: Long,
                                keyPrefix: Option[String] = None): Array[Byte] =
    concat(keyPrefix, dataset, keyBytes, Some(dayTsValue.toString.getBytes("UTF-8")))
}
