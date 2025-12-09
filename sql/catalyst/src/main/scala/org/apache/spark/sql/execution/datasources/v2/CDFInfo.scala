/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

/**
 * Represents the type of change in a Change Data Feed record.
 */
object CDFChangeType {
  val INSERT = "insert"
  val UPDATE_PREIMAGE = "update_preimage"
  val UPDATE_POSTIMAGE = "update_postimage"
  val DELETE = "delete"
}

/**
 * Indicates which batch type a CDF relation should scan.
 * This determines whether the Scan should call toAddedRecordsBatch() or toRemovedRecordsBatch().
 */
sealed trait CDFBatchType {
  def name: String
}

/**
 * Indicates the relation should scan added/new records via toAddedRecordsBatch().
 * This includes inserted rows and post-update rows.
 */
case object CDFAddedBatch extends CDFBatchType {
  override def name: String = "added"
}

/**
 * Indicates the relation should scan removed/deleted records via toRemovedRecordsBatch().
 * This includes deleted rows and pre-update rows.
 */
case object CDFRemovedBatch extends CDFBatchType {
  override def name: String = "removed"
}

/**
 * Contains information for Change Data Feed (CDF) reads.
 *
 * @param startVersion The starting version (inclusive) for CDF read. None means unbounded.
 * @param endVersion The ending version (inclusive) for CDF read. None means unbounded.
 * @param startTimestamp The starting timestamp (inclusive) for CDF read. None means unbounded.
 * @param endTimestamp The ending timestamp (inclusive) for CDF read. None means unbounded.
 * @param rowIdColumns The columns that uniquely identify a row for join purposes.
 *                     Used to match added and removed records.
 * @param batchType Indicates which batch this relation should scan:
 *                  - CDFAddedBatch: calls toAddedRecordsBatch() on the scan
 *                  - CDFRemovedBatch: calls toRemovedRecordsBatch() on the scan
 *                  - None: not yet resolved (used in the parent CDFRelation before expansion)
 */
case class CDFInfo(
    startVersion: Option[Long] = None,
    endVersion: Option[Long] = None,
    startTimestamp: Option[Long] = None,
    endTimestamp: Option[Long] = None,
    rowIdColumns: Seq[String] = Seq.empty,
    batchType: Option[CDFBatchType] = None) {

  require(
    startVersion.isDefined || startTimestamp.isDefined,
    "Either startVersion or startTimestamp must be specified for CDF read")

  /** Creates a copy with batchType set to CDFAddedBatch */
  def forAddedBatch: CDFInfo = copy(batchType = Some(CDFAddedBatch))

  /** Creates a copy with batchType set to CDFRemovedBatch */
  def forRemovedBatch: CDFInfo = copy(batchType = Some(CDFRemovedBatch))

  /** Returns true if this is configured to scan added records */
  def isAddedBatch: Boolean = batchType.contains(CDFAddedBatch)

  /** Returns true if this is configured to scan removed records */
  def isRemovedBatch: Boolean = batchType.contains(CDFRemovedBatch)

  override def toString: String = {
    val versionRange = (startVersion, endVersion) match {
      case (Some(s), Some(e)) => s"VERSION $s TO $e"
      case (Some(s), None) => s"VERSION FROM $s"
      case (None, Some(e)) => s"VERSION TO $e"
      case _ => ""
    }
    val timestampRange = (startTimestamp, endTimestamp) match {
      case (Some(s), Some(e)) => s"TIMESTAMP $s TO $e"
      case (Some(s), None) => s"TIMESTAMP FROM $s"
      case (None, Some(e)) => s"TIMESTAMP TO $e"
      case _ => ""
    }
    val batchStr = batchType.map(bt => s"[${bt.name}]").getOrElse("")
    val ranges = Seq(versionRange, timestampRange).filter(_.nonEmpty)
    s"CDF$batchStr(${ranges.mkString(", ")})"
  }
}

object CDFInfo {
  // CDF metadata column names
  val CHANGE_TYPE_COLUMN = "_change_type"
  val COMMIT_VERSION_COLUMN = "_commit_version"
  val COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp"

  /**
   * Returns the CDF metadata schema that should be appended to the data schema.
   */
  def cdfMetadataSchema: StructType = StructType(Seq(
    StructField(CHANGE_TYPE_COLUMN, StringType, nullable = false),
    StructField(COMMIT_VERSION_COLUMN, LongType, nullable = false),
    StructField(COMMIT_TIMESTAMP_COLUMN, TimestampType, nullable = false)
  ))

  /**
   * Creates CDFInfo from version range.
   */
  def fromVersionRange(
      startVersion: Long,
      endVersion: Option[Long] = None,
      rowIdColumns: Seq[String] = Seq.empty): CDFInfo = {
    CDFInfo(
      startVersion = Some(startVersion),
      endVersion = endVersion,
      rowIdColumns = rowIdColumns
    )
  }

  /**
   * Creates CDFInfo from timestamp range.
   */
  def fromTimestampRange(
      startTimestamp: Long,
      endTimestamp: Option[Long] = None,
      rowIdColumns: Seq[String] = Seq.empty): CDFInfo = {
    CDFInfo(
      startTimestamp = Some(startTimestamp),
      endTimestamp = endTimestamp,
      rowIdColumns = rowIdColumns
    )
  }
}

