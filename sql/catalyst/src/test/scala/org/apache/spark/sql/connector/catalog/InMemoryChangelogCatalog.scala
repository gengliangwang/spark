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

package org.apache.spark.sql.connector.catalog

import java.util

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, MICRO_BATCH_READ}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An [[InMemoryTableCatalog]] whose loaded tables implement [[SupportsChangelog]].
 *
 * Change rows can be pre-populated via [[addChangeRows()]] before querying.
 */
class InMemoryChangelogCatalog extends InMemoryCatalog {

  // tableName -> list of change rows (each row: Array[Any] matching changelog schema)
  private val changeData: mutable.Map[String, mutable.ArrayBuffer[InternalRow]] =
    mutable.Map.empty

  // Records the derived changelog's context and scan options independently of base-table loads.
  private var _lastChangelogContext: Option[ChangelogContext] = None
  def lastChangelogContext: Option[ChangelogContext] = _lastChangelogContext

  private var _lastScanOptions: Option[CaseInsensitiveStringMap] = None
  def lastScanOptions: Option[CaseInsensitiveStringMap] = _lastScanOptions

  // CDC metadata changes must invalidate captured base handles in the shared relation cache.
  private val changelogVersions: mutable.Map[String, Long] = mutable.Map.empty

  // Per-table overrides for Changelog properties (carry-over rows, intermediate changes,
  // update representation, row identity). Tests can set these to exercise post-processing.
  private val changelogProperties: mutable.Map[String, ChangelogProperties] =
    mutable.Map.empty

  /**
   * Override the [[Changelog]] properties returned for a given table.
   * Defaults are: containsCarryoverRows=false, containsIntermediateChanges=false,
   * representsUpdateAsDeleteAndInsert=false, no rowId, no rowVersion.
   */
  def setChangelogProperties(
      ident: Identifier,
      properties: ChangelogProperties): Unit = {
    changelogProperties(ident.toString) = properties
    advanceChangelogVersion(ident)
  }

  override def loadTable(
      ident: Identifier,
      context: TableContext,
      stateOptions: CaseInsensitiveStringMap): Table = {
    val table = super.loadTable(ident, context, stateOptions).asInstanceOf[InMemoryTable]
    val snapshot = table.copy().asInstanceOf[InMemoryTable]
    val live = liveTable(ident).asInstanceOf[SupportsWrite]
    val allRows = changeData.getOrElseUpdate(ident.toString, mutable.ArrayBuffer.empty)
    def currentRows: Seq[InternalRow] = allRows.synchronized {
      allRows.iterator.map(_.copy()).toVector
    }
    val capturedRows = currentRows
    val props = changelogProperties.getOrElse(ident.toString, ChangelogProperties())
    val capturedVersion =
      s"${snapshot.version()}:${changelogVersions.getOrElse(ident.toString, 0L)}"

    new SupportsRead with SupportsWrite with SupportsChangelog with SupportsMetadataColumns {
      override def name(): String = snapshot.name
      override def id(): String = snapshot.id
      override def version(): String = capturedVersion
      override def columns(): Array[Column] = snapshot.columns()
      override def partitioning(): Array[Transform] = snapshot.partitioning
      override def properties(): util.Map[String, String] = snapshot.properties
      override def constraints(): Array[Constraint] = snapshot.constraints
      override def capabilities(): util.Set[TableCapability] = snapshot.capabilities()
      override def metadataColumns(): Array[MetadataColumn] = snapshot.metadataColumns
      override def canRenameConflictingMetadataColumns(): Boolean =
        snapshot.canRenameConflictingMetadataColumns

      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        snapshot.newScanBuilder(options)
      }

      override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        live.newWriteBuilder(info)
      }

      override def newChangelog(changelogContext: ChangelogContext): Changelog = {
        _lastChangelogContext = Some(changelogContext)
        // _commit_version follows the data columns and _change_type.
        val commitVersionIdx = snapshot.columns().length + 1
        val range = changelogContext.range()
        new InMemoryChangelog(
          snapshot.name + "_changelog",
          snapshot.columns(),
          filterByRange(capturedRows, commitVersionIdx, range),
          props,
          Some(() => filterByRange(currentRows, commitVersionIdx, range)),
          options => _lastScanOptions = Some(options))
      }
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    val dropped = super.dropTable(ident)
    if (dropped) {
      changeData.remove(ident.toString)
      changelogProperties.remove(ident.toString)
      changelogVersions.remove(ident.toString)
    }
    dropped
  }

  private def advanceChangelogVersion(ident: Identifier): Unit = {
    val key = ident.toString
    changelogVersions(key) = changelogVersions.getOrElse(key, 0L) + 1
  }

  /**
   * Filter rows by the requested [[ChangelogRange]]. For [[VersionRange]], compares
   * the `_commit_version` (Long) against the start/end versions (parsed as Long).
   * For [[UnboundedRange]], returns all rows.
   */
  private def filterByRange(
      rows: Seq[InternalRow],
      commitVersionIdx: Int,
      range: ChangelogRange): Seq[InternalRow] = range match {
    case vr: VersionRange =>
      val startVer = vr.startingVersion().toLong
      val startInc = vr.startingBoundInclusive()
      val endVerOpt = if (vr.endingVersion().isPresent) {
        Some(vr.endingVersion().get().toLong)
      } else None
      val endInc = vr.endingBoundInclusive()
      rows.filter { row =>
        val ver = row.getLong(commitVersionIdx)
        val aboveStart = if (startInc) ver >= startVer else ver > startVer
        val belowEnd = endVerOpt match {
          case Some(endVer) => if (endInc) ver <= endVer else ver < endVer
          case None => true
        }
        aboveStart && belowEnd
      }
    case _: TimestampRange =>
      // Timestamp filtering not implemented in test catalog
      rows
    case _: UnboundedRange =>
      rows
  }

  /**
   * Add change rows for a table. Each row should match the changelog schema:
   * (data columns..., _change_type STRING, _commit_version LONG, _commit_timestamp LONG).
   */
  def addChangeRows(ident: Identifier, rows: Seq[InternalRow]): Unit = {
    val buf = changeData.getOrElseUpdate(
      ident.toString, mutable.ArrayBuffer.empty)
    buf.synchronized {
      buf ++= rows
    }
    advanceChangelogVersion(ident)
  }

  def clearChangeRows(ident: Identifier): Unit = {
    changeData.remove(ident.toString)
    advanceChangelogVersion(ident)
  }
}

/**
 * Configurable properties for [[InMemoryChangelog]] that test cases can use to exercise
 * Spark's post-processing (carry-over removal, update detection, net changes).
 *
 * @param containsCarryoverRows whether the change stream may contain identical CoW pairs
 * @param containsIntermediateChanges whether multiple changes per row may exist
 * @param representsUpdateAsDeleteAndInsert whether updates appear as raw delete+insert
 * @param rowIdNames optional row identity columns as top-level names (e.g. Seq("id"))
 * @param rowIdPaths optional row identity paths for nested struct fields
 *                   (e.g. Seq(Seq("payload", "id"))); takes precedence over rowIdNames
 * @param rowVersionName optional row version column (e.g. Some("row_commit_version"));
 *                       must be a per-row version that distinguishes carry-overs from
 *                       real updates. Do NOT pass the commit version, which is constant
 *                       within a partition and would cause every delete+insert pair to
 *                       look like a carry-over
 * @param commitTimestampNullable whether the connector declares `_commit_timestamp` as
 *                                nullable. Defaults to `true`. Tests that need to
 *                                exercise NullPropagation behaviour on a non-nullable
 *                                schema can set this to `false`.
 */
case class ChangelogProperties(
    containsCarryoverRows: Boolean = false,
    containsIntermediateChanges: Boolean = false,
    representsUpdateAsDeleteAndInsert: Boolean = false,
    rowIdNames: Seq[String] = Seq.empty,
    rowIdPaths: Seq[Seq[String]] = Seq.empty,
    rowVersionName: Option[String] = None,
    commitTimestampNullable: Boolean = true)

/**
 * A test [[Changelog]] that returns pre-populated change rows.
 *
 * Properties (carry-over presence, update representation, row identity) are configurable
 * via the [[ChangelogProperties]] parameter so tests can exercise different code paths
 * in Spark's post-processing analyzer rule.
 */
class InMemoryChangelog(
    tableName: String,
    dataColumns: Array[Column],
    changeRows: Seq[InternalRow],
    changelogProperties: ChangelogProperties = ChangelogProperties(),
    streamingRows: Option[() => Seq[InternalRow]] = None,
    onScan: CaseInsensitiveStringMap => Unit = _ => ()) extends Changelog {

  private val cdcColumns: Array[Column] = dataColumns ++ Array(
    Column.create("_change_type", StringType),
    Column.create("_commit_version", LongType),
    Column.create("_commit_timestamp", TimestampType, changelogProperties.commitTimestampNullable))

  override def name(): String = tableName

  override def columns(): Array[Column] = cdcColumns

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, MICRO_BATCH_READ)
  }

  override def containsCarryoverRows(): Boolean = changelogProperties.containsCarryoverRows

  override def containsIntermediateChanges(): Boolean =
    changelogProperties.containsIntermediateChanges

  override def representsUpdateAsDeleteAndInsert(): Boolean =
    changelogProperties.representsUpdateAsDeleteAndInsert

  override def rowId(): Array[NamedReference] = {
    if (changelogProperties.rowIdPaths.nonEmpty) {
      changelogProperties.rowIdPaths.map(parts => FieldReference(parts): NamedReference).toArray
    } else {
      changelogProperties.rowIdNames.map { name =>
        FieldReference.column(name): NamedReference
      }.toArray
    }
  }

  override def rowVersion(): NamedReference = changelogProperties.rowVersionName match {
    case Some(name) => FieldReference.column(name)
    case None => super.rowVersion()
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap): ScanBuilder = {
    onScan(options)
    new InMemoryChangelogScanBuilder(
      readSchema, changeRows, streamingRows.getOrElse(() => changeRows))
  }

  def readSchema: StructType = {
    CatalogV2Util.v2ColumnsToStructType(cdcColumns)
  }
}

private class InMemoryChangelogScanBuilder(
    schema: StructType,
    rows: Seq[InternalRow],
    streamingRows: () => Seq[InternalRow]) extends ScanBuilder {
  override def build(): Scan =
    new InMemoryChangelogScan(schema, rows, streamingRows)
}

private class InMemoryChangelogScan(
    schema: StructType,
    rows: Seq[InternalRow],
    streamingRows: () => Seq[InternalRow]) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new InMemoryChangelogMicroBatchStream(schema, streamingRows)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    Array(InMemoryChangelogPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new InMemoryChangelogReaderFactory()
  }
}

private case class InMemoryChangelogPartition(
    rows: Seq[InternalRow]) extends InputPartition

private class InMemoryChangelogReaderFactory
    extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    new InMemoryChangelogReader(
      partition.asInstanceOf[InMemoryChangelogPartition])
  }
}

private class InMemoryChangelogReader(
    partition: InMemoryChangelogPartition)
    extends PartitionReader[InternalRow] {

  private var index = -1
  private val rows = partition.rows

  override def next(): Boolean = {
    index += 1
    index < rows.size
  }

  override def get(): InternalRow = rows(index)

  override def close(): Unit = {}
}

/**
 * A simple offset for [[InMemoryChangelogMicroBatchStream]].
 * The offset value represents the number of rows consumed so far.
 */
private class InMemoryChangelogOffset(val offset: Long) extends Offset {
  override def json(): String = offset.toString
}

/**
 * A [[MicroBatchStream]] that also discovers rows appended after the base table was loaded.
 */
private class InMemoryChangelogMicroBatchStream(
    schema: StructType,
    rows: () => Seq[InternalRow]) extends MicroBatchStream {

  override def initialOffset(): Offset = new InMemoryChangelogOffset(-1)

  override def latestOffset(): Offset = new InMemoryChangelogOffset(rows().size - 1)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startIdx = start.asInstanceOf[InMemoryChangelogOffset].offset.toInt + 1
    val endIdx = end.asInstanceOf[InMemoryChangelogOffset].offset.toInt + 1
    val slice = rows().slice(startIdx, endIdx)
    Array(InMemoryChangelogPartition(slice))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new InMemoryChangelogReaderFactory()
  }

  override def deserializeOffset(json: String): Offset = {
    new InMemoryChangelogOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}
}
