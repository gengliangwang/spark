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

import java.util.{Map => JMap, Set => JSet}

import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext, Column, SupportsChangelog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An internal wrapper that retains the base table and context used to derive a [[Changelog]].
 * This lets metadata refresh reload the base table before deriving the changelog again.
 *
 * This class is NOT part of the connector API. Connectors implement [[Changelog]]; Spark
 * wraps it in [[ChangelogTable]] during analysis.
 */
case class ChangelogTable(
    baseTable: Table,
    changelog: Changelog,
    changelogContext: ChangelogContext,
    resolved: Boolean = false) extends Table with SupportsRead {

  // Validate that the connector returned a schema with the required CDC metadata columns
  // and correct types.
  ChangelogTable.validateSchema(changelog)

  private val postProcessingMetadata = ChangelogTable.capturePostProcessingMetadata(changelog)

  override def name: String = changelog.name

  override def id: String = baseTable.id

  override def version: String = baseTable.version

  override def columns: Array[Column] = changelog.columns

  override def partitioning: Array[Transform] = changelog.partitioning

  override def properties: JMap[String, String] = changelog.properties

  override def constraints: Array[Constraint] = changelog.constraints

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    changelog.newScanBuilder(options)
  }

  override def capabilities: JSet[TableCapability] = changelog.capabilities

  // Deriving a new Changelog does not change the read selected by the base state and context.
  override def equals(other: Any): Boolean = other match {
    case that: ChangelogTable =>
      that.canEqual(this) && baseTable == that.baseTable &&
        changelogContext == that.changelogContext && resolved == that.resolved
    case _ => false
  }

  override def hashCode(): Int = (baseTable, changelogContext, resolved).hashCode()

  /** Checks that refreshing the changelog preserves the already analyzed CDC rewrites. */
  def validateRefresh(current: ChangelogTable): Unit = {
    val errors = postProcessingMetadata.changes(current.postProcessingMetadata)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.changelogChangedAfterAnalysis(baseTable.name, errors)
    }
  }
}

object ChangelogTable {

  def create(baseTable: Table, context: ChangelogContext): ChangelogTable = {
    val changelog = baseTable match {
      case table: SupportsChangelog => table.newChangelog(context)
      case _ => throw QueryCompilationErrors.cdcUnsupportedOnRelationError(baseTable.name)
    }
    ChangelogTable(baseTable, changelog, context)
  }

  private case class PostProcessingMetadata(
      containsCarryoverRows: Boolean,
      containsIntermediateChanges: Boolean,
      representsUpdateAsDeleteAndInsert: Boolean,
      rowId: Seq[Seq[String]],
      rowVersion: Option[Seq[String]]) {

    def changes(current: PostProcessingMetadata): Seq[String] = {
      Seq(
        "containsCarryoverRows" -> (containsCarryoverRows != current.containsCarryoverRows),
        "containsIntermediateChanges" ->
          (containsIntermediateChanges != current.containsIntermediateChanges),
        "representsUpdateAsDeleteAndInsert" ->
          (representsUpdateAsDeleteAndInsert != current.representsUpdateAsDeleteAndInsert),
        "rowId" -> (rowId != current.rowId),
        "rowVersion" -> (rowVersion != current.rowVersion)).collect {
        case (name, true) => s"$name changed"
      }
    }
  }

  private def capturePostProcessingMetadata(cl: Changelog): PostProcessingMetadata = {
    val carryovers = cl.containsCarryoverRows()
    val intermediateChanges = cl.containsIntermediateChanges()
    val updatesAsDeleteAndInsert = cl.representsUpdateAsDeleteAndInsert()
    val rowId = if (carryovers || intermediateChanges || updatesAsDeleteAndInsert) {
      cl.rowId().toVector.map(_.fieldNames().toVector)
    } else {
      Seq.empty
    }
    val rowVersion = if (carryovers || updatesAsDeleteAndInsert) {
      Some(cl.rowVersion().fieldNames().toVector)
    } else {
      None
    }
    PostProcessingMetadata(
      carryovers, intermediateChanges, updatesAsDeleteAndInsert, rowId, rowVersion)
  }

  private[v2] def validateSchema(cl: Changelog): Unit = {
    val byName = cl.columns.map(c => c.name -> c).toMap
    def check(name: String, expected: DataType*): Unit = {
      val col = byName.getOrElse(name,
        throw QueryCompilationErrors.changelogMissingColumnError(cl.name, name))
      if (expected.nonEmpty && col.dataType != expected.head) {
        throw QueryCompilationErrors.changelogInvalidColumnTypeError(
          cl.name, name, expected.head.sql, col.dataType.sql)
      }
    }
    check("_change_type", StringType)
    // `_commit_version` must be either `LongType` or `StringType`. Connectors must
    // additionally guarantee that the column's natural ordering (numeric /
    // lexicographic) matches commit order, because the netChanges post-processing path
    // sorts rows by this column. These two types cover every realistic CDC source;
    // broader atomic types like `IntegerType` are strict subsets of `LongType`, and
    // `TimestampType` duplicates the role of `_commit_timestamp`. The narrower
    // contract can always be relaxed later (relaxing is non-breaking; restricting is
    // not).
    val versionCol = byName.getOrElse("_commit_version",
      throw QueryCompilationErrors.changelogMissingColumnError(cl.name, "_commit_version"))
    if (versionCol.dataType != LongType && versionCol.dataType != StringType) {
      throw QueryCompilationErrors.changelogInvalidColumnTypeError(
        cl.name, "_commit_version", "BIGINT or STRING", versionCol.dataType.sql)
    }
    check("_commit_timestamp", TimestampType)

    // Only call `rowId()` / `rowVersion()` when a capability requires them; a connector
    // that advertises a capability without overriding the method surfaces the default
    // UnsupportedOperationException directly.
    val needsRowId = cl.containsCarryoverRows() ||
      cl.representsUpdateAsDeleteAndInsert() ||
      cl.containsIntermediateChanges()
    if (needsRowId) {
      val rowIds = cl.rowId()
      if (rowIds == null || rowIds.isEmpty) {
        throw QueryCompilationErrors.changelogMissingRowIdError(cl.name)
      }
    }
    val needsRowVersion = cl.containsCarryoverRows() ||
      cl.representsUpdateAsDeleteAndInsert()
    if (needsRowVersion) {
      cl.rowVersion()
    }
  }
}
