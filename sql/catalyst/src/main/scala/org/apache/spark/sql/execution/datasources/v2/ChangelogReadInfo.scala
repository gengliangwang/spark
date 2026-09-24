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

import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext, SupportsChangelog, Table}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}

/**
 * Internal information about the base table and context used to derive a [[Changelog]].
 * The relation holds the connector's changelog directly, while this descriptor lets metadata
 * refresh reload the base table before deriving the changelog again.
 *
 * This descriptor does not retain the derived changelog instance. Equivalent base states,
 * contexts, and captured metadata identify the same read regardless of connector allocations.
 */
case class ChangelogReadInfo(
    baseTable: Table,
    context: ChangelogContext,
    postProcessingMetadata: ChangelogReadInfo.PostProcessingMetadata,
    resolved: Boolean = false) {

  /** Checks that refreshing the changelog preserves the already analyzed CDC rewrites. */
  def validateRefresh(current: ChangelogReadInfo): Unit = {
    val errors = postProcessingMetadata.changes(current.postProcessingMetadata)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.changelogChangedAfterAnalysis(baseTable.name, errors)
    }
  }
}

object ChangelogReadInfo {

  def create(baseTable: Table, context: ChangelogContext): (Changelog, ChangelogReadInfo) = {
    val changelog = baseTable match {
      case table: SupportsChangelog => table.newChangelog(context)
      case _ => throw QueryCompilationErrors.cdcUnsupportedOnRelationError(baseTable.name)
    }
    (changelog, fromChangelog(baseTable, changelog, context))
  }

  def fromChangelog(
      baseTable: Table,
      changelog: Changelog,
      context: ChangelogContext): ChangelogReadInfo = {
    validateSchema(changelog)
    ChangelogReadInfo(baseTable, context, capturePostProcessingMetadata(changelog))
  }

  case class PostProcessingMetadata(
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
