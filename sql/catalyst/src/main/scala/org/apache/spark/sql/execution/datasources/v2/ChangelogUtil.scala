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

/** Creates and validates changelogs derived from captured base table states. */
private[sql] object ChangelogUtil {

  def create(baseTable: Table, context: ChangelogContext): Changelog = {
    val changelog = baseTable match {
      case table: SupportsChangelog => table.newChangelog(context)
      case _ => throw QueryCompilationErrors.cdcUnsupportedOnRelationError(baseTable.name)
    }
    validateSchema(changelog)
    changelog
  }

  /** Checks that refreshing the changelog preserves the already analyzed CDC rewrites. */
  def validateRefresh(captured: Changelog, current: Changelog): Unit = {
    def rowId(cl: Changelog): Seq[Seq[String]] = {
      if (cl.containsCarryoverRows() || cl.containsIntermediateChanges() ||
          cl.representsUpdateAsDeleteAndInsert()) {
        cl.rowId().toVector.map(_.fieldNames().toVector)
      } else {
        Seq.empty
      }
    }
    def rowVersion(cl: Changelog): Option[Seq[String]] = {
      if (cl.containsCarryoverRows() || cl.representsUpdateAsDeleteAndInsert()) {
        Some(cl.rowVersion().fieldNames().toVector)
      } else {
        None
      }
    }
    val errors = Seq(
      "containsCarryoverRows" ->
        (captured.containsCarryoverRows() != current.containsCarryoverRows()),
      "containsIntermediateChanges" ->
        (captured.containsIntermediateChanges() != current.containsIntermediateChanges()),
      "representsUpdateAsDeleteAndInsert" ->
        (captured.representsUpdateAsDeleteAndInsert() !=
          current.representsUpdateAsDeleteAndInsert()),
      "rowId" -> (rowId(captured) != rowId(current)),
      "rowVersion" -> (rowVersion(captured) != rowVersion(current))).collect {
      case (name, true) => s"$name changed"
    }
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.changelogChangedAfterAnalysis(
        captured.baseTable().name(), errors)
    }
  }

  def validateSchema(cl: Changelog): Unit = {
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
