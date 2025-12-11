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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, CaseWhen, Coalesce, EqualNullSafe, GreaterThan, IsNotNull, Literal, Not}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{CDFChangeType, CDFInfo, DataSourceV2Relation}
import org.apache.spark.sql.types.StringType

/**
 * Expands a [[DataSourceV2Relation]] with CDF info (but no batch type) into a full outer join
 * of two DataSourceV2Relations: one for added records and one for removed records.
 *
 * The expansion works as follows:
 * 1. Create two child relations from the original:
 *    - addedRelation: has cdfInfo.batchType = CDFAddedBatch
 *    - removedRelation: has cdfInfo.batchType = CDFRemovedBatch
 * 2. Perform a full outer join on the row ID columns
 * 3. Add a projection to compute the _change_type column based on join results and commit versions:
 *    - INSERT: row exists only in added batch (removed side is null), OR
 *              row exists in both batches but added version > removed version
 *    - DELETE: row exists only in removed batch (added side is null), OR
 *              row exists in both batches but removed version > added version
 *    - UPDATE_POSTIMAGE: row exists in both batches with equal versions
 *
 * The version comparison ensures that when a row has changes across multiple versions,
 * the final change type reflects the latest operation (e.g., a row inserted at version 1
 * and deleted at version 4 should be reported as deleted).
 */
object ExpandCDFRelation extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case r @ DataSourceV2Relation(_, _, _, _, _, _, Some(cdfInfo))
        if cdfInfo.batchType.isEmpty && cdfInfo.rowIdColumns.nonEmpty =>
      expandCDFRelation(r, cdfInfo)
  }

  private def expandCDFRelation(
      relation: DataSourceV2Relation,
      cdfInfo: CDFInfo): LogicalPlan = {
    // Create two child relations with appropriate batch types
    val addedRelation = relation.copy(
      output = relation.output.map(_.newInstance()),
      cdfInfo = Some(cdfInfo.forAddedBatch)
    )
    val removedRelation = relation.copy(
      output = relation.output.map(_.newInstance()),
      cdfInfo = Some(cdfInfo.forRemovedBatch)
    )

    // Build join condition on row ID columns
    val rowIdColNames = cdfInfo.rowIdColumns
    val addedRowIds = rowIdColNames.map(name =>
      addedRelation.output.find(_.name.equalsIgnoreCase(name)).getOrElse(
        throw new IllegalStateException(s"Row ID column $name not found in added relation")
      )
    )
    val removedRowIds = rowIdColNames.map(name =>
      removedRelation.output.find(_.name.equalsIgnoreCase(name)).getOrElse(
        throw new IllegalStateException(s"Row ID column $name not found in removed relation")
      )
    )

    val joinCondition = addedRowIds.zip(removedRowIds).map { case (added, removed) =>
      EqualNullSafe(added, removed)
    }.reduceLeft(And)

    // Create the full outer join
    val joined = Join(addedRelation, removedRelation, FullOuter, Some(joinCondition), JoinHint.NONE)

    // Build the change type expression:
    // - If removed is null -> INSERT
    // - If added is null -> DELETE
    // - If both exist, compare commit versions to determine final change type:
    //   - If added version > removed version -> INSERT (latest change was an add)
    //   - If removed version > added version -> DELETE (latest change was a removal)
    //   - If versions are equal -> UPDATE_POSTIMAGE
    val addedNotNull = IsNotNull(addedRowIds.head)
    val removedNotNull = IsNotNull(removedRowIds.head)

    // Get commit version columns for comparison
    val addedVersionCol = addedRelation.output.find(_.name == CDFInfo.COMMIT_VERSION_COLUMN)
    val removedVersionCol = removedRelation.output.find(_.name == CDFInfo.COMMIT_VERSION_COLUMN)

    val changeTypeExpr = (addedVersionCol, removedVersionCol) match {
      case (Some(addedVer), Some(removedVer)) =>
        // Both version columns exist - compare versions to determine change type
        CaseWhen(Seq(
          // Only added exists -> INSERT
          (And(addedNotNull, Not(removedNotNull)), Literal(CDFChangeType.INSERT)),
          // Only removed exists -> DELETE
          (And(Not(addedNotNull), removedNotNull), Literal(CDFChangeType.DELETE)),
          // Both exist: added version > removed version -> INSERT (latest was add)
          (And(And(addedNotNull, removedNotNull), GreaterThan(addedVer, removedVer)),
            Literal(CDFChangeType.INSERT)),
          // Both exist: removed version > added version -> DELETE (latest was removal)
          (And(And(addedNotNull, removedNotNull), GreaterThan(removedVer, addedVer)),
            Literal(CDFChangeType.DELETE))
        ), Some(Literal(CDFChangeType.UPDATE_POSTIMAGE)))
      case _ =>
        // Fallback: no version columns, use original logic
        CaseWhen(Seq(
          (And(addedNotNull, Not(removedNotNull)), Literal(CDFChangeType.INSERT)),
          (And(Not(addedNotNull), removedNotNull), Literal(CDFChangeType.DELETE))
        ), Some(Literal(CDFChangeType.UPDATE_POSTIMAGE)))
    }

    // Build the output projection
    // For data columns, coalesce added and removed (prefer added for insert/update)
    val dataColumns = relation.output.filterNot { attr =>
      attr.name == CDFInfo.CHANGE_TYPE_COLUMN ||
        attr.name == CDFInfo.COMMIT_VERSION_COLUMN ||
        attr.name == CDFInfo.COMMIT_TIMESTAMP_COLUMN
    }

    val coalescedDataCols = dataColumns.map { attr =>
      val addedCol = addedRelation.output.find(_.name == attr.name)
      val removedCol = removedRelation.output.find(_.name == attr.name)
      (addedCol, removedCol) match {
        case (Some(a), Some(r)) =>
          Alias(Coalesce(Seq(a, r)), attr.name)(attr.exprId)
        case (Some(a), None) =>
          Alias(a, attr.name)(attr.exprId)
        case (None, Some(r)) =>
          Alias(r, attr.name)(attr.exprId)
        case _ =>
          throw new IllegalStateException(s"Column ${attr.name} not found in either batch")
      }
    }

    // Get commit version and timestamp (coalesce from both sides)
    val addedVersion = addedRelation.output.find(_.name == CDFInfo.COMMIT_VERSION_COLUMN)
    val removedVersion = removedRelation.output.find(_.name == CDFInfo.COMMIT_VERSION_COLUMN)
    val commitVersionExpr = (addedVersion, removedVersion) match {
      case (Some(a), Some(r)) =>
        Alias(Coalesce(Seq(a, r)), CDFInfo.COMMIT_VERSION_COLUMN)()
      case (Some(a), None) => Alias(a, CDFInfo.COMMIT_VERSION_COLUMN)()
      case (None, Some(r)) => Alias(r, CDFInfo.COMMIT_VERSION_COLUMN)()
      case _ =>
        // If metadata columns are not present, they will be added by the scan
        Alias(Literal(null, org.apache.spark.sql.types.LongType), CDFInfo.COMMIT_VERSION_COLUMN)()
    }

    val addedTimestamp = addedRelation.output.find(_.name == CDFInfo.COMMIT_TIMESTAMP_COLUMN)
    val removedTimestamp = removedRelation.output.find(_.name == CDFInfo.COMMIT_TIMESTAMP_COLUMN)
    val commitTimestampExpr = (addedTimestamp, removedTimestamp) match {
      case (Some(a), Some(r)) =>
        Alias(Coalesce(Seq(a, r)), CDFInfo.COMMIT_TIMESTAMP_COLUMN)()
      case (Some(a), None) => Alias(a, CDFInfo.COMMIT_TIMESTAMP_COLUMN)()
      case (None, Some(r)) => Alias(r, CDFInfo.COMMIT_TIMESTAMP_COLUMN)()
      case _ =>
        Alias(
          Literal(null, org.apache.spark.sql.types.TimestampType),
          CDFInfo.COMMIT_TIMESTAMP_COLUMN
        )()
    }

    // Create the change type column
    val changeTypeCol = Alias(changeTypeExpr, CDFInfo.CHANGE_TYPE_COLUMN)()

    // Final projection
    val projections = coalescedDataCols ++ Seq(changeTypeCol, commitVersionExpr, commitTimestampExpr)

    Project(projections, joined)
  }
}

