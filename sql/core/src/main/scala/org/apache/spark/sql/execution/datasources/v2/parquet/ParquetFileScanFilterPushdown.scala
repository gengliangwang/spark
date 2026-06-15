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

package org.apache.spark.sql.execution.datasources.v2.parquet

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils}
import org.apache.spark.sql.types.StructType

/**
 * Catalyst-filter splitting for [[ParquetFileScanBuilder]] (a Java class), shared so the Java
 * side does not have to juggle Scala collections. Mirrors `FileScanBuilder.pushFilters`:
 *
 *  - Partition predicates that can drive pruning (no subquery / Python UDF) are consumed at the
 *    DSv2 level and reported via `ParquetFileScan.partitionFilters()`. The planner re-applies
 *    them when lowering to `FileSourceScanExec`, so partition pruning is identical to V1.
 *  - Data predicates are reported via `pushedFilters()` / `ParquetFileScan.dataFilters()` but
 *    kept as post-scan filters: they are re-evaluated above the lowered scan and pushed into the
 *    parquet reader by `FileSourceStrategy`.
 *  - Non-deterministic predicates and partition predicates that cannot prune (subquery / Python
 *    UDF) are also kept as post-scan filters so nothing is dropped.
 */
private[parquet] object ParquetFileScanFilterPushdown {

  /** Outcome of splitting the pushed catalyst filters. Arrays are convenient for the Java side. */
  class Result(
      val postScanFilters: Seq[Expression],
      val partitionFilters: Array[Expression],
      val dataFilters: Array[Expression],
      val pushedFilters: Array[Predicate])

  def pushFilters(partitionSchema: StructType, filters: Seq[Expression]): Result = {
    val (deterministic, nonDeterministic) = filters.partition(_.deterministic)
    val (partitionFilters, dataFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, deterministic)
    // Python UDFs may still be present (this runs before ExtractPythonUDFs) and subqueries cannot
    // be evaluated during partition listing, so such partition predicates cannot prune. Keep them
    // as post-scan filters rather than dropping them.
    val (prunablePartitionFilters, nonPrunablePartitionFilters) = partitionFilters.partition { f =>
      !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])
    }
    // Translate data predicates to V2 predicates for reporting (pushedFilters). Best-effort: a
    // predicate the source cannot represent (e.g. a comparison against a whole `_metadata` struct,
    // whose value has no V2 literal) is skipped here. It still stays a post-scan filter, so this
    // only affects what is displayed, not correctness or pushdown.
    val pushable = mutable.ArrayBuffer.empty[Predicate]
    for (filterExpr <- dataFilters) {
      try {
        DataSourceStrategy.translateFilter(filterExpr, supportNestedPredicatePushdown = true)
          .foreach(f => pushable += f.toV2)
      } catch {
        case NonFatal(_) => // not representable as a pushed V2 predicate; reported via post-scan
      }
    }
    new Result(
      postScanFilters = dataFilters ++ nonPrunablePartitionFilters ++ nonDeterministic,
      partitionFilters = prunablePartitionFilters.toArray,
      dataFilters = dataFilters.toArray,
      pushedFilters = pushable.toArray)
  }
}
