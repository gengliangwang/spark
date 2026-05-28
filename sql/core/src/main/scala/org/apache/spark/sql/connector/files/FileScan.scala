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

package org.apache.spark.sql.connector.files

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Batch, Scan}

/**
 * An internal DSv2 [[Scan]] for connectors that delegate execution to Spark's V1 file scan path.
 *
 * Instead of producing DSv2 `Batch` / `MicroBatchStream` directly, a `FileScan` produces a
 * [[FileBatch]] or [[FileMicroBatchStream]] that enumerates `FileSet`s. A planner strategy
 * converts each `FileSet` into a `HadoopFsRelation` and lowers it via `FileSourceScanExec`,
 * which keeps existing physical optimizations (e.g. Photon) intact.
 *
 * Filters are tracked as Catalyst expressions because the lowering target (`FileSourceScanExec`)
 * consumes Catalyst, and round-tripping through the DSv2 `Predicate` surface would be lossy.
 */
trait FileScan extends Scan {

  /** Filters pushed down to partition pruning (applied during file listing). */
  def partitionFilters: Seq[Expression]

  /** Filters pushed down to data file reading (applied per row group / stripe / page). */
  def dataFilters: Seq[Expression]

  /** Returns the batch view of this scan. */
  def toFileBatch(): FileBatch

  /** Returns the micro-batch streaming view of this scan. */
  def toFileMicroBatchStream(checkpointLocation: String): FileMicroBatchStream

  override def toBatch: Batch =
    throw new UnsupportedOperationException(
      "FileScan lowers through toFileBatch(), not toBatch()")
}
