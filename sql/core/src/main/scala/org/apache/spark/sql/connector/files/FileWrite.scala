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

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.execution.datasources.{FileFormat, WriteJobStatsTracker}

/**
 * An internal DSv2 `Write` for connectors that delegate execution to Spark's V1 file write
 * path (`FileFormatWriter.write`). The new batch/streaming write exec nodes route this kind of
 * `Write` through `FileFormatWriter`, so connectors don't have to implement DSv2 `BatchWrite` /
 * `StreamingWrite` and per-task writer factories themselves.
 *
 * Output columns are intentionally omitted: the exec node uses the planned `query.output`,
 * which is already aligned by the `V2Writes` rule and avoids ExprId mismatches.
 */
trait FileWrite extends Write {

  def commitProtocol: FileCommitProtocol
  def fileFormat: FileFormat
  def options: Map[String, String]
  def statsTrackers: Seq[WriteJobStatsTracker]
  def outputPath: String
  def partitionColumns: Seq[Attribute]
  def bucketSpec: Option[BucketSpec]
  def customPartitionLocations: Map[TablePartitionSpec, String]
  def numStaticPartitionCols: Int

  override def toBatch: BatchWrite =
    throw new UnsupportedOperationException(
      "FileWrite is executed via FileFormatWriter, not BatchWrite")
}
