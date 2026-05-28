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

package org.apache.spark.sql.execution.datasources.v2.hackparquet

import org.apache.spark.SparkContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.files.FileWrite
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormat, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

/**
 * A [[FileWrite]] backed by [[ParquetFileFormat]] that the new batch-write exec nodes can route
 * through `FileFormatWriter.write`. The hackathon prototype keeps things minimal: no partition
 * columns or bucketing, and the `path` option must be set by the caller (which is what
 * `df.write.format("hackparquet").save(path)` does).
 */
case class HackParquetFileWrite(
    options: Map[String, String],
    info: LogicalWriteInfo,
    dynamicPartitionOverwrite: Boolean) extends FileWrite {

  override val outputPath: String = options.getOrElse(
    "path",
    throw new IllegalArgumentException("`path` option is required for hackparquet writes"))

  override val fileFormat: FileFormat = new ParquetFileFormat

  override val commitProtocol: FileCommitProtocol = FileCommitProtocol.instantiate(
    SQLConf.get.fileCommitProtocolClass,
    info.queryId,
    outputPath,
    dynamicPartitionOverwrite)

  override val statsTrackers: Seq[WriteJobStatsTracker] = {
    val sc = SparkContext.getActive.getOrElse {
      throw new IllegalStateException("Active SparkContext required to build write stats tracker")
    }
    val hadoopConf = sc.hadoopConfiguration
    Seq(new BasicWriteJobStatsTracker(
      new SerializableConfiguration(hadoopConf),
      BasicWriteJobStatsTracker.metrics))
  }

  // Prototype scope: no partition columns or bucketing yet.
  override val partitionColumns: Seq[Attribute] = Seq.empty
  override val bucketSpec: Option[BucketSpec] = None
  override val customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
  override val numStaticPartitionCols: Int = 0
}
