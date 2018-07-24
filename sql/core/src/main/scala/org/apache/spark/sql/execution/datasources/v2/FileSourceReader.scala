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

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FileSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends DataSourceReader
  with SupportsScanUnsafeRow
  with SupportsPushDownRequiredColumns
  with SupportsPushDownCatalystFilters {
  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(files: Seq[FileStatus]): Option[StructType]

  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  /**
   * Returns a function that can be used to read a single file in as an [[InputPartitionReader]] of
   * [[UnsafeRow]].
   */
  def unsafeInputPartitionReader: PartitionedFile => InputPartitionReader[UnsafeRow]

  protected val sparkSession = SparkSession.getActiveSession
    .getOrElse(SparkSession.getDefaultSession.get)
  protected val hadoopConf =
    sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
  protected val sqlConf = sparkSession.sessionState.conf

  protected val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  protected val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  protected val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private lazy val rootPathsSpecified = {
    val filePaths = options.paths()
    if (filePaths.isEmpty) {
      throw new AnalysisException("Reading data source requires a" +
        " path (e.g. data backed by a local or distributed file system).")
    }
    DataSource.checkAndGlobPathIfNecessary(filePaths, hadoopConf,
      checkEmptyGlobPath = false, checkFilesExist = false)
  }

  protected lazy val fileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(sparkSession, rootPathsSpecified,
      options.asMap().asScala.toMap, userSpecifiedSchema, fileStatusCache)
  }

  protected lazy val partitionSchema = fileIndex.partitionSchema

  protected lazy val dataSchema = userSpecifiedSchema.orElse {
    inferSchema(fileIndex.allFiles())
  }.getOrElse {
    throw new AnalysisException(
      s"Unable to infer schema for $rootPathsSpecified. It must be specified manually.")
  }
  protected val (fullSchema, _) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema, partitionSchema, isCaseSensitive)
  protected var requiredSchema = fullSchema
  protected var partitionFilters: Array[Expression] = Array.empty
  protected var pushedFiltersArray: Array[Expression] = Array.empty

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, Seq.empty)
    val maxSplitBytes = PartitionedFileUtil.maxSplitBytes(sparkSession, selectedPartitions)
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    FilePartitionUtil.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = Array.empty

  override def pushedCatalystFilters(): Array[Expression] = {
    pushedFiltersArray
  }

  override def planUnsafeInputPartitions: JList[InputPartition[UnsafeRow]] = {
    partitions.map { filePartition =>
      new FileInputPartition[UnsafeRow](filePartition, unsafeInputPartitionReader,
        ignoreCorruptFiles, ignoreMissingFiles)
        .asInstanceOf[InputPartition[UnsafeRow]]
    }.asJava
  }
}

abstract class ColumnarBatchFileSourceReader(
    options: DataSourceOptions,
    userSpecifiedSchema: Option[StructType])
  extends FileSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  with SupportsScanColumnarBatch {
  /**
   * Returns a function that can be used to read a single file in as an [[InputPartitionReader]] of
   * [[ColumnarBatch]].
   */
  def columnarBatchInputPartitionReader: PartitionedFile => InputPartitionReader[ColumnarBatch]

  override def planBatchInputPartitions(): JList[InputPartition[ColumnarBatch]] = {
    partitions.map { filePartition =>
      new FileInputPartition[ColumnarBatch](filePartition, columnarBatchInputPartitionReader,
        ignoreCorruptFiles, ignoreMissingFiles)
        .asInstanceOf[InputPartition[ColumnarBatch]]
    }.asJava
  }
}
