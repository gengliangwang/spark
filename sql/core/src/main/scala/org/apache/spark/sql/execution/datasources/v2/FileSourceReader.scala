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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.TaskContext
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.PartitioningUtils.columnNameEquality
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FileSourceMetaData(
    val options: DataSourceOptions,
    val userSpecifiedSchema: Option[StructType])
  extends Metadata
  with SupportsPushDownRequiredColumns
  with SupportsPushDownCatalystFilters {
  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(files: Seq[FileStatus]): Option[StructType]

  val sparkSession = SparkSession.getActiveSession
    .getOrElse(SparkSession.getDefaultSession.get)
  val hadoopConf =
    sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
  protected val sqlConf = sparkSession.sessionState.conf

  val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private lazy val rootPathsSpecified = {
    val filePaths = options.paths()
    if (filePaths.isEmpty) {
      throw new AnalysisException("Reading data source requires a" +
        " path (e.g. data backed by a local or distributed file system).")
    }
    DataSource.checkAndGlobPathIfNecessary(filePaths, hadoopConf,
      checkEmptyGlobPath = false, checkFilesExist = false)
  }

  lazy val fileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(sparkSession, rootPathsSpecified,
      options.asMap().asScala.toMap, userSpecifiedSchema, fileStatusCache)
  }

  lazy val partitionSchema = fileIndex.partitionSchema

  protected lazy val dataSchema = userSpecifiedSchema.orElse {
    inferSchema(fileIndex.allFiles())
  }.getOrElse {
    throw new AnalysisException(
      s"Unable to infer schema for $rootPathsSpecified. It must be specified manually.")
  }
  val (fullSchema, _) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema, partitionSchema, isCaseSensitive)
  protected var requiredSchema = fullSchema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def getSchema: StructType = requiredSchema
}

abstract class FileSourceSplitManager(metadata: Metadata) extends SplitManager {
  val fileSourceMetadata = metadata.asInstanceOf[FileSourceMetaData]
  val sparkSession = fileSourceMetadata.sparkSession
  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  override def getSplits: Array[InputSplit] = {
    val equality = columnNameEquality(fileSourceMetadata.isCaseSensitive)
    val partitionFilters = fileSourceMetadata match {
      case s: SupportsPushDownCatalystFilters =>
        val partitionSet = fileSourceMetadata.partitionSchema.toAttributes.map(_.name)
        s.pushedCatalystFilters().filter(
          _.references.forall(a => partitionSet.exists(p => equality(p, a.name))))
      case _ => Array.empty
    }
    val selectedPartitions = fileSourceMetadata.fileIndex.listFiles(partitionFilters, Seq.empty)
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
    FilePartitionUtil.getFilePartitions(sparkSession, splitFiles, maxSplitBytes).toArray
  }
}

class SplitableFileSourceSplitManager(meta: FileSourceMetaData)
  extends FileSourceSplitManager(meta) {
  override def isSplitable(path: Path): Boolean = true
}

abstract class PartitionReaderProvider extends Serializable {
  def createRowReader(split: PartitionedFile): InputPartitionReader[InternalRow]

  def createColumnarReader(split: PartitionedFile): InputPartitionReader[ColumnarBatch]

  def supportColumnarReader: Boolean
}

class FileSplitReaderProvider(
    reader: PartitionReaderProvider,
    fileSourceMetaData: FileSourceMetaData) extends SplitReaderProvider {
  private val ignoreCorruptFiles: Boolean = fileSourceMetaData.ignoreCorruptFiles
  private val ignoreMissingFiles: Boolean = fileSourceMetaData.ignoreMissingFiles

  override def createColumnarReader(split: InputSplit): InputPartitionReader[ColumnarBatch] = {
    val iter = split.asInstanceOf[FilePartition].files.iterator.map { f =>
      PartitionedFileReader(f, reader.createColumnarReader(f))
    }
    FileInputPartitionReader[ColumnarBatch](TaskContext.get(), iter,
      ignoreCorruptFiles, ignoreMissingFiles)
  }

  override def createRowReader(split: InputSplit): InputPartitionReader[InternalRow] = {
    val iter = split.asInstanceOf[FilePartition].files.iterator.map { f =>
      PartitionedFileReader(f, reader.createRowReader(f))
    }
    FileInputPartitionReader[InternalRow](TaskContext.get(), iter,
      ignoreCorruptFiles, ignoreMissingFiles)
  }

  override def supportColumnarReader(): Boolean = reader.supportColumnarReader
}

abstract class FileSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends DataSourceReader {

  override def getMetadata: FileSourceMetaData

  override def getSplitManager(meta: Metadata): FileSourceSplitManager

  def getPartitionReaderProvider(meta: Metadata): PartitionReaderProvider

  override def getReaderProvider(metadata: Metadata): SplitReaderProvider = {
    val fileSourceMetadata = metadata.asInstanceOf[FileSourceMetaData]
    new FileSplitReaderProvider(getPartitionReaderProvider(fileSourceMetadata), fileSourceMetadata)
  }

}

