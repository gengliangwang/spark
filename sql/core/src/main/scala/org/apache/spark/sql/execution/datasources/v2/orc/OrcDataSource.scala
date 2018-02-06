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
package org.apache.spark.sql.execution.datasources.v2.orc

import java.net.URI
import java.util.{ArrayList, List => JList}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitionDirectory, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.orc.{OrcDeserializer, OrcUtils}
import org.apache.spark.sql.execution.streaming.sources.RateStreamSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class OrcDataSource(
    sparkSession: SparkSession,
    files: Seq[FileStatus])
  extends DataSourceV2
  with ReadSupport
  with DataSourceRegister {
  /**
   * Creates a {@link DataSourceReader} to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new OrcDataSourceReader(sparkSession, files, options)
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "orc"
}

class OrcDataSourceReader(
    sparkSession: SparkSession,
    files: Seq[FileStatus],
    options: DataSourceOptions)
  extends DataSourceReader
  with SupportsScanColumnarBatch
  with SupportsScanUnsafeRow {

  private val conf = sparkSession.sessionState.newHadoopConfWithOptions(Map.empty)
  private val numPartitions = options.get(RateStreamSourceV2.NUM_PARTITIONS).orElse("5").toInt
  private val fileIndex =
    new InMemoryFileIndex(sparkSession, files.map(_.getPath), Map.empty, None)
  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def readSchema(): StructType = {
    OrcUtils.readSchema(sparkSession, files).getOrElse {
      throw new AnalysisException(
        s"Unable to infer schema for Orc. It must be specified manually.")
    }
  }

  /**
   * Similar to {@link DataSourceReader#createDataReaderFactories()}, but returns columnar data
   * in batches.
   */
  override def createBatchDataReaderFactories(): JList[DataReaderFactory[ColumnarBatch]] = {
    new ArrayList[DataReaderFactory[ColumnarBatch]]
  }

  /**
   * Similar to {@link DataSourceReader#createDataReaderFactories()},
   * but returns data in unsafe row format.
   */
  override def createUnsafeRowReaderFactories: JList[DataReaderFactory[UnsafeRow]] = {
    val selectedPartitions =
      PartitionDirectory(InternalRow.empty, files.filter(f => isDataPath(f.getPath))) :: Nil
    val defaultMaxSplitBytes =
      sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        (0L until file.getLen by maxSplitBytes).map { offset =>
          val remaining = file.getLen - offset
          val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
          val hosts = getBlockHosts(blockLocations, offset, size)
          PartitionedFile(
            partition.values, file.getPath.toUri.toString, offset, size, hosts)
        }
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    splitFiles.map { partitionedFile =>
      new OrcUnsafeRowReaderFactory(
        sparkSession, partitionedFile, readSchema(), readSchema(), conf)
        .asInstanceOf[DataReaderFactory[UnsafeRow]]
    }.asJava
  }

  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }
  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }

  override def enableBatchRead(): Boolean = {
    false
  }
}

case class OrcUnsafeRowReaderFactory(
    sparkSession: SparkSession,
    file: PartitionedFile,
    dataSchema: StructType,
    requiredSchema: StructType,
    conf: Configuration)
  extends DataReaderFactory[UnsafeRow] with DataReader[UnsafeRow] {
  val filePath = new Path(new URI(file.filePath))
  val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
  val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
  val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)
  val orcRecordReader = new OrcInputFormat[OrcStruct]
    .createRecordReader(fileSplit, taskAttemptContext)
  val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
  val fullSchema = requiredSchema.toAttributes
  val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
  val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  val fs = filePath.getFileSystem(conf)
  val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
  val reader = OrcFile.createReader(filePath, readerOptions)
  val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
    isCaseSensitive, dataSchema, requiredSchema, reader, conf)
  val requestedColIds = requestedColIdsOrEmptyFile.get
  assert(requestedColIds.length == requiredSchema.length,
    "[BUG] requested column IDs do not match required schema")
  val deserializer = new OrcDeserializer(dataSchema, requiredSchema, requestedColIds)
  val taskConf = new Configuration(conf)
  taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
    requestedColIds.filter(_ != -1).sorted.mkString(","))

  /**
   * Returns a data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createDataReader(): DataReader[UnsafeRow] = this

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = iter.hasNext

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): UnsafeRow = {
    unsafeProjection(deserializer.deserialize(iter.next()))
  }

  override def close(): Unit = {
    iter.close()
  }
}

object OrcDataSource {
  val NUM_PARTITIONS = "numPartitions"
}
