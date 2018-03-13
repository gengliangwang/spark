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

import java.io.IOException
import java.net.URI
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory
import org.apache.spark.TaskContext

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, ExpressionSet, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitionDirectory, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReader, OrcDeserializer, OrcFilters, OrcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class OrcDataSource extends DataSourceV2 with ReadSupport {
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
    new OrcDataSourceReader(options)
  }
}

class OrcDataSourceReader(options: DataSourceOptions)
  extends DataSourceReader
  with SupportsScanColumnarBatch
  with SupportsScanUnsafeRow
  with SupportsPushDownCatalystFilters
  with SupportsPushDownRequiredColumns
  with SupportsReportPartitioning {

  private def sparkSession = SparkSession.getActiveSession
    .getOrElse(SparkSession.getDefaultSession.get)

  private def filePath = new Path(options.get(OrcDataSource.PATH).orElse("."))
  private def fileIndex =
    new InMemoryFileIndex(sparkSession, Seq(filePath), options.asMap().asScala.toMap, None)
  private def files = fileIndex.allFiles()
  private def selectedPartitions = fileIndex.listFiles(pushedCatalystFilters(), Seq.empty)
  private def maxSplitBytes: Long = {
    val defaultMaxSplitBytes =
      sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism
    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
  private def splitFiles = selectedPartitions.flatMap { partition =>
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
  private val partitionSchema = fileIndex.partitionSchema
  var requiredSchema: Option[StructType] = None
  var pushedFiltersArray: Array[Expression] = Array.empty
  private val hadoopConf =
    sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)

  private val dataSchema: StructType = {
    OrcUtils.readSchema(sparkSession, files).getOrElse {
      throw new AnalysisException(
        s"Unable to infer schema for Orc. It must be specified manually.")
    }
  }
  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def readSchema(): StructType = {
    requiredSchema.getOrElse(dataSchema)
  }

  /**
   * Similar to {@link DataSourceReader#createDataReaderFactories()}, but returns columnar data
   * in batches.
   */
  override def createBatchDataReaderFactories(): JList[DataReaderFactory[ColumnarBatch]] = {
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val capacity = sqlConf.orcVectorizedReaderBatchSize
    val copyToSpark = sparkSession.sessionState.conf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK)
//    val hadoopConf =
//      sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    splitFiles.map { partitionedFile =>
      new OrcBatchDataReaderFactory(partitionedFile, dataSchema, partitionSchema,
        dataSchema, enableOffHeapColumnVector, copyToSpark, capacity, broadcastedConf)
        .asInstanceOf[DataReaderFactory[ColumnarBatch]]
    }.asJava
  }

  /**
   * Similar to {@link DataSourceReader#createDataReaderFactories()},
   * but returns data in unsafe row format.
   */
  override def createUnsafeRowReaderFactories: JList[DataReaderFactory[UnsafeRow]] = {
//    val hadoopConf =
//      sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    splitFiles.map { partitionedFile =>
      new OrcUnsafeRowReaderFactory(
        partitionedFile, dataSchema, partitionSchema, readSchema(), broadcastedConf)
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
    val conf = sparkSession.sessionState.conf
    val schema = readSchema() ++ partitionSchema
    conf.orcVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = Some(requiredSchema)
  }

//  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//    if (true) {
//      filters
//    } else {
//      pushedFiltersArray.clear()
//      val dataTypeMap = dataSchema.map(f => f.name -> f.dataType).toMap
//      val ret = mutable.ArrayBuffer[Filter]()
//      filters.foreach { f =>
//    val arg = OrcFilters.buildSearchArgument(dataTypeMap, f, SearchArgumentFactory.newBuilder())
//        if (arg.isDefined) {
//          pushedFiltersArray += f
//        }
//      }
// //    val searchArgument = for {
// //    // Combines all convertible filters using `And` to produce a single conjunction
// //      conjunction <- pushedFiltersArray.reduceOption(org.apache.spark.sql.sources.And)
// //      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
//    builder <- buildSearchArgument(dataTypeMap, conjunction, SearchArgumentFactory.newBuilder())
// //    } yield builder.build()
//
//      OrcFilters.createFilter(dataSchema, filters).foreach { f =>
//        OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
//      }
//      filters
//    }
//  }
//
//  override def pushedFilters(): Array[Filter] = {
//    pushedFiltersArray.toArray
//  }

  override def outputPartitioning(): Partitioning = {
    val bucketing = options.get("bucket")
    if (bucketing.isPresent) {
      PartitionFromJson.fromJson(bucketing.get())
    } else {
      EmptyPartition
    }
  }

  override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = {
    if (true) {
      filters
    } else {
      val partitionColumnNames = partitionSchema.toAttributes.map(_.name).toSet
      val (partitionKeyFilters, otherFilters) = filters.partition {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      }
      pushedFiltersArray = partitionKeyFilters
      otherFilters
    }
  }

  override def pushedCatalystFilters(): Array[Expression] = {
    pushedFiltersArray
  }
}

object PartitionFromJson {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  case class Bucket(number: Int, columns: Array[String]) extends Partitioning {
    override def numPartitions(): Int = number

    override def satisfy(distribution: Distribution): Boolean = distribution match {
      case c: ClusteredDistribution => columns.forall(c.clusteredColumns.contains(_))
      case _ => false
    }
  }

  def fromJson(json: String)(implicit m : Manifest[Bucket]): Partitioning = {
    mapper.readValue[Bucket](json)
  }
}

object EmptyPartition extends Partitioning {
  override def numPartitions(): Int = 0

  override def satisfy(distribution: Distribution): Boolean = false
}

case class OrcUnsafeRowDataReader(
    iter: RecordReaderIterator[OrcStruct],
    fullSchema: Seq[Attribute],
    deserializer: OrcDeserializer)
  extends DataReader[UnsafeRow] {
  private lazy val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
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

case class OrcColumnarBatchDataReader(iter: RecordReaderIterator[ColumnarBatch])
  extends DataReader[ColumnarBatch] {
  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    iter.nonEmpty && iter.hasNext
  }

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): ColumnarBatch = {
    iter.next()
  }

  override def close(): Unit = {
    iter.close()
  }
}

object EmptyOrcColumnarBatchDataReader extends DataReader[ColumnarBatch] {
  override def next(): Boolean = false

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): ColumnarBatch = throw new IOException("No record should be returned")

  override def close(): Unit = {}
}

case class OrcBatchDataReaderFactory(
    file: PartitionedFile,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    enableOffHeapColumnVector: Boolean,
    copyToSpark: Boolean,
    capacity: Int,
    broadcastedConf: Broadcast[SerializableConfiguration])
  extends DataReaderFactory[ColumnarBatch] {
  override def createDataReader(): DataReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))
    val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)
    val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
      true, dataSchema, requiredSchema, reader, conf)
    if (requestedColIdsOrEmptyFile.isEmpty) {
      EmptyOrcColumnarBatchDataReader
    } else {
      val requestedColIds = requestedColIdsOrEmptyFile.get
      assert(requestedColIds.length == requiredSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskContext = Option(TaskContext.get())
      val batchReader = new OrcColumnarBatchReader(
        enableOffHeapColumnVector && taskContext.isDefined, copyToSpark, capacity)

      val iter = new RecordReaderIterator(batchReader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

      val taskConf = new Configuration(conf)
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
        requestedColIds.filter(_ != -1).sorted.mkString(","))
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)
      val partitionColumnNames = partitionSchema.toAttributes.map(_.name).toSet
      val requiredFields = requiredSchema.fields.filterNot { f =>
        partitionColumnNames.contains(f.name)
      }
      println(reader.getSchema)
        println(requestedColIds)
        println(requiredFields)
        println(partitionSchema)
        println(file.partitionValues)

      batchReader.initialize(fileSplit, taskAttemptContext)
      batchReader.initBatch(
        reader.getSchema,
        requestedColIds,
        requiredFields,
        partitionSchema,
        file.partitionValues)
      OrcColumnarBatchDataReader(iter)
    }
  }
}

case class OrcUnsafeRowReaderFactory(
    file: PartitionedFile,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    broadcastedConf: Broadcast[SerializableConfiguration])
  extends DataReaderFactory[UnsafeRow] {
  private def conf = broadcastedConf.value.value
  private def filePath = new Path(new URI(file.filePath))
  private def fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
  private def attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
  private def fs = filePath.getFileSystem(conf)
  private def readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
  private def reader = OrcFile.createReader(filePath, readerOptions)
  private def isCaseSensitive = true
  private def requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
    isCaseSensitive, dataSchema, requiredSchema, reader, conf)
  private def requestedColIds = requestedColIdsOrEmptyFile.get
  assert(requestedColIds.length == requiredSchema.length,
    "[BUG] requested column IDs do not match required schema")
  private def taskConf = new Configuration(conf)
  taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
    requestedColIds.filter(_ != -1).sorted.mkString(","))
  private def taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)
  private def orcRecordReader = new OrcInputFormat[OrcStruct]
    .createRecordReader(fileSplit, taskAttemptContext)
  private def iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
  private def fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes

  private def deserializer = new OrcDeserializer(dataSchema, requiredSchema, requestedColIds)

  /**
   * Returns a data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createDataReader(): DataReader[UnsafeRow] =
    OrcUnsafeRowDataReader(iter, fullSchema, deserializer)
}

object OrcDataSource {
  val PATH = "path"
}
