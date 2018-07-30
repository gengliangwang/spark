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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.v2.{EmptyInputPartitionReader, _}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class OrcDataSourceV2 extends FileDataSourceV2 with ReadSupport with ReadSupportWithSchema {
  override def createReader(options: DataSourceOptions): FileSourceReader = {
    new OrcDataSourceReader(options, None)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): FileSourceReader = {
    new OrcDataSourceReader(options, Some(schema))
  }

  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[OrcFileFormat]

  override def shortName(): String = "orc"
}

class OrcMetaData(
    options: DataSourceOptions,
    userSpecifiedSchema: Option[StructType])
  extends FileSourceMetaData(options, userSpecifiedSchema)
  with SupportsPushDownCatalystFilters {
  // Should we make partition filter public?
  private var partitionFilters: Array[Expression] = Array.empty
  private var pushedFiltersArray: Array[Expression] = Array.empty

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    OrcUtils.readSchema(sparkSession, files)

  override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = {
    val partitionColumnNames = partitionSchema.toAttributes.map(_.name).toSet
    val (partitionKeyFilters, otherFilters) = filters.partition {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    this.partitionFilters = partitionKeyFilters
    pushedFiltersArray = partitionKeyFilters
    if (sqlConf.orcFilterPushDown) {
      val dataFilters = otherFilters.map { f =>
        (DataSourceStrategy.translateFilter(f), f)
      }.collect { case (optionalFilter, catalystFilter) if optionalFilter.isDefined =>
        (optionalFilter.get, catalystFilter)
      }.toMap
      val pushedDataFilters =
        OrcFilters.convertibleFilters(fullSchema, dataFilters.keys.toSeq).map(dataFilters).toArray
      pushedFiltersArray ++= pushedDataFilters
      OrcFilters.createFilter(fullSchema, dataFilters.keys.toSeq).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, fullSchema.fieldNames)
      }
    }
    otherFilters
  }

  override def pushedCatalystFilters(): Array[Expression] = pushedFiltersArray
}

class OrcSplitReaderProvider(
    @transient val meta: FileSourceMetaData) extends PartitionReaderProvider {
  @transient private val sparkSession =
    SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
  @transient private val sqlConf = sparkSession.sessionState.conf
  val capacity = sqlConf.orcVectorizedReaderBatchSize
  val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  val copyToSpark = sqlConf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK)
  val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  val dataSchema = meta.fullSchema
  val readSchema = meta.getSchema
  val partitionSchema = meta.partitionSchema
  val orcVectorizedReaderEnabled = sqlConf.orcVectorizedReaderEnabled
  val wholeStageEnabled = sqlConf.wholeStageEnabled
  val wholeStageMaxNumFields = sqlConf.wholeStageMaxNumFields
  val serializedConf = new SerializableConfiguration(meta.hadoopConf)

  override def supportColumnarReader(): Boolean = {
    orcVectorizedReaderEnabled && wholeStageEnabled &&
      readSchema.length <= wholeStageMaxNumFields &&
      readSchema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def createColumnarReader(file: PartitionedFile): InputPartitionReader[ColumnarBatch] = {
    val conf = serializedConf.value
    val filePath = new Path(new URI(file.filePath))

    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)

    val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
      isCaseSensitive, dataSchema, readSchema, reader, conf)

    if (requestedColIdsOrEmptyFile.isEmpty) {
      new EmptyInputPartitionReader
    } else {
      val requestedColIds = requestedColIdsOrEmptyFile.get
      assert(requestedColIds.length == readSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskConf = new Configuration(conf)
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
        requestedColIds.filter(_ != -1).sorted.mkString(","))

      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      val taskContext = Option(TaskContext.get())
      val batchReader = new OrcColumnarBatchReader(
        enableOffHeapColumnVector && taskContext.isDefined, copyToSpark, capacity)
      batchReader.initialize(fileSplit, taskAttemptContext)
      val partitionColIds = PartitioningUtils.requestedPartitionColumnIds(
        partitionSchema, readSchema, isCaseSensitive)

      batchReader.initBatch(
        reader.getSchema,
        readSchema.fields,
        requestedColIds,
        partitionColIds,
        file.partitionValues)
      new PartitionRecordReader(batchReader)
    }
  }

  override def createRowReader(file: PartitionedFile): InputPartitionReader[InternalRow] = {
    val conf = serializedConf.value
    val filePath = new Path(new URI(file.filePath))

    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)

    val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
      isCaseSensitive, dataSchema, readSchema, reader, conf)

    if (requestedColIdsOrEmptyFile.isEmpty) {
      new EmptyInputPartitionReader
    } else {
      val requestedColIds = requestedColIdsOrEmptyFile.get
      assert(requestedColIds.length == readSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskConf = new Configuration(conf)
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
        requestedColIds.filter(_ != -1).sorted.mkString(","))

      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      val requiredDataSchema =
        PartitioningUtils.subtractSchema(readSchema, partitionSchema, isCaseSensitive)
      val orcRecordReader = new OrcInputFormat[OrcStruct]
        .createRecordReader(fileSplit, taskAttemptContext)

      val fullSchema = requiredDataSchema.toAttributes ++ partitionSchema.toAttributes
      val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
      val deserializer = new OrcDeserializer(dataSchema, requiredDataSchema, requestedColIds)

      val projection = if (partitionSchema.length == 0) {
        (value: OrcStruct) => unsafeProjection(deserializer.deserialize(value))
      } else {
        val joinedRow = new JoinedRow()
        (value: OrcStruct) =>
          unsafeProjection(joinedRow(deserializer.deserialize(value), file.partitionValues))
      }
      new PartitionRecordDReaderWithProject(orcRecordReader, projection)
    }
  }
}

case class OrcDataSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends FileSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType]) {

  override def getMetadata: FileSourceMetaData = new OrcMetaData(options, userSpecifiedSchema)

  override def getSplitManager(meta: Metadata): FileSourceSplitManager =
    new SplitableFileSourceSplitManager(meta.asInstanceOf[FileSourceMetaData])

  override def getPartitionReaderProvider(meta: Metadata): PartitionReaderProvider = {
    new OrcSplitReaderProvider(meta.asInstanceOf[FileSourceMetaData])
  }
}


