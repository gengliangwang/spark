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

import java.util.{Optional, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, OutputWriterFactory, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.WriteJobDescription
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

trait FileWriteSupport extends WriteSupport {
  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {
    assert(options.paths().length == 1)
    val pathName = options.paths().head
    val path = new Path(pathName)

    val committer = new HadoopMapReduceCommitProtocol(jobId, pathName)

    val sparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val optionsAsScala = options.asMap().asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(optionsAsScala)

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(pathName))

    val caseInsensitiveOptions = CaseInsensitiveMap(optionsAsScala)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, schema)
    val allColumns = schema.toAttributes
    lazy val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    val sqlConf = sparkSession.sessionState.conf
    val partitionColumns = PartitioningUtils.partitionColumnsSchema(schema,
      options.partitionColumns(), sqlConf.caseSensitiveAnalysis).toAttributes
    val description = new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns,
      partitionColumns = partitionColumns,
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = Map.empty,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sqlConf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sqlConf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )

    val fs = path.getFileSystem(hadoopConf)
    mode match {
      case SaveMode.ErrorIfExists if (fs.exists(path)) =>
        throw new RuntimeException("data already exists.")

      case SaveMode.Ignore if (fs.exists(path)) =>
        Optional.empty()

      case SaveMode.Overwrite =>
        fs.delete(path, true)
        committer.setupJob(job)
        Optional.of(new FileSourceWriter(job, description, committer))

      case _ =>
        committer.setupJob(job)
        Optional.of(new FileSourceWriter(job, description, committer))

    }
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory
}
