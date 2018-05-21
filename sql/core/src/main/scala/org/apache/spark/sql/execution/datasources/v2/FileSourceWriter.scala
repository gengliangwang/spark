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

import java.util.Date

import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.FileFormatWriter._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.util.SerializableConfiguration

class FileSourceWriter(
    job: Job,
    description: WriteJobDescription,
    committer: FileCommitProtocol)
  extends DataSourceWriter
  with SupportsWriteInternalRow {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    committer.commitJob(job, messages.map(_.asInstanceOf[WriteTaskResult].commitMsg))
  }

  override def useCommitCoordinator(): Boolean = false

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    committer.abortJob(job)
  }

  override def createInternalRowWriterFactory(): DataWriterFactory[InternalRow] = {
    val conf = new SerializableConfiguration(job.getConfiguration)
    FileDataWriterFactory(description, committer, conf)
  }
}

case class FileDataWriterFactory (
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    conf: SerializableConfiguration) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
      partitionId: Int,
      attemptNumber: Int,
      epochId: Long): DataWriter[InternalRow] = {
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, partitionId)
    val taskAttemptId = new TaskAttemptID(taskId, attemptNumber)
    val taskAttemptContext: TaskAttemptContextImpl = {
      // Set up the configuration object
      val hadoopConf = conf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }
    committer.setupTask(taskAttemptContext)
    if (description.partitionColumns.isEmpty) {
      new SingleDirectoryWriteTask(description, taskAttemptContext, committer)
    } else {
      new DynamicPartitionWriteTask(description, taskAttemptContext, committer)
    }
  }
}

case class FileDataWriterCommitMessage(msg: TaskCommitMessage) extends WriterCommitMessage

case class FileDataWriter(
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    taskAttemptContext: TaskAttemptContextImpl) extends DataWriter[InternalRow] {
  private var fileCounter = 0
  private val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
  private val currentPath = committer.newTaskTempFile(
    taskAttemptContext,
    None,
    f"-c$fileCounter%03d" + ext)

  private val writer = description.outputWriterFactory.newInstance(
    path = currentPath,
    dataSchema = description.dataColumns.toStructType,
    context = taskAttemptContext)
  override def write(record: InternalRow): Unit = {
    writer.write(record)
  }

  override def commit(): WriterCommitMessage = {
    writer.close()
    FileDataWriterCommitMessage(committer.commitTask(taskAttemptContext))
  }

  override def abort(): Unit = {
    writer.close()
    committer.abortTask(taskAttemptContext)
  }
}
