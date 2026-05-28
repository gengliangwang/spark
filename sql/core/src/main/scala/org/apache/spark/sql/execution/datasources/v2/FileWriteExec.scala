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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.connector.files.FileWrite
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.FileFormatWriter

/**
 * Common implementation for the new batch write exec nodes that route DSv2 [[FileWrite]] writes
 * through Spark's V1 file write path (`FileFormatWriter.write`), instead of the standard
 * `BatchWrite` factory path.
 */
trait FileWriteExec extends V2CommandExec with UnaryExecNode {

  def query: SparkPlan
  def refreshCache: () => Unit
  def write: FileWrite

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    prepareWrite()
    val sparkSession = session
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(write.options)
    FileFormatWriter.write(
      sparkSession = sparkSession,
      plan = query,
      fileFormat = write.fileFormat,
      committer = write.commitProtocol,
      outputSpec = FileFormatWriter.OutputSpec(
        write.outputPath, write.customPartitionLocations, query.output),
      hadoopConf = hadoopConf,
      partitionColumns = write.partitionColumns,
      bucketSpec = write.bucketSpec,
      statsTrackers = write.statsTrackers,
      options = write.options,
      numStaticPartitionCols = write.numStaticPartitionCols)
    refreshCache()
    Nil
  }

  /** Hook for subclasses to run any pre-write cleanup (e.g. truncate the output dir). */
  protected def prepareWrite(): Unit
}

/**
 * Physical plan node for an append into a [[FileWrite]]-backed connector.
 */
case class AppendFilesExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: FileWrite,
    tableName: String) extends FileWriteExec {

  override def nodeName: String = s"AppendFiles $tableName"

  override protected def prepareWrite(): Unit = ()

  override protected def withNewChildInternal(newChild: SparkPlan): AppendFilesExec =
    copy(query = newChild)
}

/**
 * Physical plan node for an overwrite-by-expression into a [[FileWrite]]-backed connector.
 *
 * Hackathon prototype: only `Literal.TrueLiteral` (i.e. truncate-and-replace) is supported.
 * Predicate-driven overwrite is left as a follow-up because it has no clean mapping to the V1
 * `FileFormatWriter` codepath.
 */
case class OverwriteFilesByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: FileWrite,
    deleteWhere: Expression,
    tableName: String) extends FileWriteExec {

  override def nodeName: String = s"OverwriteFilesByExpression $tableName"

  override protected def prepareWrite(): Unit = {
    if (!isTruncate) {
      throw new UnsupportedOperationException(
        s"OverwriteFilesByExpressionExec only supports overwrite-all (truncate); " +
        s"got delete predicate: $deleteWhere")
    }
    val out = new Path(write.outputPath)
    val fs = out.getFileSystem(session.sessionState.newHadoopConf())
    if (fs.exists(out)) {
      fs.delete(out, true)
    }
  }

  private def isTruncate: Boolean = deleteWhere match {
    case Literal.TrueLiteral => true
    case Literal(true, _) => true
    case _ => false
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): OverwriteFilesByExpressionExec =
    copy(query = newChild)
}

/**
 * Physical plan node for dynamic partition overwrite into a [[FileWrite]]-backed connector.
 *
 * Relies on the connector having instantiated its `FileCommitProtocol` with
 * `dynamicPartitionOverwrite = true` so the commit protocol replaces only the partitions that
 * the job writes to. No pre-write cleanup is required here.
 */
case class OverwritePartitionFilesDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: FileWrite,
    tableName: String) extends FileWriteExec {

  override def nodeName: String = s"OverwritePartitionFilesDynamic $tableName"

  override protected def prepareWrite(): Unit = ()

  override protected def withNewChildInternal(
      newChild: SparkPlan): OverwritePartitionFilesDynamicExec =
    copy(query = newChild)
}
