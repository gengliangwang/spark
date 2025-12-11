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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, SparkDataStream}
import org.apache.spark.sql.execution.StreamSourceAwareSparkPlan
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning a micro-batch of data from a data source.
 *
 * @param output the output attributes of this scan
 * @param scan the DSv2 Scan
 * @param stream the MicroBatchStream for this scan
 * @param start the start offset for this micro-batch
 * @param end the end offset for this micro-batch
 * @param keyGroupedPartitioning partitioning expressions, if any
 * @param ordering ordering provided by the scan, if any
 * @param cdfInfo optional CDF info for Change Data Feed streaming reads
 */
case class MicroBatchScanExec(
    output: Seq[Attribute],
    @transient scan: Scan,
    @transient stream: MicroBatchStream,
    @transient start: Offset,
    @transient end: Offset,
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None,
    cdfInfo: Option[CDFInfo] = None)
  extends DataSourceV2ScanExecBase
  with StreamSourceAwareSparkPlan {

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: MicroBatchScanExec => this.stream == other.stream
    case _ => false
  }

  override def hashCode(): Int = stream.hashCode()

  override lazy val inputPartitions: Seq[InputPartition] =
    stream.planInputPartitions(start, end).toImmutableArraySeq

  override lazy val readerFactory: PartitionReaderFactory = stream.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    val inputRDD = new DataSourceRDD(sparkContext, partitions, readerFactory, supportsColumnar,
      customMetrics)
    postDriverMetrics()
    inputRDD
  }

  override def getStream: Option[SparkDataStream] = Some(stream)

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val cdfString = cdfInfo.map(info => s" $info").getOrElse("")
    s"$nodeName$truncatedOutputString ${scan.description()}$cdfString"
  }

  override def nodeName: String = {
    val cdfSuffix = cdfInfo.flatMap(_.batchType).map {
      case CDFAddedBatch => " [CDF Added]"
      case CDFRemovedBatch => " [CDF Removed]"
    }.getOrElse("")
    s"MicroBatchScan$cdfSuffix"
  }
}
