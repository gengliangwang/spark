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
package org.apache.spark.status

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.EmptyScalaKryoInstantiator

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, TaskResourceRequest}
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}
import org.apache.spark.util.kvstore.KVStoreSerializer


class KVKryoSerializer() extends KVStoreSerializer {
  private val kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val _kryo = instantiator.newKryo()
    _kryo.setRegistrationRequired(false)
    KVKryoSerializer.uiClasses.foreach(_kryo.register)
    _kryo.register(classOf[UUID], new KryoJavaSerializer())
    _kryo
  }

  override def serialize(o: Any): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    kryo.writeObject(output, o)
    output.close()
    outputStream.toByteArray
  }

  override def deserialize[T](data: Array[Byte], clazz: Class[T]): T = {
    val inputStream = new ByteArrayInputStream(data)
    val input = new Input(inputStream)
    val obj = kryo.readObject(input, clazz)
    input.close()
    obj
  }
}

object KVKryoSerializer {
  private val uiClasses = Seq(
    classOf[JobData],
    classOf[JobDataWrapper],
    classOf[AccumulableInfo],
    classOf[TaskDataWrapper],
    classOf[ExecutorMetrics],
    classOf[ExecutorStageSummary],
    classOf[ExecutorStageSummaryWrapper],
    classOf[ExecutorResourceRequest],
    classOf[TaskResourceRequest],
    classOf[ResourceProfileInfo],
    classOf[RuntimeInfo],
    classOf[ApplicationEnvironmentInfo],
    classOf[ApplicationEnvironmentInfoWrapper],
    classOf[ApplicationAttemptInfo],
    classOf[ApplicationInfo],
    classOf[ApplicationInfoWrapper],
    classOf[StreamBlockData],
    classOf[RDDDataDistribution],
    classOf[RDDPartitionInfo],
    classOf[RDDStorageInfo],
    classOf[RDDStorageInfoWrapper],
    classOf[ResourceProfileWrapper],
    classOf[CachedQuantile],
    classOf[SpeculationStageSummary],
    classOf[SpeculationStageSummaryWrapper],
    classOf[ProcessSummary],
    classOf[ProcessSummaryWrapper],
    classOf[MemoryMetrics],
    classOf[ResourceInformation],
    classOf[ExecutorSummary],
    classOf[ExecutorSummaryWrapper],
    classOf[RDDOperationEdge],
    classOf[RDDOperationNode],
    classOf[RDDOperationClusterWrapper],
    classOf[RDDOperationGraphWrapper],
    classOf[StageDataWrapper],
    classOf[TaskData],
    classOf[StageData],
    classOf[TaskMetrics],
    classOf[InputMetrics],
    classOf[OutputMetrics],
    classOf[ShuffleReadMetrics],
    classOf[ShufflePushReadMetrics],
    classOf[ShuffleWriteMetrics],
    classOf[TaskMetricDistributions],
    classOf[InputMetricDistributions],
    classOf[OutputMetricDistributions],
    classOf[ShuffleReadMetricDistributions],
    classOf[ShufflePushReadMetricDistributions],
    classOf[ShuffleWriteMetricDistributions],
    classOf[ExecutorMetricsDistributions],
    classOf[ExecutorPeakMetricsDistributions],
    classOf[AppSummary],
    classOf[PoolData]
  )
}

