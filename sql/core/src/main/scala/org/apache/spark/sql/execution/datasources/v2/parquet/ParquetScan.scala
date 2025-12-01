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
package org.apache.spark.sql.execution.datasources.v2.parquet

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, VariantProjection}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class ParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    pushedAggregate: Option[Aggregation] = None,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    pushedVariantProjections: Array[VariantProjection] = Array.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  // Build transformed schema if variant pushdown is active
  private def effectiveReadDataSchema: StructType = {
    if (pushedVariantProjections.isEmpty) {
      readDataSchema
    } else {
      // Build a mapping from column name (first element) to expected data type
      val variantSchemaMap = pushedVariantProjections.groupBy(_.columnName()(0)).map {
        case (colName, projections) =>
          // Create a struct with all projections for this column
          val fields = projections.zipWithIndex.map { case (proj, idx) =>
            StructField(idx.toString, proj.expectedDataType())
          }
          colName -> StructType(fields)
      }

      // Transform the read data schema by replacing variant columns with their extracted schemas
      StructType(readDataSchema.map { field =>
        variantSchemaMap.get(field.name) match {
          case Some(extractedSchema) => field.copy(dataType = extractedSchema)
          case None => field
        }
      })
    }
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) {
      effectiveReadDataSchema
    } else {
      // super.readSchema() combines readDataSchema + readPartitionSchema
      // Apply variant transformation if variant pushdown is active
      val baseSchema = super.readSchema()
      if (pushedVariantProjections.isEmpty) {
        baseSchema
      } else {
        val variantSchemaMap = pushedVariantProjections.groupBy(_.columnName()(0)).map {
          case (colName, projections) =>
            val fields = projections.zipWithIndex.map { case (proj, idx) =>
              StructField(idx.toString, proj.expectedDataType())
            }
            colName -> StructType(fields)
        }
        StructType(baseSchema.map { field =>
          variantSchemaMap.get(field.name) match {
            case Some(extractedSchema) => field.copy(dataType = extractedSchema)
            case None => field
          }
        })
      }
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val effectiveSchema = effectiveReadDataSchema
    val readDataSchemaAsJson = effectiveSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      conf.legacyParquetNanosAsLong)

    val broadcastedConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    ParquetPartitionReaderFactory(
      conf,
      broadcastedConf,
      dataSchema,
      effectiveSchema,
      readPartitionSchema,
      pushedFilters,
      pushedAggregate,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, conf))
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: ParquetScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      val pushedVariantEqual =
        java.util.Arrays.equals(pushedVariantProjections.asInstanceOf[Array[Object]],
          p.pushedVariantProjections.asInstanceOf[Array[Object]])
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual &&
        pushedVariantEqual
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions.toImmutableArraySeq),
      seqToString(pushedAggregate.get.groupByExpressions.toImmutableArraySeq))
  } else {
    ("[]", "[]")
  }

  override def getMetaData(): Map[String, String] = {
    val variantProjectionsStr = if (pushedVariantProjections.nonEmpty) {
      pushedVariantProjections.map(proj =>
        s"${java.util.Arrays.toString(proj.columnName())}->${proj.expectedDataType()}"
      ).mkString("[", ", ", "]")
    } else {
      "[]"
    }
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr) ++
      Map("PushedVariantProjections" -> variantProjectionsStr)
  }
}
