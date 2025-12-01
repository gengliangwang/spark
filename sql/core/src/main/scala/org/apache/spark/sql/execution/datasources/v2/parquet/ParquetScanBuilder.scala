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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{SupportsPushDownAggregates, SupportsPushDownVariantProjections, VariantProjection}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

case class ParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates
    with SupportsPushDownVariantProjections {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  private var pushedVariantProjections: Array[VariantProjection] = Array.empty

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    val sqlConf = sparkSession.sessionState.conf
    if (sqlConf.parquetFilterPushDown) {
      val pushDownDate = sqlConf.parquetFilterPushDownDate
      val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
      val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
      val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
      val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
      val isCaseSensitive = sqlConf.caseSensitiveAnalysis
      val parquetSchema =
        new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(readDataSchema())
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringPredicate,
        pushDownInFilterThreshold,
        isCaseSensitive,
        // The rebase mode doesn't matter here because the filters are used to determine
        // whether they is convertible.
        RebaseSpec(LegacyBehaviorPolicy.CORRECTED))
      parquetFilters.convertibleFilters(dataFilters.toImmutableArraySeq).toArray
    } else {
      Array.empty[Filter]
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  override def pushVariantProjections(projections: Array[VariantProjection]): Array[Boolean] = {
    if (projections.isEmpty) {
      return Array.empty
    }

    // Check each projection individually to determine if it can be pushed down.
    // A projection can only be pushed down if the connector can guarantee that
    // all files have the variant field fully shredded.
    val pushdownResults = projections.map(canPushDownVariantProjection)

    // Only store the projections that were successfully pushed down
    pushedVariantProjections = projections.zip(pushdownResults).collect {
      case (proj, true) => proj
    }

    pushdownResults
  }

  /**
   * Check if a variant projection can be pushed down.
   * A projection can only be pushed down if the connector can guarantee that
   * all files it is about to read have the variant field fully shredded.
   */
  private def canPushDownVariantProjection(projection: VariantProjection): Boolean = {
    val sqlConf = sparkSession.sessionState.conf

    // Check if variant pushdown is enabled
    if (!sqlConf.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN)) {
      return false
    }

    // Check if reading shredded variants is allowed
    if (!sqlConf.getConf(SQLConf.VARIANT_ALLOW_READING_SHREDDED)) {
      return false
    }

    val columnPath = projection.columnName()
    if (columnPath.isEmpty) {
      return false
    }

    // Verify the column exists and is a variant type by traversing the path
    val rootColumnName = columnPath(0)
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis

    // Find the column in the data schema
    val columnType = findColumnType(dataSchema, rootColumnName, isCaseSensitive)

    // Only push down if the root column is a variant type
    // Note: At ScanBuilder time, we cannot efficiently check per-file shredding metadata
    // without reading all file footers. The Parquet reader handles both shredded and
    // unshredded variants, falling back to reading from the 'value' field when
    // 'typed_value' is not present. Therefore, we allow pushdown for variant columns
    // and rely on the reader to handle files that may not be fully shredded.
    columnType.exists(_ == VariantType)
  }

  /**
   * Find the data type of a column in the schema by name.
   */
  private def findColumnType(
      schema: StructType,
      columnName: String,
      caseSensitive: Boolean): Option[DataType] = {
    val field = if (caseSensitive) {
      schema.find(_.name == columnName)
    } else {
      schema.find(_.name.equalsIgnoreCase(columnName))
    }
    field.map(_.dataType)
  }

  override def build(): ParquetScan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, finalSchema,
      readPartitionSchema(), pushedDataFilters, options, pushedAggregations,
      partitionFilters, dataFilters, pushedVariantProjections)
  }
}
