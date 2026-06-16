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

package org.apache.spark.sql.execution.datasources.v2.parquet;

import scala.collection.immutable.Seq;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownVariantExtractions;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Scan builder for {@link ParquetFileScanTable}. Predicate pushdown uses
 * {@link SupportsPushDownCatalystFilters}, mirroring the Scala {@code FileScanBuilder}: prunable
 * partition predicates are consumed (reported via {@link ParquetFileScan#partitionFilters()} and
 * re-applied for partition pruning when the planner lowers the scan to the V1 file-source path),
 * while data predicates are reported via {@link #pushedFilters()} but kept as post-scan filters
 * so they are re-evaluated above the lowered {@code FileSourceScanExec} and pushed into the
 * parquet reader by {@code FileSourceStrategy} -- exactly as on the V1 read path. Column pruning
 * (top-level, nested fields, and {@code _metadata} struct subfields) IS accepted, so the
 * optimizer's pruned schema reaches the lowered scan.
 */
public class ParquetFileScanBuilder
    implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownCatalystFilters,
        SupportsPushDownVariantExtractions {

  private final SparkSession session;
  private final PartitioningAwareFileIndex fileIndex;
  private final StructType dataSchema;
  private final CaseInsensitiveStringMap options;

  private StructType requiredSchema;
  private Expression[] partitionFilters = new Expression[0];
  private Expression[] dataFilters = new Expression[0];
  private Predicate[] pushedFilters = new Predicate[0];
  private VariantExtraction[] variantExtractions = new VariantExtraction[0];

  public ParquetFileScanBuilder(
      SparkSession session,
      PartitioningAwareFileIndex fileIndex,
      StructType tableSchema,
      StructType dataSchema,
      CaseInsensitiveStringMap options) {
    this.session = session;
    this.fileIndex = fileIndex;
    this.dataSchema = dataSchema;
    this.options = options;
    this.requiredSchema = tableSchema;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Seq<Expression> pushFilters(Seq<Expression> filters) {
    ParquetFileScanFilterPushdown.Result result =
        ParquetFileScanFilterPushdown.pushFilters(fileIndex.partitionSchema(), filters);
    this.partitionFilters = result.partitionFilters();
    this.dataFilters = result.dataFilters();
    this.pushedFilters = result.pushedFilters();
    return result.postScanFilters();
  }

  @Override
  public Predicate[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean[] pushVariantExtractions(VariantExtraction[] extractions) {
    // Parquet supports all variant extractions: the request is encoded as VariantMetadata on the
    // read schema and lowered to the V1 parquet reader, which shreds the variant.
    this.variantExtractions = extractions;
    boolean[] accepted = new boolean[extractions.length];
    java.util.Arrays.fill(accepted, true);
    return accepted;
  }

  @Override
  public Scan build() {
    return new ParquetFileScan(session, fileIndex, dataSchema, requiredSchema, options,
      partitionFilters, dataFilters, variantExtractions);
  }
}
