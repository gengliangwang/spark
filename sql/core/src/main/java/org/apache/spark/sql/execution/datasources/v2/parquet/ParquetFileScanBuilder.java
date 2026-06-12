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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Scan builder for {@link ParquetFileScanTable}. Filters are intentionally NOT pushed at the
 * DSv2 level: the planner re-derives partition and data filters when it lowers the
 * {@link ParquetFileScan} to the V1 file-source path, so partition pruning and parquet filter
 * pushdown happen there, exactly as on the V1 read path. Column pruning (top-level, nested
 * fields, and `_metadata` struct subfields) IS accepted, so the optimizer's pruned schema
 * reaches the lowered scan; the planner re-synthesizes the full-schema V1 relation underneath
 * and restricts the branch to the pruned output.
 */
public class ParquetFileScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

  private final SparkSession session;
  private final PartitioningAwareFileIndex fileIndex;
  private final StructType dataSchema;
  private final CaseInsensitiveStringMap options;

  private StructType requiredSchema;

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
  public Scan build() {
    return new ParquetFileScan(session, fileIndex, dataSchema, requiredSchema, options);
  }
}
