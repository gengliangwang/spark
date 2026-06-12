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
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Scan builder for {@link ParquetFileScanTable}. Neither filters nor columns are pushed at the
 * DSv2 level: the planner re-derives partition/data filters and prunes columns when it lowers
 * the {@link ParquetFileScan} to the V1 file-source path, so partition pruning, parquet filter
 * pushdown and column pruning all happen there, exactly as on the V1 read path. The V1 lowering
 * requires the scan to keep the relation's full output (a {@code LogicalRelation} always
 * carries the full table schema; pruning lives in the Project re-planned above it), so this
 * builder intentionally does not implement {@code SupportsPushDownRequiredColumns}.
 */
public class ParquetFileScanBuilder implements ScanBuilder {

  private final SparkSession session;
  private final PartitioningAwareFileIndex fileIndex;
  private final StructType tableSchema;
  private final StructType dataSchema;
  private final CaseInsensitiveStringMap options;

  public ParquetFileScanBuilder(
      SparkSession session,
      PartitioningAwareFileIndex fileIndex,
      StructType tableSchema,
      StructType dataSchema,
      CaseInsensitiveStringMap options) {
    this.session = session;
    this.fileIndex = fileIndex;
    this.tableSchema = tableSchema;
    this.dataSchema = dataSchema;
    this.options = options;
  }

  @Override
  public Scan build() {
    return new ParquetFileScan(session, fileIndex, dataSchema, tableSchema, options);
  }
}
