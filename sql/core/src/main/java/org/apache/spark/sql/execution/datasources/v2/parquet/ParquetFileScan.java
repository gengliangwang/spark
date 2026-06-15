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

import java.util.Map;
import java.util.OptionalLong;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.FileBatch;
import org.apache.spark.sql.connector.read.FileScan;
import org.apache.spark.sql.connector.read.FileSet;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A {@link FileScan} over a Parquet table: a single {@link FileSet} pairing the table's
 * {@code FileIndex} with the Parquet {@code FileFormat}. Predicate pushdown is captured via
 * {@link ParquetFileScanBuilder} (via SupportsPushDownCatalystFilters) and surfaced through
 * {@link #partitionFilters()} / {@link #dataFilters()}. The planner lowers this scan to a V1
 * {@code FileSourceScanExec}, re-applying the pushed partition filters and the post-scan data
 * filters and column pruning, so execution (vectorized reader, filter pushdown, partition
 * pruning) is identical to the V1 read path.
 */
public class ParquetFileScan implements FileScan, SupportsReportStatistics {

  private final SparkSession session;
  private final PartitioningAwareFileIndex fileIndex;
  private final StructType dataSchema;
  private final StructType readSchema;
  private final CaseInsensitiveStringMap options;
  private final Expression[] partitionFilters;
  private final Expression[] dataFilters;

  public ParquetFileScan(
      SparkSession session,
      PartitioningAwareFileIndex fileIndex,
      StructType dataSchema,
      StructType readSchema,
      CaseInsensitiveStringMap options,
      Expression[] partitionFilters,
      Expression[] dataFilters) {
    this.session = session;
    this.fileIndex = fileIndex;
    this.dataSchema = dataSchema;
    this.readSchema = readSchema;
    this.options = options;
    this.partitionFilters = partitionFilters;
    this.dataFilters = dataFilters;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  // Prunable partition predicates pushed via SupportsPushDownCatalystFilters. The planner
  // re-applies these when lowering to FileSourceScanExec so partition pruning matches the V1 read
  // path; they are not in the post-scan predicates because the DSv2 push consumed them.
  @Override
  public Expression[] partitionFilters() {
    return partitionFilters;
  }

  // Data predicates pushed via SupportsPushDownCatalystFilters, reported for display. They are
  // also kept as post-scan filters, so the planner re-evaluates them above the lowered scan and
  // FileSourceStrategy pushes them into the parquet reader.
  @Override
  public Expression[] dataFilters() {
    return dataFilters;
  }

  @Override
  public FileBatch planFileBatch() {
    final FileSet fileSet = new FileSet() {
      @Override
      public FileIndex index() {
        return fileIndex;
      }

      @Override
      public FileFormat format() {
        return new ParquetFileFormat();
      }

      @Override
      public Map<String, String> options() {
        return options.asCaseSensitiveMap();
      }

      @Override
      public NamedReference[] partitionColumns() {
        return toReferences(fileIndex.partitionSchema().names());
      }

      @Override
      public NamedReference[] dataColumns() {
        return toReferences(dataSchema.names());
      }
    };
    return () -> new FileSet[] { fileSet };
  }

  // Mirror HadoopFsRelation.sizeInBytes so the optimizer (e.g. broadcast-join selection) sees
  // the same size estimate as on the V1 read path.
  @Override
  public Statistics estimateStatistics() {
    double compressionFactor = session.sessionState().conf().fileCompressionFactor();
    long sizeInBytes = (long) (fileIndex.sizeInBytes() * compressionFactor);
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.of(sizeInBytes);
      }

      @Override
      public OptionalLong numRows() {
        return OptionalLong.empty();
      }
    };
  }

  @Override
  public String description() {
    return "ParquetFileScan " + fileIndex.rootPaths().mkString(", ");
  }

  private static NamedReference[] toReferences(String[] names) {
    NamedReference[] refs = new NamedReference[names.length];
    for (int i = 0; i < names.length; i++) {
      refs[i] = FieldReference.column(names[i]);
    }
    return refs;
  }
}
