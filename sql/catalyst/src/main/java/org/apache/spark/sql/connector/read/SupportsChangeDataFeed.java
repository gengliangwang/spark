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

package org.apache.spark.sql.connector.read;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * A mix-in interface for {@link Scan}. Data sources can implement this interface to
 * support Change Data Feed (CDF) reads, which captures row-level changes between
 * versions or timestamps.
 * <p>
 * CDF reads work by scanning two separate batches:
 * <ul>
 *   <li>Added records batch: Contains rows that were added (including updates as new values)</li>
 *   <li>Removed records batch: Contains rows that were deleted (including updates as old values)</li>
 * </ul>
 * <p>
 * Spark will perform a full outer join on these two batches using the row ID columns
 * to produce a unified change feed with change types (insert, update, delete).
 * <p>
 * The output schema for each batch includes:
 * <ul>
 *   <li>All data columns from the table schema</li>
 *   <li>Partition columns (if any)</li>
 *   <li>_commit_version: The commit version when the change occurred</li>
 *   <li>_commit_timestamp: The timestamp when the change occurred</li>
 * </ul>
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsChangeDataFeed extends Scan {

  /**
   * Returns the physical representation for scanning addition changes.
   * <p>
   * This batch scans files that contain rows being added to the table state, including:
   * <ul>
   *   <li>Inserted rows</li>
   *   <li>Updated rows (the new version after update)</li>
   * </ul>
   * <p>
   * The output should include all data columns, partition columns, and CDF metadata
   * columns (_commit_version, _commit_timestamp).
   *
   * @return a {@link Batch} for scanning addition changes
   */
  Batch additionChangesBatch();

  /**
   * Returns the physical representation for scanning deletion changes.
   * <p>
   * This batch scans files that contain rows being removed from the table state, including:
   * <ul>
   *   <li>Deleted rows</li>
   *   <li>Updated rows (the old version before update)</li>
   * </ul>
   * <p>
   * The output should include all data columns, partition columns, and CDF metadata
   * columns (_commit_version, _commit_timestamp).
   *
   * @return a {@link Batch} for scanning deletion changes
   */
  Batch deletionChangesBatch();

  /**
   * Returns the row identifier columns used to join added and removed records.
   * <p>
   * These columns uniquely identify a row across versions and are used by Spark
   * to perform a full outer join between the added and removed batches to determine:
   * <ul>
   *   <li>INSERT: Row exists only in added batch</li>
   *   <li>DELETE: Row exists only in removed batch</li>
   *   <li>UPDATE: Row exists in both batches (matched by row ID)</li>
   * </ul>
   *
   * @return an array of {@link NamedReference} representing the row ID columns
   */
  NamedReference[] rowIdColumns();

  /**
   * Returns the schema for CDF output, which includes the base read schema
   * plus CDF metadata columns.
   * <p>
   * By default, this returns {@link #readSchema()} with additional metadata fields.
   * Data sources may override this to customize the CDF output schema.
   *
   * @return the schema for CDF reads including metadata columns
   */
  default StructType cdfReadSchema() {
    return readSchema();
  }

  // ==================== Streaming CDF Methods ====================

  /**
   * Returns a {@link MicroBatchStream} for streaming addition changes in micro-batch mode.
   * <p>
   * This stream provides rows being added to the table state, including:
   * <ul>
   *   <li>Inserted rows</li>
   *   <li>Updated rows (the new version after update)</li>
   * </ul>
   * <p>
   * The output should include all data columns, partition columns, and CDF metadata
   * columns (_commit_version, _commit_timestamp).
   * <p>
   * By default, this method throws an exception. Data sources must override this method
   * to provide streaming CDF support.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space for failure recovery
   * @return a {@link MicroBatchStream} for streaming addition changes
   * @throws UnsupportedOperationException if streaming CDF is not supported
   */
  default MicroBatchStream additionChangesMicroBatchStream(String checkpointLocation) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3148", Map.of("description", description()));
  }

  /**
   * Returns a {@link MicroBatchStream} for streaming deletion changes in micro-batch mode.
   * <p>
   * This stream provides rows being removed from the table state, including:
   * <ul>
   *   <li>Deleted rows</li>
   *   <li>Updated rows (the old version before update)</li>
   * </ul>
   * <p>
   * The output should include all data columns, partition columns, and CDF metadata
   * columns (_commit_version, _commit_timestamp).
   * <p>
   * By default, this method throws an exception. Data sources must override this method
   * to provide streaming CDF support.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space for failure recovery
   * @return a {@link MicroBatchStream} for streaming deletion changes
   * @throws UnsupportedOperationException if streaming CDF is not supported
   */
  default MicroBatchStream deletionChangesMicroBatchStream(String checkpointLocation) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3148", Map.of("description", description()));
  }
}

