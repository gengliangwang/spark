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

package org.apache.spark.sql.connector.catalog;

import java.util.Optional;

import org.apache.spark.annotation.Evolving;

/**
 * Represents the range of changes requested in a CDC (Change Data Capture) query.
 * <p>
 * A {@code ChangelogRange} defines the boundaries for reading row-level changes from a table.
 * Three types of ranges are supported:
 * <ul>
 *   <li>{@link VersionRange} — bounded by version identifiers (e.g., Delta commit numbers,
 *       Iceberg snapshot IDs)</li>
 *   <li>{@link TimestampRange} — bounded by timestamps</li>
 *   <li>{@link Unbounded} — no explicit boundaries; used by streaming queries where the
 *       connector determines the starting point</li>
 * </ul>
 *
 * @since 4.2.0
 */
@Evolving
public sealed interface ChangelogRange
    permits ChangelogRange.VersionRange, ChangelogRange.TimestampRange, ChangelogRange.Unbounded {

  /**
   * Returns whether the starting bound is inclusive.
   */
  boolean startingBoundInclusive();

  /**
   * Returns whether the ending bound is inclusive.
   */
  boolean endingBoundInclusive();

  /**
   * A version-based range for CDC queries.
   * <p>
   * Version identifiers are strings whose interpretation is connector-specific. For example,
   * Delta Lake uses numeric commit versions, while Iceberg uses snapshot IDs.
   *
   * @param startingVersion the starting version (required)
   * @param endingVersion the ending version (optional; absent means "latest")
   * @param startingBoundInclusive whether the starting bound is inclusive (default: true)
   * @param endingBoundInclusive whether the ending bound is inclusive (default: true)
   */
  record VersionRange(
      String startingVersion,
      Optional<String> endingVersion,
      boolean startingBoundInclusive,
      boolean endingBoundInclusive) implements ChangelogRange {
  }

  /**
   * A timestamp-based range for CDC queries.
   *
   * @param startingTimestamp the starting timestamp in microseconds since epoch (required)
   * @param endingTimestamp the ending timestamp in microseconds since epoch
   *                        (optional; absent means "latest")
   * @param startingBoundInclusive whether the starting bound is inclusive (default: true)
   * @param endingBoundInclusive whether the ending bound is inclusive (default: true)
   */
  record TimestampRange(
      long startingTimestamp,
      Optional<Long> endingTimestamp,
      boolean startingBoundInclusive,
      boolean endingBoundInclusive) implements ChangelogRange {
  }

  /**
   * An unbounded range for streaming CDC queries.
   * <p>
   * When no starting or ending boundaries are specified, the connector determines the
   * starting point (e.g., emit the initial snapshot as INSERTs, then stream future changes).
   */
  record Unbounded() implements ChangelogRange {
    @Override
    public boolean startingBoundInclusive() { return true; }

    @Override
    public boolean endingBoundInclusive() { return true; }
  }
}
