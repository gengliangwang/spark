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

import org.apache.spark.annotation.Evolving;

/**
 * Encapsulates the parameters for a CDC (Change Data Capture) query, passed from the
 * SQL parser or DataFrame API to the catalog via
 * {@link TableCatalog#loadChangelog(Identifier, ChangelogInfo)}.
 *
 * @since 4.2.0
 */
@Evolving
public class ChangelogInfo {

  /**
   * Controls how Spark post-processes the raw change data returned by a connector.
   */
  public enum DeduplicationMode {
    /** Raw change stream with no post-processing. Suitable for audit/compliance use cases. */
    NONE,
    /** Remove identical insert/delete pairs produced by copy-on-write file rewrites. */
    DROP_CARRYOVERS,
    /** Collapse to one net change per row identity. */
    NET_CHANGES
  }

  private final ChangelogRange range;
  private final DeduplicationMode deduplicationMode;
  private final boolean computeUpdates;

  public ChangelogInfo(
      ChangelogRange range,
      DeduplicationMode deduplicationMode,
      boolean computeUpdates) {
    this.range = range;
    this.deduplicationMode = deduplicationMode;
    this.computeUpdates = computeUpdates;
  }

  /**
   * Returns the range of changes requested.
   */
  public ChangelogRange range() {
    return range;
  }

  /**
   * Returns the deduplication mode for post-processing change data.
   * <p>
   * Defaults to {@link DeduplicationMode#DROP_CARRYOVERS}.
   */
  public DeduplicationMode deduplicationMode() {
    return deduplicationMode;
  }

  /**
   * Returns whether Spark should derive {@code update_preimage}/{@code update_postimage}
   * from raw insert/delete pairs.
   * <p>
   * When {@code true} and the connector's {@link Changelog#representsUpdateAsDeleteAndInsert()}
   * also returns {@code true}, Spark will use window functions to identify insert/delete pairs
   * within the same commit version that share the same row identity and rewrite their
   * {@code _change_type} to {@code update_preimage}/{@code update_postimage}.
   */
  public boolean computeUpdates() {
    return computeUpdates;
  }

  /**
   * Creates a {@code ChangelogInfo} with default settings:
   * {@link DeduplicationMode#DROP_CARRYOVERS} and {@code computeUpdates = false}.
   */
  public static ChangelogInfo of(ChangelogRange range) {
    return new ChangelogInfo(range, DeduplicationMode.DROP_CARRYOVERS, false);
  }

  /**
   * Creates a {@code ChangelogInfo} with the specified range and deduplication mode,
   * and {@code computeUpdates = false}.
   */
  public static ChangelogInfo of(ChangelogRange range, DeduplicationMode deduplicationMode) {
    return new ChangelogInfo(range, deduplicationMode, false);
  }
}
