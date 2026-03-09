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
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A connector interface for exposing row-level change data (CDC) from a table.
 * <p>
 * Connectors implement this interface to provide raw change data to Spark. Spark wraps the
 * {@code Changelog} in an internal {@code ChangelogTable} and handles post-processing
 * (carry-over removal, update detection, net change computation) based on the properties
 * declared by this interface.
 * <p>
 * The schema returned by {@link #columns()} must include three metadata columns:
 * <ul>
 *   <li>{@code _change_type} (STRING) — the kind of change: {@code insert}, {@code delete},
 *       {@code update_preimage}, or {@code update_postimage}</li>
 *   <li>{@code _commit_version} (connector-defined type, e.g., LONG) — the version
 *       containing this change</li>
 *   <li>{@code _commit_timestamp} (TIMESTAMP) — the timestamp of the commit</li>
 * </ul>
 * <p>
 * Instances are created by {@link TableCatalog#loadChangelog(Identifier, ChangelogInfo)} and
 * are not expected to be thread-safe.
 *
 * @since 4.2.0
 */
@Evolving
public interface Changelog {

  /**
   * A name to identify this changelog. Typically includes the source table name,
   * e.g., {@code "my_table$changelog"}.
   */
  String name();

  /**
   * Returns the columns of the change data, including data columns and the required
   * metadata columns ({@code _change_type}, {@code _commit_version},
   * {@code _commit_timestamp}).
   */
  Column[] columns();

  /**
   * Returns whether the change data may contain carry-over rows.
   * <p>
   * Copy-on-write (COW) table formats rewrite entire data files on updates, producing
   * identical insert/delete pairs for unchanged rows in the same file. When this returns
   * {@code true}, Spark may inject carry-over removal logic (depending on the
   * {@link ChangelogInfo#deduplicationMode()}).
   * <p>
   * Returning {@code false} guarantees no carry-over rows are present, so Spark skips
   * carry-over removal entirely.
   */
  boolean containsCarryoverRows();

  /**
   * Returns whether the change data may contain multiple intermediate changes per row
   * within the queried range.
   * <p>
   * When {@code true} and the deduplication mode is
   * {@link ChangelogInfo.DeduplicationMode#NET_CHANGES}, Spark collapses intermediate
   * changes to produce one net change per row identity.
   * <p>
   * Returning {@code false} guarantees at most one change per row identity per commit
   * version, so Spark skips net change computation.
   */
  boolean containsIntermediateChanges();

  /**
   * Returns whether updates are represented as raw delete/insert pairs rather than
   * explicit {@code update_preimage}/{@code update_postimage} entries.
   * <p>
   * When this returns {@code true} and {@link ChangelogInfo#computeUpdates()} is also
   * {@code true}, Spark derives update operations from insert/delete pairs that share the
   * same row identity and commit version.
   * <p>
   * Returning {@code false} means the connector already emits fully materialized
   * {@code update_preimage} and {@code update_postimage} entries. Spark will not attempt
   * update detection.
   */
  boolean representsUpdateAsDeleteAndInsert();

  /**
   * Returns a {@link ScanBuilder} for reading the raw change data.
   *
   * @param options read options, which is an immutable case-insensitive string-to-string map
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);

  /**
   * Returns the columns that uniquely identify a row, used for update detection and
   * net change computation.
   * <p>
   * Required when {@link #representsUpdateAsDeleteAndInsert()} or
   * {@link #containsIntermediateChanges()} returns {@code true}.
   *
   * @return array of named references to identity columns, or empty array if not applicable
   */
  default NamedReference[] rowId() {
    return new NamedReference[0];
  }

  /**
   * Returns the column used for ordering changes within the same row identity,
   * used for update detection.
   * <p>
   * Typically {@code _commit_version} or a similar monotonically increasing column.
   * Required when {@link #representsUpdateAsDeleteAndInsert()} returns {@code true}
   * and update detection is requested.
   *
   * @return a named reference to the ordering column, or {@code null} if not applicable
   */
  default NamedReference rowVersion() {
    return null;
  }
}
