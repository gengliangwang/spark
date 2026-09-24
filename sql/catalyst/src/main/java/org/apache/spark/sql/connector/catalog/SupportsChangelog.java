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
 * A mix-in interface of {@link Table} for reading row-level changes.
 * <p>
 * Spark loads the base table using {@link TableCatalog#tableStateOptionKeys()} and can reuse that
 * instance for ordinary reads and changelog reads with matching table-state options. The changelog
 * range and post-processing parameters select a derived read of that table, rather than a separate
 * base table state.
 * <p>
 * Spark invokes this interface directly on its loaded table. The deprecated
 * {@link TableCatalog#loadChangelog} default delegates here as a migration aid for direct callers.
 *
 * @since 5.0.0
 */
@Evolving
public interface SupportsChangelog extends Table {

  /**
   * Returns a changelog derived from the state captured by this table instance.
   * <p>
   * This method must not refresh the base table. For a batch read with no ending bound, the
   * changelog ends at this table instance's captured state. For a streaming read with no ending
   * bound, the changelog remains open to subsequent changes. A changelog that supports both modes
   * must preserve this distinction when constructing its batch or streaming scan.
   * Repeated calls for equivalent captured table states and equal contexts must return changelogs
   * with equivalent read semantics and metadata.
   * <p>
   * Connector-specific options needed to construct the changelog must be declared by
   * {@link TableCatalog#tableStateOptionKeys()} and captured when the base table is loaded. Spark
   * passes each read's complete options to {@link Changelog#newScanBuilder} during scan planning.
   * The returned changelog declares its own read capabilities and schema. Spark uses the base
   * table's {@link Table#id()} and {@link Table#version()} for source identity and metadata
   * refresh, regardless of the corresponding methods on the returned changelog.
   *
   * @param context the changelog range and post-processing parameters
   * @return a changelog for the requested range
   */
  Changelog newChangelog(ChangelogContext context);
}
