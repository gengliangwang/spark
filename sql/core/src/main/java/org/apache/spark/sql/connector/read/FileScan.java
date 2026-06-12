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

import org.apache.spark.annotation.Unstable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;

/**
 * A {@link Scan} that exposes its file-source structure to the planner. When the planner sees a
 * {@code FileScan}, it rewrites the DSv2 scan into a V1 {@code LogicalRelation} backed by a
 * {@code HadoopFsRelation} and re-plans it through {@code FileSourceStrategy}, picking up the
 * full V1 file-source execution path (including Photon).
 *
 * <p>Connectors that want to reuse V1 file-source execution implement this on their {@code Scan}
 * in addition to (or instead of) the standard DSv2 {@link Batch} surface.
 *
 * @since 5.0.0
 */
@Unstable
public interface FileScan extends Scan {

  /**
   * Partition-column predicates the connector reports as having applied at file-listing time.
   * Informational only: the planner re-derives partition/data filters from the post-scan predicates
   * left by {@code PhysicalOperation} and does not re-apply these (re-adding a predicate the
   * connector already removed from the post-scan set would evaluate it twice).
   */
  Expression[] partitionFilters();

  /**
   * Data-column predicates the connector reports as having pushed into the file reader.
   * Informational only, like {@link #partitionFilters()}: the planner does not re-apply these.
   */
  Expression[] dataFilters();

  /**
   * Plans this scan as a batch over one or more file sets.
   */
  FileBatch planFileBatch();

  /**
   * Additional filters implied by {@code filters} that the planner should include in the
   * re-planned V1 plan -- e.g. partition predicates derived from generated-column expressions --
   * resolved against {@code output} (the attributes of the relation the planner synthesized) so
   * the returned references carry that relation's attribute ids. The planner folds literal-only
   * subexpressions afterwards (the optimizer has already run by lowering time), so connectors may
   * return unfolded expressions.
   */
  default Expression[] derivePartitionFilters(Expression[] filters, Attribute[] output) {
    return new Expression[0];
  }

  /**
   * Rebuilds this scan's {@code _metadata} column from {@code metadata} -- the synthesized
   * relation's metadata attribute -- materializing subfields the V1 file format does not produce
   * on its own (e.g. a connector may coalesce row-tracking subfields from materialized values
   * and computed defaults). Return {@code metadata} itself when nothing
   * needs rebuilding; otherwise the planner projects the returned expression in place of the
   * attribute, re-aliased to this scan's expr id.
   */
  default NamedExpression rewriteMetadataColumn(AttributeReference metadata) {
    return metadata;
  }
}
