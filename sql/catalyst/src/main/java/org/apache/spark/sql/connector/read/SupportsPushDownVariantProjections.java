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

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * support pushing down variant field projections to the data source.
 * <p>
 * When variant columns are accessed with specific field extractions (e.g., variant_get),
 * the optimizer can push these projections down to the data source. The data source can then
 * read only the required fields from variant columns, reducing I/O and improving performance.
 * <p>
 * Variant type projections can only be fully pushed down or not pushed down at all.
 * The method returns a boolean array indicating which projections were successfully pushed.
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsPushDownVariantProjections extends ScanBuilder {

  /**
   * Pushes down variant projections to the data source.
   * <p>
   * Each projection represents a specific field extraction from a variant column.
   * The data source should validate if each projection can be pushed down based on
   * its capabilities.
   * <p>
   * Variant projections can only be fully pushed down or not pushed down at all -
   * there is no partial pushdown for individual projections.
   *
   * @param projections Array of variant projections to push down
   * @return A boolean array with the same length as projections, where each element
   *         indicates whether the corresponding projection was successfully pushed down
   *         (true) or not (false)
   */
  boolean[] pushVariantProjections(VariantProjection[] projections);
}

