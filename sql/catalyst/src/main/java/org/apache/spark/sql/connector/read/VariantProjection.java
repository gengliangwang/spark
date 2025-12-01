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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.DataType;

/**
 * Represents a variant field projection that can be pushed down to the data source.
 * <p>
 * This class captures information about a specific field extraction from a variant column.
 * Data sources can use this information to optimize reading by extracting only the
 * requested fields from variant columns.
 * <p>
 * For example, if a query accesses {@code variant_get(v, '$.a.b', 'int')}, the projection
 * would have:
 * <ul>
 *   <li>columnName: ["v", "a", "b"] - the path from the root schema to the variant field</li>
 *   <li>expectedDataType: IntegerType - the expected data type of the extracted value</li>
 * </ul>
 *
 * @since 4.1.0
 */
@Evolving
public final class VariantProjection implements Serializable {
  private final String[] columnName;
  private final DataType expectedDataType;

  /**
   * Creates a variant projection.
   *
   * @param columnName The column path to the actual nested field in the variant from the root
   *                   schema. This is an array of field names representing the path from the
   *                   root of the table schema to the specific variant field being accessed.
   * @param expectedDataType The expected data type that the variant field should be cast to.
   */
  public VariantProjection(String[] columnName, DataType expectedDataType) {
    this.columnName = Objects.requireNonNull(columnName, "columnName cannot be null");
    this.expectedDataType = Objects.requireNonNull(expectedDataType, "expectedDataType cannot be null");
  }

  /**
   * Returns the column path to the variant field.
   * <p>
   * The path is represented as an array of field names from the root of the table schema
   * to the specific variant field being accessed.
   */
  public String[] columnName() {
    return columnName;
  }

  /**
   * Returns the expected data type for this variant projection.
   * <p>
   * This is the type that the variant field value should be cast to.
   */
  public DataType expectedDataType() {
    return expectedDataType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariantProjection that = (VariantProjection) o;
    return Arrays.equals(columnName, that.columnName) &&
           expectedDataType.equals(that.expectedDataType);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(expectedDataType);
    result = 31 * result + Arrays.hashCode(columnName);
    return result;
  }

  @Override
  public String toString() {
    return "VariantProjection{" +
           "columnName=" + Arrays.toString(columnName) +
           ", expectedDataType=" + expectedDataType +
           '}';
  }
}

