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

import java.util.Map;

import org.apache.spark.annotation.Unstable;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.FileIndex;

/**
 * A single (FileIndex, FileFormat) pair plus the metadata needed to construct a
 * {@code HadoopFsRelation} / {@code LogicalRelation}.
 *
 * @since 5.0.0
 */
@Unstable
public interface FileSet {

  /** The file index used to enumerate files for this set. */
  FileIndex index();

  /** The file format used to read files in this set (e.g. Parquet). */
  FileFormat format();

  /** Read options to thread through to the resulting {@code HadoopFsRelation}. */
  Map<String, String> options();

  /** Partition columns of this set, in partition order. */
  NamedReference[] partitionColumns();

  /** Data (non-partition) columns of this set. */
  NamedReference[] dataColumns();
}
