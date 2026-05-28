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

package org.apache.spark.sql.execution.datasources.v2.hackparquet

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.files.SupportsNewFileWritePath
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The hackparquet DSv2 connector — registered as the V2 Parquet provider so the new
 * [[org.apache.spark.sql.connector.files.FileWrite]] write path is exercised whenever
 * `df.write.format("parquet")` resolves to V2. Reads are inherited from the existing
 * `ParquetTable` infrastructure so round-trip tests keep working.
 *
 * Implements [[SupportsNewFileWritePath]] so `DataFrameWriter` does not skip our provider for
 * writes (the generic file-source-V2 skip exists to avoid the broken legacy V2 write path; our
 * write path goes through `FileFormatWriter` and is the intended fix).
 */
class HackParquetDataSourceV2 extends FileDataSourceV2 with SupportsNewFileWritePath {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    new HackParquetTable(
      tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    new HackParquetTable(
      tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}
