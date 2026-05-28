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

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Extends [[ParquetTable]] so reads (and all the FileTable/FileScanBuilder machinery) keep
 * working, while overriding the write path to use the new [[org.apache.spark.sql.connector.files
 * .FileWrite]] flow.
 */
class HackParquetTable(
    tableName: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends ParquetTable(
    tableName, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val outputPath = paths.headOption.getOrElse(
      throw new IllegalArgumentException("hackparquet write requires at least one path"))
    val tableOptions = options.asCaseSensitiveMap().asScala.toMap + ("path" -> outputPath)
    new HackParquetWriteBuilder(tableOptions, info)
  }

  override def capabilities(): util.Set[TableCapability] = {
    val caps = util.EnumSet.copyOf(super.capabilities())
    caps.add(TRUNCATE)
    caps.add(OVERWRITE_BY_FILTER)
    caps.add(OVERWRITE_DYNAMIC)
    caps
  }
}
