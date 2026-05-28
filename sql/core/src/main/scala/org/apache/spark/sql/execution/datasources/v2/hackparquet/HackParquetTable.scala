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

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

/**
 * A minimal writeable Parquet [[org.apache.spark.sql.connector.catalog.Table]] backing the
 * hackparquet DSv2 connector. Returns a [[HackParquetWriteBuilder]] that ultimately yields a
 * [[org.apache.spark.sql.connector.files.FileWrite]] for the new exec-node path.
 */
class HackParquetTable(
    private val tableSchema: StructType,
    private val options: Map[String, String])
  extends SupportsWrite {

  override def name(): String = "hackparquet"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC,
      TableCapability.TRUNCATE).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new HackParquetWriteBuilder(options, info)
  }
}
