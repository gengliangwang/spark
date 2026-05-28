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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsDynamicOverwrite, SupportsTruncate, Write, WriteBuilder}

/**
 * Write builder for the hackparquet connector. Mixes in [[SupportsTruncate]] and
 * [[SupportsDynamicOverwrite]] so that the V2Writes rule accepts
 * `OverwriteByExpression` (with `Literal.TrueLiteral`, i.e. truncate) and
 * `OverwritePartitionsDynamic`. The selected mode is recorded so that `build()` produces a
 * [[HackParquetFileWrite]] configured accordingly (dynamicPartitionOverwrite=true for the
 * dynamic case).
 */
class HackParquetWriteBuilder(
    tableOptions: Map[String, String],
    info: LogicalWriteInfo)
  extends WriteBuilder with SupportsTruncate with SupportsDynamicOverwrite {

  private var dynamicPartitionOverwrite: Boolean = false

  override def truncate(): WriteBuilder = {
    // The OverwriteFilesByExpressionExec wipes the output directory before writing, so the
    // commit protocol does not need to do anything special for truncate.
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    dynamicPartitionOverwrite = true
    this
  }

  override def build(): Write = {
    val mergedOptions = tableOptions ++ info.options.asCaseSensitiveMap.asScala.toMap
    HackParquetFileWrite(mergedOptions, info, dynamicPartitionOverwrite)
  }
}
