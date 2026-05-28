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

import java.util.Map

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * A minimal Parquet DSv2 connector that exercises the new [[org.apache.spark.sql.connector.files
 * .FileWrite]] write path. Sits alongside the existing `parquet` DSv2 connector under a separate
 * short name so the hackathon prototype does not perturb the production Parquet plumbing.
 *
 * Read support is intentionally omitted in this prototype — written data is verified by reading
 * back via the existing `parquet` source.
 */
class HackParquetDataSourceV2 extends TableProvider with DataSourceRegister {

  override def shortName(): String = "hackparquet"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: Map[String, String]): Table = {
    new HackParquetTable(schema, properties.asScala.toMap)
  }
}
