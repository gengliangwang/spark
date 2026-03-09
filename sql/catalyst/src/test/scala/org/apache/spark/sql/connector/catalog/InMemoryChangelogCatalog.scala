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

package org.apache.spark.sql.connector.catalog

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * An in-memory catalog that supports CDC (Change Data Capture) via
 * [[TableCatalog.loadChangelog]]. Used for testing the CDC API.
 */
class InMemoryChangelogCatalog extends BasicInMemoryTableCatalog {
  import CatalogV2Implicits._

  override def capabilities(): util.Set[TableCatalogCapability] = {
    Set(TableCatalogCapability.SUPPORT_CHANGELOG).asJava
  }

  override def loadChangelog(
      ident: Identifier,
      changelogInfo: ChangelogInfo): Changelog = {
    Option(tables.get(ident)) match {
      case Some(_) =>
        new InMemoryChangelog(s"$name.${ident.quoted}", changelogInfo)
      case _ =>
        throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
  }
}

/**
 * A simple in-memory [[Changelog]] that returns hardcoded CDC rows.
 * The data simulates insert, delete, and update change events.
 */
class InMemoryChangelog(
    override val name: String,
    val changelogInfo: ChangelogInfo) extends Changelog {

  override def columns(): Array[Column] = InMemoryChangelog.columns

  override def containsCarryoverRows(): Boolean = true

  override def containsIntermediateChanges(): Boolean = false

  override def representsUpdateAsDeleteAndInsert(): Boolean = true

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryChangelogScanBuilder()
  }
}

object InMemoryChangelog {
  val schema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("_change_type", StringType)
    .add("_commit_version", LongType)
    .add("_commit_timestamp", TimestampType)

  val columns: Array[Column] = CatalogV2Util.structTypeToV2Columns(schema)

  val testData: Array[InternalRow] = Array(
    InternalRow(1, UTF8String.fromString("Alice"),
      UTF8String.fromString("insert"), 12L, 1705312200000000L),
    InternalRow(2, UTF8String.fromString("Bob"),
      UTF8String.fromString("delete"), 14L, 1705385600000000L),
    InternalRow(3, UTF8String.fromString("Charlie"),
      UTF8String.fromString("delete"), 18L, 1705752000000000L),
    InternalRow(3, UTF8String.fromString("Charles"),
      UTF8String.fromString("insert"), 18L, 1705752000000000L)
  )
}

class InMemoryChangelogScanBuilder extends ScanBuilder {
  override def build(): Scan = new InMemoryChangelogScan()
}

class InMemoryChangelogScan extends Scan with Batch {
  override def readSchema(): StructType = InMemoryChangelog.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(InMemoryChangelogPartition(0))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    InMemoryChangelogReaderFactory
  }
}

case class InMemoryChangelogPartition(index: Int) extends InputPartition

object InMemoryChangelogReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PartitionReader[InternalRow] {
      private val data = InMemoryChangelog.testData
      private var current = -1

      override def next(): Boolean = {
        current += 1
        current < data.length
      }

      override def get(): InternalRow = data(current)

      override def close(): Unit = {}
    }
  }
}
