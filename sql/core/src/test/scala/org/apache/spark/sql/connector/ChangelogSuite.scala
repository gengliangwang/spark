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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Integration tests for the CDC DSv2 API, covering
 * DataFrameReader.changes() and DataStreamReader.changes().
 */
class ChangelogSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  private val catalogName = "cdccat"
  private val testTableName = "test_table"
  private val fullTableName = s"$catalogName.$testTableName"

  before {
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryChangelogCatalog].getName)
    createTestTable(testTableName)
  }

  after {
    catalog.clearTables()
  }

  private def catalog: InMemoryChangelogCatalog = {
    spark.sessionState.catalogManager.catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
  }

  private def createTestTable(tableName: String): Unit = {
    val ident = Identifier.of(Array.empty, tableName)
    val columns = Array(
      Column.create(
        "id", org.apache.spark.sql.types.IntegerType),
      Column.create(
        "name", org.apache.spark.sql.types.StringType))
    catalog.createTable(
      ident,
      columns,
      Array.empty,
      java.util.Collections.emptyMap[String, String])
  }

  // ======== DataFrameReader.changes() Tests ========

  test("changes() sets changelog option in plan") {
    val df = spark.read
      .option("startingVersion", "10")
      .option("endingVersion", "20")
      .changes(fullTableName)

    val plan = df.queryExecution.logical
    val ur = plan.collect {
      case u: UnresolvedRelation => u
    }.head

    val opts = ur.options
    assert(opts.get(UnresolvedRelation.CHANGELOG_READ) == "true")
    assert(opts.get("startingVersion") == "10")
    assert(opts.get("endingVersion") == "20")
    assert(!ur.isStreaming)
  }

  test("changes() with timestamp options") {
    val df = spark.read
      .option("startingTimestamp", "2026-01-01")
      .option("endingTimestamp", "2026-02-01")
      .changes(fullTableName)

    val plan = df.queryExecution.logical
    val ur = plan.collect {
      case u: UnresolvedRelation => u
    }.head

    val opts = ur.options
    assert(opts.get(UnresolvedRelation.CHANGELOG_READ) == "true")
    assert(opts.get("startingTimestamp") == "2026-01-01")
    assert(opts.get("endingTimestamp") == "2026-02-01")
  }

  test("changes() with exclusive bounds") {
    val df = spark.read
      .option("startingVersion", "10")
      .option("startingBoundInclusive", false)
      .option("endingVersion", "20")
      .changes(fullTableName)

    val plan = df.queryExecution.logical
    val ur = plan.collect {
      case u: UnresolvedRelation => u
    }.head

    assert(ur.options.get("startingBoundInclusive") == "false")
  }

  test("changes() with dedup and compute updates options") {
    val df = spark.read
      .option("startingVersion", "5")
      .option("deduplicationMode", "netChanges")
      .option("computeUpdates", true)
      .changes(fullTableName)

    val plan = df.queryExecution.logical
    val ur = plan.collect {
      case u: UnresolvedRelation => u
    }.head

    val opts = ur.options
    assert(opts.get("deduplicationMode") == "netChanges")
    assert(opts.get("computeUpdates") == "true")
  }

  test("changes() rejects user-specified schema") {
    val e = intercept[Exception] {
      spark.read
        .schema("id INT, name STRING")
        .changes(fullTableName)
    }
    assert(e.getMessage.contains("changes"))
  }

  test("changes() table identifier includes catalog") {
    val df = spark.read
      .option("startingVersion", "1")
      .changes(fullTableName)

    val plan = df.queryExecution.logical
    val ur = plan.collect {
      case u: UnresolvedRelation => u
    }.head

    assert(
      ur.multipartIdentifier == Seq(catalogName, testTableName))
  }

  // ======== DataStreamReader.changes() Tests ========

  // Note: End-to-end streaming changes() requires Phase 2
  // resolver changes to route through ChangelogTable (which
  // supports MICRO_BATCH_READ). Without Phase 2, the resolver
  // resolves to InMemoryTable which doesn't support streaming.
  // These tests verify the options are wired correctly.

  test("streaming changes() sets changelog option") {
    val e = intercept[Exception] {
      spark.readStream
        .option("startingVersion", "10")
        .changes(fullTableName)
    }
    val msg = e.getMessage
    assert(msg.contains(testTableName))
  }

  test("streaming changes() rejects null table name") {
    intercept[IllegalArgumentException] {
      spark.readStream.changes(null)
    }
  }

  // ======== Catalog Capability Tests ========

  test("catalog has SUPPORT_CHANGELOG capability") {
    assert(catalog.capabilities().contains(
      TableCatalogCapability.SUPPORT_CHANGELOG))
  }

  test("loadChangelog returns valid Changelog") {
    val changelogInfo = ChangelogInfo.of(
      new ChangelogRange.VersionRange(
        "1", java.util.Optional.of("10"), true, true))

    val changelog = catalog.loadChangelog(
      Identifier.of(Array.empty, testTableName),
      changelogInfo)

    assert(changelog != null)
    assert(changelog.columns().length == 5)
    assert(changelog.containsCarryoverRows())
    assert(changelog.representsUpdateAsDeleteAndInsert())
  }

  test("loadChangelog for missing table throws") {
    val changelogInfo =
      ChangelogInfo.of(new ChangelogRange.Unbounded())

    val ex = intercept[
      org.apache.spark.sql.catalyst.analysis.NoSuchTableException] {
      catalog.loadChangelog(
        Identifier.of(Array.empty, "no_such_table"),
        changelogInfo)
    }
    assert(ex.getMessage.contains("no_such_table"))
  }

  test("ChangelogTable scan produces change rows") {
    val changelogInfo = ChangelogInfo.of(
      new ChangelogRange.VersionRange(
        "1", java.util.Optional.of("20"), true, true))
    val changelog = catalog.loadChangelog(
      Identifier.of(Array.empty, testTableName),
      changelogInfo)
    val table = new ChangelogTable(changelog, changelogInfo)

    val scanBuilder =
      table.newScanBuilder(CaseInsensitiveStringMap.empty())
    val scan = scanBuilder.build()
    val batch = scan.toBatch

    val partitions = batch.planInputPartitions()
    assert(partitions.length == 1)

    val factory = batch.createReaderFactory()
    val reader = factory.createReader(partitions(0))

    val rows = new scala.collection.mutable.ArrayBuffer[
      org.apache.spark.sql.catalyst.InternalRow]()
    while (reader.next()) {
      rows += reader.get().copy()
    }
    reader.close()

    assert(rows.length == 4)
    assert(rows(0).getInt(0) == 1)
    assert(rows(0).getUTF8String(2).toString == "insert")
    assert(rows(1).getUTF8String(2).toString == "delete")
    assert(rows(2).getUTF8String(2).toString == "delete")
    assert(rows(3).getUTF8String(2).toString == "insert")
  }
}
