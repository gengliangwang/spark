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

import java.util.Collections

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.V2TableReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.ChangelogRange
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogUtil, DataSourceV2Relation, DataSourceV2ScanRelation, StreamingDataSourceV2Relation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for the CDC (Change Data Capture) analyzer resolution path:
 * RelationChanges -> resolveChangelog -> DataSourceV2Relation(Changelog).
 */
class ChangelogResolutionSuite extends SharedSparkSession {

  private val cdcCatalogName = "cdc_catalog"
  private val noCdcCatalogName = "no_cdc_catalog"
  private val ident = Identifier.of(Array.empty, "test_table")

  private def cdcCatalog: InMemoryChangelogCatalog = {
    spark.sessionState.catalogManager.catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.sql.catalog.$cdcCatalogName",
      classOf[LegacyChangelogOverrideCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$cdcCatalogName.tableStateOptionKeys", "branch")
    spark.conf.set(s"spark.sql.catalog.$noCdcCatalogName",
      classOf[InMemoryTableCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$cdcCatalogName")
    spark.conf.unset(s"spark.sql.catalog.$cdcCatalogName.tableStateOptionKeys")
    spark.conf.unset(s"spark.sql.catalog.$noCdcCatalogName")
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val catalog = spark.sessionState.catalogManager.catalog(cdcCatalogName).asTableCatalog
    val ident = Identifier.of(Array.empty, "test_table")
    if (catalog.tableExists(ident)) {
      catalog.dropTable(ident)
    }
    catalog.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())

    val noCdcCat = spark.sessionState.catalogManager.catalog(noCdcCatalogName).asTableCatalog
    val ident2 = Identifier.of(Array.empty, "test_table")
    if (noCdcCat.tableExists(ident2)) {
      noCdcCat.dropTable(ident2)
    }
    noCdcCat.createTable(
      ident2,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
  }

  test("CHANGES clause resolves to DataSourceV2Relation with the connector Changelog") {
    val df = sql(
      s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val analyzed = df.queryExecution.analyzed
    val dsv2Relations = analyzed.collect {
      case r: DataSourceV2Relation => r
    }
    assert(dsv2Relations.length == 1)
    assert(dsv2Relations.head.table.isInstanceOf[InMemoryChangelog])
    val changelogTable = dsv2Relations.head.table.asInstanceOf[Changelog]
    assert(changelogTable.name().endsWith("test_table_changelog"))
    assert(changelogTable.context() == cdcCatalog.lastChangelogContext.get)
  }

  test("CHANGES clause - table without SupportsChangelog throws") {
    checkError(
      intercept[AnalysisException] {
        sql(s"SELECT * FROM $noCdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE_ON_RELATION",
      parameters = Map("relationId" -> s"`$noCdcCatalogName`.`test_table`"))
  }

  test("CHANGES clause - table not found throws") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT * FROM $cdcCatalogName.nonexistent CHANGES FROM VERSION 1 TO VERSION 5")
    }
    assert(e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") ||
      e.getMessage.contains("nonexistent"))
  }

  test("DataFrame API - changes() resolves correctly") {
    val df = spark.read
      .option("startingVersion", "1")
      .option("endingVersion", "5")
      .changes(s"$cdcCatalogName.test_table")
    val analyzed = df.queryExecution.analyzed
    val dsv2Relations = analyzed.collect {
      case r: DataSourceV2Relation => r
    }
    assert(dsv2Relations.length == 1)
    assert(dsv2Relations.head.table.isInstanceOf[InMemoryChangelog])
  }

  test("DataFrame API - changes() on catalog without CDC throws") {
    checkError(
      intercept[AnalysisException] {
        spark.read
          .option("startingVersion", "1")
          .changes(s"$noCdcCatalogName.test_table")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE_ON_RELATION",
      parameters = Map("relationId" -> s"`$noCdcCatalogName`.`test_table`"))
  }

  test("CHANGES clause - schema includes CDC metadata columns") {
    val df = sql(
      s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val colNames = df.schema.fieldNames
    assert(colNames.contains("id"))
    assert(colNames.contains("data"))
    assert(colNames.contains("_change_type"))
    assert(colNames.contains("_commit_version"))
    assert(colNames.contains("_commit_timestamp"))
  }

  test("DataStreamReader - changes() rejects user-specified schema") {
    val e = intercept[AnalysisException] {
      import org.apache.spark.sql.types.StructType
      spark.readStream
        .schema(new StructType().add("id", LongType))
        .changes(s"$cdcCatalogName.test_table")
    }
    assert(e.getMessage.contains("changes"))
  }

  test("DataStreamReader - changes() resolves to StreamingRelationV2 with Changelog") {
    val df = spark.readStream
      .option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table")
    val analyzed = df.queryExecution.analyzed
    val streamRelations = analyzed.collect {
      case r: StreamingRelationV2 => r
    }
    assert(streamRelations.length == 1)
    assert(streamRelations.head.table.isInstanceOf[InMemoryChangelog])
    val colNames = df.schema.fieldNames
    assert(colNames.contains("_change_type"))
    assert(colNames.contains("_commit_version"))
    assert(colNames.contains("_commit_timestamp"))
  }

  test("DataStreamReader - changes() on catalog without CDC throws") {
    checkError(
      intercept[AnalysisException] {
        spark.readStream
          .option("startingVersion", "1")
          .changes(s"$noCdcCatalogName.test_table")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE_ON_RELATION",
      parameters = Map("relationId" -> s"`$noCdcCatalogName`.`test_table`"))
  }

  test("CHANGES clause on CTE relation throws") {
    checkError(
      intercept[AnalysisException] {
        sql("WITH x AS (SELECT 1) SELECT * FROM x CHANGES FROM VERSION 1 TO VERSION 5")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE_ON_RELATION",
      sqlState = None,
      parameters = Map("relationId" -> "`x`"))
  }

  test("CHANGES clause passes changelogContext to the loaded table") {
    sql(s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    val info = cat.lastChangelogContext
    assert(info.isDefined)
    val range = info.get.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(range.startingVersion() == "1")
    assert(range.endingVersion().get() == "5")
  }

  test("changes() filters table-state options and forwards complete scan options") {
    val cat = cdcCatalog
    cat.resetLoadTableCalls()
    val df = spark.read
      .option("startingVersion", "1")
      .option("BrAnCh", "main")
      .option("customOption", "customValue")
      .changes(s"$cdcCatalogName.test_table")
    df.queryExecution.optimizedPlan

    assert(cat.loadTableCalls.nonEmpty)
    assert(cat.loadTableCalls.forall { case (_, options) =>
      options.size() == 1 && options.get("branch") == "main"
    })
    val opts = cat.lastScanOptions
    assert(opts.isDefined)
    assert(opts.get.get("customOption") == "customValue")
    assert(opts.get.get("startingVersion") == "1")
    assert(opts.get.get("branch") == "main")
  }

  test("CHANGES WITH filters table-state options and forwards complete scan options") {
    val cat = cdcCatalog
    cat.resetLoadTableCalls()
    sql(s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 " +
      "WITH ('branch' = 'main', 'customOption' = 'customValue')")
      .queryExecution.optimizedPlan

    assert(cat.loadTableCalls.nonEmpty)
    assert(cat.loadTableCalls.forall { case (_, options) =>
      options.size() == 1 && options.get("branch") == "main"
    })
    val opts = cat.lastScanOptions
    assert(opts.isDefined)
    assert(opts.get.get("customOption") == "customValue")
    assert(opts.get.get("branch") == "main")
  }

  test("streaming changes() filters state options and retains complete relation options") {
    val cat = cdcCatalog
    cat.resetLoadTableCalls()
    val analyzed = spark.readStream
      .option("startingVersion", "1")
      .option("branch", "main")
      .option("customOption", "customValue")
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed

    assert(cat.loadTableCalls.size == 1)
    assert(cat.lastLoadTableOptions.get.size() == 1)
    assert(cat.lastLoadTableOptions.get.get("branch") == "main")
    val relation = analyzed.collectFirst { case r: StreamingRelationV2 => r }.get
    assert(relation.extraOptions.get("customOption") == "customValue")
    assert(relation.extraOptions.get("startingVersion") == "1")
    assert(relation.extraOptions.get("branch") == "main")
  }

  test("streaming CHANGES WITH filters state options and retains complete relation options") {
    val cat = cdcCatalog
    cat.resetLoadTableCalls()
    val analyzed = sql(
      s"SELECT * FROM STREAM $cdcCatalogName.test_table CHANGES FROM VERSION 1 " +
        "WITH ('branch' = 'main', 'customOption' = 'customValue')").queryExecution.analyzed

    assert(cat.loadTableCalls.size == 1)
    assert(cat.lastLoadTableOptions.get.size() == 1)
    assert(cat.lastLoadTableOptions.get.get("branch") == "main")
    val relation = analyzed.collectFirst { case r: StreamingRelationV2 => r }.get
    assert(relation.extraOptions.get("customOption") == "customValue")
    assert(relation.extraOptions.get("branch") == "main")
  }

  // ===========================================================================
  // Shared table state and refresh
  // ===========================================================================

  private def changeRow(id: Long, version: Long): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(s"data-$id"),
      UTF8String.fromString(Changelog.CHANGE_TYPE_INSERT),
      version,
      version * 1000000L)
  }

  gridTest("ordinary and changelog reads share table state in either order")(
      Seq(true, false)) { changelogFirst =>
    val tableName = s"$cdcCatalogName.test_table"
    sql(s"INSERT INTO $tableName VALUES (10, 'current')")
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    cat.resetLoadTableCalls()

    val ordinary = s"SELECT id FROM $tableName " +
      "WITH ('branch' = 'main', 'split-size' = '5')"
    val changes = s"SELECT id FROM $tableName CHANGES FROM VERSION 1 " +
      "WITH ('BrAnCh' = 'main', 'split-size' = '9')"
    val reads = if (changelogFirst) Seq(changes, ordinary) else Seq(ordinary, changes)
    val df = sql(reads.mkString(" UNION ALL "))
    val analyzedRelations = df.queryExecution.analyzed.collect {
      case r: DataSourceV2Relation => r
    }
    val changelog = analyzedRelations.collectFirst {
      case r if r.table.isInstanceOf[InMemoryChangelog] =>
        r.table.asInstanceOf[Changelog]
    }.get
    val base = analyzedRelations.find(r => !r.table.isInstanceOf[InMemoryChangelog]).get.table
    assert(changelog.baseTable() eq base)
    assert(cat.loadTableCalls.size == 1)
    assert(cat.lastLoadTableOptions.get.size() == 1)
    assert(cat.lastLoadTableOptions.get.get("branch") == "main")

    cat.resetLoadTableCalls()
    QueryTest.checkAnswer(df, Seq(Row(1L), Row(10L)), checkToRDD = false)
    assert(cat.loadTableCalls.size == 1)
    val refreshed = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation => r.relation
    }
    val refreshedChangelog = refreshed.collectFirst {
      case r if r.table.isInstanceOf[InMemoryChangelog] =>
        r.table.asInstanceOf[Changelog]
    }.get
    val refreshedBase = refreshed.find(r => !r.table.isInstanceOf[InMemoryChangelog]).get.table
    assert(refreshedChangelog.baseTable() eq refreshedBase)
    assert(refreshed.map(_.options.get("split-size")).sorted == Seq("5", "9"))
    assert(cat.lastScanOptions.get.get("split-size") == "9")
  }

  test("changelog contexts distinguish reads while sharing a base table") {
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L), changeRow(2L, 2L)))
    cat.resetLoadTableCalls()
    val tableName = s"$cdcCatalogName.test_table"
    val df = sql(
      s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('split-size' = '5') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 2 TO VERSION 2 " +
        "WITH ('split-size' = '9') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('split-size' = '7')")
    val relations = df.queryExecution.analyzed.collect { case r: DataSourceV2Relation => r }
    val changelogs = relations.map(_.table.asInstanceOf[Changelog])
    assert(changelogs.size == 3)
    assert(changelogs.forall(_.baseTable() eq changelogs.head.baseTable()))
    assert(changelogs.head ne changelogs.last)
    assert(changelogs.head == changelogs.last)
    assert(Set(changelogs.head).contains(changelogs.last))
    assert(changelogs.head != changelogs(1))
    assert(changelogs.head != changelogs.head.baseTable())
    assert(!relations.head.sameResult(relations.last))
    val sameOptions = relations.last.copy(options = relations.head.options)
    assert(relations.head.sameResult(sameOptions))
    assert(relations.head.semanticHash() == sameOptions.semanticHash())
    val sameOutput = sameOptions.copy(output = relations.head.output)
    assert(relations.head == sameOutput)
    assert(relations.head.hashCode() == sameOutput.hashCode())
    assert(!relations.head.sameResult(relations(1).copy(options = relations.head.options)))
    assert(cat.loadTableCalls.size == 1)
    assert(cat.lastLoadTableOptions.get.isEmpty)

    cat.resetLoadTableCalls()
    QueryTest.checkAnswer(df, Seq(Row(1L), Row(1L), Row(2L)), checkToRDD = false)
    assert(cat.loadTableCalls.size == 1)
    val scanOptions = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation => r.relation.options.get("split-size")
    }
    assert(scanOptions.sorted == Seq("5", "7", "9"))
  }

  test("streaming changelog identity uses base state, context, and scan options") {
    val analyzed = spark.readStream.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table").queryExecution.analyzed
    val original = analyzed.collectFirst { case r: StreamingRelationV2 => r }.get
    val captured = original.table.asInstanceOf[Changelog]
    val changelog = ChangelogUtil.create(captured.baseTable(), captured.context())
    val independentCopy = original.copy(table = changelog)
    assert(original == independentCopy)
    assert(original.hashCode() == independentCopy.hashCode())
    val independent = independentCopy.newInstance().asInstanceOf[StreamingRelationV2]

    assert(original.table ne independent.table)
    assert(original.sameResult(independent))
    assert(original.semanticHash() == independent.semanticHash())
    val changedOptions = new CaseInsensitiveStringMap(
      Collections.singletonMap("split-size", "5"))
    assert(!original.sameResult(independent.copy(extraOptions = changedOptions)))

    val otherContext = new ChangelogContext(
      new ChangelogRange.VersionRange("2", java.util.Optional.empty[String](), true, true),
      captured.context().deduplicationMode(),
      captured.context().computeUpdates())
    val otherChangelog = ChangelogUtil.create(captured.baseTable(), otherContext)
    assert(!original.sameResult(independent.copy(table = otherChangelog)))
    assert(!original.sameResult(independent.copy(table = captured.baseTable())))

    val executionRelation = StreamingDataSourceV2Relation(
      original.table, original.output, original.catalog, original.identifier,
      original.extraOptions, "metadata")
    val independentExecution = executionRelation.copy(table = independentCopy.table)
    assert(executionRelation == independentExecution)
    assert(executionRelation.hashCode() == independentExecution.hashCode())
    val newInstance = independentExecution.newInstance()
    assert(newInstance.table eq independentCopy.table)
    assert(executionRelation.sameResult(newInstance))
    assert(executionRelation.semanticHash() == newInstance.semanticHash())
    assert(!executionRelation.sameResult(independentExecution.copy(table = otherChangelog)))
  }

  test("changelog reads with different state options load separate base tables") {
    val cat = cdcCatalog
    cat.resetLoadTableCalls()
    val tableName = s"$cdcCatalogName.test_table"
    val df = sql(
      s"SELECT id FROM $tableName CHANGES FROM VERSION 1 " +
        "WITH ('branch' = 'main') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 1 " +
        "WITH ('branch' = 'dev')")
    val changelogs = df.queryExecution.analyzed.collect {
      case r: DataSourceV2Relation => r.table.asInstanceOf[Changelog]
    }
    assert(changelogs.size == 2)
    assert(changelogs.head.baseTable() ne changelogs.last.baseTable())
    assert(changelogs.head != changelogs.last)
    assert(cat.loadTableCalls.map(_._2.get("branch")).sorted == Seq("dev", "main"))

    cat.resetLoadTableCalls()
    df.collect()
    assert(cat.loadTableCalls.map(_._2.get("branch")).sorted == Seq("dev", "main"))
  }

  test("execution refresh derives changelogs from the refreshed base state") {
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    val df = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table").select("id")
    df.queryExecution.analyzed

    cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
    cat.resetLoadTableCalls()
    QueryTest.checkAnswer(df, Seq(Row(1L), Row(2L)), checkToRDD = false)
    assert(cat.loadTableCalls.size == 1)
    assert(cat.lastLoadTableOptions.get.isEmpty)
  }

  test("execution refresh validates the newly derived changelog schema") {
    val df = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table")
    df.queryExecution.analyzed
    cdcCatalog.alterTable(ident, TableChange.deleteColumn(Array("data"), false))

    checkError(
      intercept[AnalysisException] { df.collect() },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
      parameters = Map(
        "tableName" -> s"`$cdcCatalogName`.`test_table`",
        "errors" -> "- `data` STRING has been removed"))
  }

  test("an open-ended changelog pins batch state and follows new streaming changes") {
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    val options = CaseInsensitiveStringMap.empty()
    val base = cat.loadTable(ident, new TableContext(null, null), options)
      .asInstanceOf[SupportsChangelog]
    cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
    val context = new ChangelogContext(
      new ChangelogRange.VersionRange("1", java.util.Optional.empty[String](), true, true),
      ChangelogContext.DeduplicationMode.DROP_CARRYOVERS,
      false)
    val changelog = base.newChangelog(context)
    assert(changelog.baseTable() eq base)
    assert(changelog.context() eq context)
    assert(changelog.id() != null)
    assert(changelog.id() == base.id())
    assert(changelog.version() == base.version())
    val scan = changelog.newScanBuilder(options).build()

    def readIds(
        partitions: Array[InputPartition],
        factory: PartitionReaderFactory): Seq[Long] = {
      partitions.toSeq.flatMap { partition =>
        val reader = factory.createReader(partition)
        try {
          Iterator.continually(reader.next()).takeWhile(identity)
            .map(_ => reader.get().getLong(0)).toVector
        } finally {
          reader.close()
        }
      }
    }

    val batch = scan.toBatch()
    assert(readIds(batch.planInputPartitions(), batch.createReaderFactory()) == Seq(1L))
    withTempDir { dir =>
      val stream = scan.toMicroBatchStream(dir.getCanonicalPath)
      try {
        val end = stream.latestOffset()
        assert(readIds(
          stream.planInputPartitions(stream.initialOffset(), end),
          stream.createReaderFactory()) == Seq(1L, 2L))
        stream.commit(end)
        cat.addChangeRows(ident, Seq(changeRow(3L, 3L)))
        assert(readIds(
          stream.planInputPartitions(end, stream.latestOffset()),
          stream.createReaderFactory()) == Seq(3L))
      } finally {
        stream.stop()
      }
    }
    assert(readIds(batch.planInputPartitions(), batch.createReaderFactory()) == Seq(1L))
  }

  test("execution refresh rejects changes to changelog post-processing metadata") {
    val cat = cdcCatalog
    val df = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table")
    df.queryExecution.analyzed
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id")))

    checkError(
      intercept[AnalysisException] { df.collect() },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.CHANGELOG_METADATA_MISMATCH",
      parameters = Map(
        "tableName" -> s"`$cdcCatalogName`.`test_table`",
        "errors" -> "- containsIntermediateChanges changed\n- rowId changed"))
  }

  test("cached changelogs remain separate from ordinary tables after refresh") {
    val tableName = s"$cdcCatalogName.test_table"
    sql(s"INSERT INTO $tableName VALUES (10, 'current')")
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    val cached = sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 1").cache()
    val cacheManager = spark.sharedState.cacheManager
    try {
      checkAnswer(cached.select("id"), Seq(Row(1L)))
      assert(cacheManager.numCachedEntries == 1)
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(10L)))

      cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
      spark.catalog.refreshTable(tableName)

      assert(cacheManager.numCachedEntries == 1)
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(10L)))
      val refreshedChanges = sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 1")
      assert(refreshedChanges.queryExecution.analyzed.collect {
        case r: DataSourceV2Relation => r.table.isInstanceOf[InMemoryChangelog]
      } == Seq(true))
      checkAnswer(refreshedChanges.select("id"), Seq(Row(1L), Row(2L)))
    } finally {
      spark.catalog.clearCache()
    }
  }

  test("identical changelog reads reuse cached results before and after refresh") {
    val tableName = s"$cdcCatalogName.test_table"
    sql(s"INSERT INTO $tableName VALUES (10, 'current')")
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    def readChanges(): DataFrame = {
      spark.read.option("startingVersion", "1").changes(tableName)
    }
    val cached = readChanges().cache()
    val cacheManager = spark.sharedState.cacheManager
    try {
      checkAnswer(cached.select("id"), Seq(Row(1L)))
      val repeated = readChanges()
      assert(cacheManager.lookupCachedData(repeated).isDefined)
      assertCached(repeated)
      checkAnswer(repeated.select("id"), Seq(Row(1L)))
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(10L)))

      cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
      spark.catalog.refreshTable(tableName)

      val refreshed = readChanges()
      val recached = cacheManager.lookupCachedData(refreshed)
      assert(recached.isDefined)
      assert(recached.get.plan.collect {
        case r: DataSourceV2Relation => r.table.isInstanceOf[InMemoryChangelog]
      } == Seq(true))
      assertCached(refreshed)
      checkAnswer(refreshed.select("id"), Seq(Row(1L), Row(2L)))
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(10L)))
    } finally {
      spark.catalog.clearCache()
    }
  }

  test("DataFrame temp views preserve changelog context when loading fresh table state") {
    withTempView("cdc_view") {
      val cat = cdcCatalog
      cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
      val changes = spark.read.option("startingVersion", "1")
        .changes(s"$cdcCatalogName.test_table")
      val original = changes.queryExecution.analyzed.collectFirst {
        case r: DataSourceV2Relation => r.table.asInstanceOf[Changelog]
      }.get
      changes.createOrReplaceTempView("cdc_view")

      cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
      val fromView = spark.table("cdc_view")
      val relation = fromView.queryExecution.analyzed.collectFirst {
        case r: DataSourceV2Relation => r
      }.get
      assert(relation.table.isInstanceOf[InMemoryChangelog])
      assert(relation.changelogResolved)
      val changelog = relation.table.asInstanceOf[Changelog]
      assert(changelog.context() == original.context())
      assert(changelog.baseTable() ne original.baseTable())
      checkAnswer(fromView.select("id"), Seq(Row(1L), Row(2L)))
    }
  }

  test("different changelog temp views and ordinary reads share only base table state") {
    withTempView("cdc_first", "cdc_second") {
      val tableName = s"$cdcCatalogName.test_table"
      sql(s"INSERT INTO $tableName VALUES (10, 'current')")
      val cat = cdcCatalog
      cat.addChangeRows(ident, Seq(changeRow(1L, 1L), changeRow(2L, 2L)))
      // SQL ranges leave all three option maps empty, so only the context distinguishes reads.
      sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1")
        .createOrReplaceTempView("cdc_first")
      sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 2 TO VERSION 2")
        .createOrReplaceTempView("cdc_second")
      cat.resetLoadTableCalls()

      val df = sql("SELECT id FROM cdc_first UNION ALL SELECT id FROM cdc_second " +
        s"UNION ALL SELECT id FROM $tableName")
      val relations = df.queryExecution.analyzed.collect {
        case r: DataSourceV2Relation => r
      }
      assert(relations.size == 3)
      assert(relations.forall(_.options.isEmpty))
      val baseTables = relations.map(_.table).map {
        case changelog: Changelog => changelog.baseTable()
        case table => table
      }
      assert(baseTables.forall(_ eq baseTables.head))
      val changelogRelations = relations.filter(_.table.isInstanceOf[Changelog])
      assert(changelogRelations.size == 2)
      assert(changelogRelations.forall(_.table.isInstanceOf[InMemoryChangelog]))
      assert(changelogRelations.forall(_.changelogResolved))
      val changelogs = changelogRelations.map(_.table.asInstanceOf[Changelog])
      assert(changelogs.head.context() != changelogs.last.context())
      assert(relations.count(r => !r.table.isInstanceOf[Changelog]) == 1)
      assert(cat.loadTableCalls.size == 1)

      cat.resetLoadTableCalls()
      QueryTest.checkAnswer(df, Seq(Row(1L), Row(2L), Row(10L)), checkToRDD = false)
      assert(cat.loadTableCalls.size == 1)
    }
  }

  test("transaction table references retain changelog context and base table identity") {
    val changes = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table")
    val original = changes.queryExecution.analyzed.collectFirst {
      case r: DataSourceV2Relation => r
    }.get
    val changelog = original.table.asInstanceOf[Changelog]
    val reference = V2TableReference.createForTransaction(original)
    assert(changelog.baseTable().id() != null)
    assert(changelog.id() == changelog.baseTable().id())
    assert(reference.info.tableId.contains(changelog.baseTable().id()))
    assert(reference.changelog.contains(changelog))
    assert(reference.changelogResolved == original.changelogResolved)

    val rederived = reference.toRelation(changelog.baseTable())
    assert(rederived.table.isInstanceOf[InMemoryChangelog])
    assert(rederived.table ne original.table)
    val rederivedChangelog = rederived.table.asInstanceOf[Changelog]
    assert(rederivedChangelog.baseTable() eq changelog.baseTable())
    assert(rederivedChangelog.context() == changelog.context())
    assert(rederived.changelogResolved == original.changelogResolved)
    assert(rederived.sameResult(original))
    assert(rederived.semanticHash() == original.semanticHash())
  }

  // ===========================================================================
  // Streaming post-processing
  // ===========================================================================
  //
  // Row-level passes (carry-over removal and update detection) rewrite the streaming plan
  // into Aggregate -> [Filter] -> Generate(Inline) -> [Project] under an
  // EventTimeWatermark on `_commit_timestamp`. Net-change computation is still rejected
  // since it requires reasoning over the entire requested range.

  /** Re-creates the test table with non-nullable columns suitable as rowId / rowVersion. */
  private def recreatePostProcessingTable(): Identifier = {
    val cat = spark.sessionState.catalogManager.catalog(cdcCatalogName).asTableCatalog
    val ident = Identifier.of(Array.empty, "test_table")
    if (cat.tableExists(ident)) cat.dropTable(ident)
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType, false),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    ident
  }

  private def assertStreamingRowLevelRewrite(plan: LogicalPlan): Unit = {
    import org.apache.spark.sql.catalyst.plans.logical.{
      Aggregate, EventTimeWatermark, Generate}
    val watermarks = plan.collect { case w: EventTimeWatermark => w }
    assert(watermarks.nonEmpty,
      s"Expected EventTimeWatermark in streaming row-level rewrite. Plan:\n$plan")
    assert(watermarks.head.eventTime.name == "_commit_timestamp",
      s"Watermark must be on `_commit_timestamp`. Plan:\n$plan")
    val aggs = plan.collect { case a: Aggregate => a }
    assert(aggs.nonEmpty,
      s"Expected Aggregate in streaming row-level rewrite. Plan:\n$plan")
    val gens = plan.collect { case g: Generate => g }
    assert(gens.nonEmpty,
      s"Expected Generate(Inline) in streaming row-level rewrite. Plan:\n$plan")
  }

  test("DataStreamReader - changes() with carry-over capability rewrites plan") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    assertStreamingRowLevelRewrite(analyzed)
  }

  test("DataStreamReader - changes() with computeUpdates rewrites plan") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .option("computeUpdates", "true")
      .option("deduplicationMode", "none")
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    assertStreamingRowLevelRewrite(analyzed)
  }

  test("DataStreamReader - changes() with deduplicationMode=netChanges rewrites plan") {
    import org.apache.spark.sql.catalyst.plans.logical.TransformWithState
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .option("deduplicationMode", "netChanges")
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    val tws = analyzed.collect { case t: TransformWithState => t }
    assert(tws.size == 1,
      s"Expected exactly one TransformWithState; found ${tws.size}. Plan:\n$analyzed")
  }

  // ===========================================================================
  // Generic changelog schema validation
  // ===========================================================================

  private def validateChangelog(changelog: Changelog): Changelog = {
    ChangelogUtil.validateSchema(changelog)
    changelog
  }

  private def cl(name: String, cols: (String, org.apache.spark.sql.types.DataType)*)
      : TestChangelog = {
    new TestChangelog(name, cols.map { case (n, t) => Column.create(n, t) }.toArray)
  }

  private def missing(columnName: String): Map[String, String] =
    Map("changelogName" -> "bad_cl", "columnName" -> columnName)

  private def wrongType(columnName: String, expected: String, actual: String)
      : Map[String, String] = Map(
    "changelogName" -> "bad_cl",
    "columnName" -> columnName,
    "expectedType" -> expected,
    "actualType" -> actual)

  // Valid metadata tuples; tests swap one of these out to create broken schemas.
  private val validChangeType = "_change_type" -> StringType
  private val validVersion = "_commit_version" -> LongType
  private val validTimestamp = "_commit_timestamp" -> TimestampType

  test("changelog relations preserve the connector read capabilities") {
    val changelog = validateChangelog(
      cl("batch_cl", validChangeType, validVersion, validTimestamp))
    val relation = DataSourceV2Relation.create(changelog, None, None)
    assert(relation.table eq changelog)
    assert(relation.table.capabilities() == Collections.singleton(TableCapability.BATCH_READ))
  }

  test("relation copies preserve changelog context and connector metadata columns") {
    val changelog = new TestChangelog(
      "metadata_cl",
      Array(
        Column.create("id", LongType),
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType))) with SupportsMetadataColumns {
      override def metadataColumns(): Array[MetadataColumn] = Array(new MetadataColumn {
        override def name(): String = "_source"
        override def dataType(): StringType = StringType
      })
    }
    val options = CaseInsensitiveStringMap.empty()
    val batch = DataSourceV2Relation.create(changelog, Some(cdcCatalog), Some(ident), options)
      .copy(changelogResolved = true)
    val batchWithMetadata = batch.withMetadataColumns()
    val batchInstance = batchWithMetadata.copy().newInstance()
    Seq(batch, batchWithMetadata, batchInstance).foreach { relation =>
      assert(relation.table eq changelog)
      assert(relation.changelogResolved)
    }
    assert(batchWithMetadata.output.last.name == "_source")
    assert(batchInstance.output.map(_.exprId).toSet
      .intersect(batchWithMetadata.output.map(_.exprId).toSet).isEmpty)
    assert(batchInstance.sameResult(batchWithMetadata))

    val stream = StreamingRelationV2(
      None, changelog.name(), changelog, options, batch.output,
      Some(cdcCatalog), Some(ident), None, changelogResolved = true)
    val streamWithMetadata = stream.withMetadataColumns()
    val streamInstance = streamWithMetadata.copy().newInstance()
      .asInstanceOf[StreamingRelationV2]
    Seq(stream, streamWithMetadata, streamInstance).foreach { relation =>
      assert(relation.table eq changelog)
      assert(relation.changelogResolved)
    }
    assert(streamWithMetadata.output.last.name == "_source")
    assert(streamInstance.output.map(_.exprId).toSet
      .intersect(streamWithMetadata.output.map(_.exprId).toSet).isEmpty)
    assert(streamInstance.sameResult(streamWithMetadata))
  }

  gridTest("changelog refresh rejects changed row identity metadata")(
      Seq("rowId", "rowVersion")) { changedField =>
    def changelog(rowId: String, rowVersion: String): Changelog = new TestChangelog(
      "refresh_cl",
      Array(
        Column.create("id", LongType),
        Column.create("other_id", LongType),
        Column.create("row_version", LongType),
        Column.create("other_version", LongType),
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdRefs = Array(FieldReference.column(rowId)),
      rowVersionRef = Some(FieldReference.column(rowVersion)))

    val captured = changelog("id", "row_version")
    val current = if (changedField == "rowId") {
      changelog("other_id", "row_version")
    } else {
      changelog("id", "other_version")
    }
    checkError(
      intercept[AnalysisException] { ChangelogUtil.validateRefresh(captured, current) },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.CHANGELOG_METADATA_MISMATCH",
      parameters = Map("tableName" -> "`refresh_cl`", "errors" -> s"- $changedField changed"))
  }

  test("changelog refresh does not require unused row identity methods") {
    def changelog(): Changelog = new TestChangelog(
      "plain_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      rowIdSupported = false)
    ChangelogUtil.validateRefresh(changelog(), changelog())
  }

  test("Changelog schema - missing _change_type column throws") {
    checkError(
      intercept[AnalysisException] {
        validateChangelog(cl("bad_cl", validVersion, validTimestamp))
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_change_type"))
  }

  test("Changelog schema - missing _commit_version column throws") {
    checkError(
      intercept[AnalysisException] {
        validateChangelog(cl("bad_cl", validChangeType, validTimestamp))
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_commit_version"))
  }

  test("Changelog schema - missing _commit_timestamp column throws") {
    checkError(
      intercept[AnalysisException] {
        validateChangelog(cl("bad_cl", validChangeType, validVersion))
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_commit_timestamp"))
  }

  test("Changelog schema - wrong _change_type data type throws") {
    checkError(
      intercept[AnalysisException] {
        validateChangelog(
          cl("bad_cl", "_change_type" -> IntegerType, validVersion, validTimestamp))
      },
      condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
      parameters = wrongType("_change_type", "STRING", "INT"))
  }

  test("Changelog schema - wrong _commit_timestamp data type throws") {
    checkError(
      intercept[AnalysisException] {
        validateChangelog(
          cl("bad_cl", validChangeType, validVersion, "_commit_timestamp" -> LongType))
      },
      condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
      parameters = wrongType("_commit_timestamp", "TIMESTAMP", "BIGINT"))
  }

  test("Changelog schema - _commit_version accepts LongType and StringType") {
    Seq(LongType, StringType).foreach { versionType =>
      validateChangelog(
        cl("any_cl", validChangeType, "_commit_version" -> versionType, validTimestamp))
    }
  }

  test("Changelog schema - _commit_version rejects all other data types") {
    val structVersion = StructType(Seq(StructField("v", LongType)))
    Seq[(org.apache.spark.sql.types.DataType, String)](
      // Other atomic types previously allowed under the AtomicType contract.
      IntegerType -> "INT",
      TimestampType -> "TIMESTAMP",
      // Complex types (always rejected).
      ArrayType(LongType) -> "ARRAY<BIGINT>",
      MapType(StringType, LongType) -> "MAP<STRING, BIGINT>",
      structVersion -> structVersion.sql).foreach { case (versionType, sql) =>
      checkError(
        intercept[AnalysisException] {
          validateChangelog(
            cl("bad_cl", validChangeType, "_commit_version" -> versionType, validTimestamp))
        },
        condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
        parameters = wrongType("_commit_version", "BIGINT or STRING", sql))
    }
  }

  test("Changelog schema - valid schema with data columns passes") {
    validateChangelog(
      cl("good_cl", "id" -> LongType, "name" -> StringType,
        validChangeType, validVersion, validTimestamp))
  }

  test("Changelog schema - nested rowId and rowVersion references pass (Delta-style _metadata)") {
    val metadataRowId = FieldReference(Seq("_metadata", "row_id"))
    val metadataRowVersion = FieldReference(Seq("_metadata", "row_commit_version"))
    val cl = new TestChangelog(
      "delta_cl",
      Array(
        Column.create("id", LongType, false),
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdRefs = Array(metadataRowId),
      rowVersionRef = Some(metadataRowVersion))
    validateChangelog(cl)
  }

  test("Changelog schema - representsUpdateAsDeleteAndInsert=true requires non-empty rowId") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      updateAsDeleteInsert = true,
      rowIdRefs = Array.empty,
      rowVersionRef = Some(FieldReference.column("_commit_version")))
    checkError(
      intercept[AnalysisException] { validateChangelog(cl) },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_ROW_ID",
      parameters = Map("changelogName" -> "bad_cl"))
  }

  test("Changelog schema - containsIntermediateChanges=true requires non-empty rowId") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      intermediateChanges = true,
      rowIdRefs = Array.empty)
    checkError(
      intercept[AnalysisException] { validateChangelog(cl) },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_ROW_ID",
      parameters = Map("changelogName" -> "bad_cl"))
  }

  test("Changelog schema - UnsupportedOperationException surfaces when rowId() not implemented") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdSupported = false,
      rowVersionRef = Some(FieldReference.column("_commit_version")))
    intercept[UnsupportedOperationException] { validateChangelog(cl) }
  }

  test("Changelog schema - UnsupportedOperationException surfaces when rowVersion() missing") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdRefs = Array(FieldReference.column("id")),
      rowVersionRef = None)
    intercept[UnsupportedOperationException] { validateChangelog(cl) }
  }

}

/** Verifies that Spark derives changelogs from loaded tables without calling the legacy API. */
class LegacyChangelogOverrideCatalog extends InMemoryChangelogCatalog {
  override def loadChangelog(
      ident: Identifier,
      context: ChangelogContext,
      options: CaseInsensitiveStringMap): Changelog = {
    throw new IllegalStateException("Spark must derive changelogs from the shared base table")
  }
}

/**
 * Test-only [[Changelog]] implementation that returns a hand-crafted schema. Used to
 * exercise [[ChangelogUtil]]'s schema validation without going through a real catalog.
 *
 * Defaults match a minimal connector with no post-processing capabilities. Tests opt
 * into capability flags or `rowVersion()` overrides via constructor params.
 */
private class TestChangelog(
    nameArg: String,
    cols: Array[Column],
    carryoverRows: Boolean = false,
    updateAsDeleteInsert: Boolean = false,
    intermediateChanges: Boolean = false,
    rowIdRefs: Array[NamedReference] = Array.empty,
    rowIdSupported: Boolean = true,
    rowVersionRef: Option[NamedReference] = None) extends Changelog {
  private val sourceTable = new Table {
    override def name(): String = nameArg
    override def columns(): Array[Column] = cols
    override def capabilities(): java.util.Set[TableCapability] = java.util.Set.of()
  }
  private val changelogContext = new ChangelogContext(
    new ChangelogRange.VersionRange("1", java.util.Optional.of("2"), true, true),
    ChangelogContext.DeduplicationMode.DROP_CARRYOVERS,
    false)

  override def name(): String = nameArg
  override def baseTable(): Table = sourceTable
  override def context(): ChangelogContext = changelogContext
  override def columns(): Array[Column] = cols
  override def capabilities(): java.util.Set[TableCapability] =
    Collections.singleton(TableCapability.BATCH_READ)
  override def containsCarryoverRows(): Boolean = carryoverRows
  override def containsIntermediateChanges(): Boolean = intermediateChanges
  override def representsUpdateAsDeleteAndInsert(): Boolean = updateAsDeleteInsert
  override def rowId(): Array[NamedReference] =
    if (rowIdSupported) rowIdRefs else super.rowId()
  override def rowVersion(): NamedReference =
    rowVersionRef.getOrElse(super.rowVersion())
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw new UnsupportedOperationException("not needed for schema validation tests")
  }
}
