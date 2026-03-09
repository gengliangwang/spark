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

package org.apache.spark.sql.connector.catalog;

import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Tests for the CDC (Change Data Capture) DSv2 API types:
 * {@link ChangelogRange}, {@link ChangelogInfo}, {@link Changelog}, and {@link ChangelogTable}.
 */
public class ChangelogAPISuite {

  // ========================== ChangelogRange Tests ==========================

  @Test
  public void testVersionRangeWithBothBounds() {
    ChangelogRange.VersionRange range = new ChangelogRange.VersionRange(
        "10", Optional.of("20"), true, true);
    assertEquals("10", range.startingVersion());
    assertEquals(Optional.of("20"), range.endingVersion());
    assertTrue(range.startingBoundInclusive());
    assertTrue(range.endingBoundInclusive());
  }

  @Test
  public void testVersionRangeWithoutEndingVersion() {
    ChangelogRange.VersionRange range = new ChangelogRange.VersionRange(
        "5", Optional.empty(), true, true);
    assertEquals("5", range.startingVersion());
    assertTrue(range.endingVersion().isEmpty());
  }

  @Test
  public void testVersionRangeExclusiveBounds() {
    ChangelogRange.VersionRange range = new ChangelogRange.VersionRange(
        "10", Optional.of("20"), false, false);
    assertFalse(range.startingBoundInclusive());
    assertFalse(range.endingBoundInclusive());
  }

  @Test
  public void testVersionRangeWithStringIdentifiers() {
    ChangelogRange.VersionRange range = new ChangelogRange.VersionRange(
        "5765898212649545898", Optional.of("8439568982649545102"), true, true);
    assertEquals("5765898212649545898", range.startingVersion());
    assertEquals("8439568982649545102", range.endingVersion().get());
  }

  @Test
  public void testTimestampRangeWithBothBounds() {
    ChangelogRange.TimestampRange range = new ChangelogRange.TimestampRange(
        1000000L, Optional.of(2000000L), true, true);
    assertEquals(1000000L, range.startingTimestamp());
    assertEquals(Optional.of(2000000L), range.endingTimestamp());
    assertTrue(range.startingBoundInclusive());
    assertTrue(range.endingBoundInclusive());
  }

  @Test
  public void testTimestampRangeWithoutEndingTimestamp() {
    ChangelogRange.TimestampRange range = new ChangelogRange.TimestampRange(
        1000000L, Optional.empty(), true, true);
    assertTrue(range.endingTimestamp().isEmpty());
  }

  @Test
  public void testTimestampRangeExclusiveBounds() {
    ChangelogRange.TimestampRange range = new ChangelogRange.TimestampRange(
        1000000L, Optional.of(2000000L), false, true);
    assertFalse(range.startingBoundInclusive());
    assertTrue(range.endingBoundInclusive());
  }

  @Test
  public void testUnboundedRange() {
    ChangelogRange.Unbounded unbounded = new ChangelogRange.Unbounded();
    assertTrue(unbounded.startingBoundInclusive());
    assertTrue(unbounded.endingBoundInclusive());
  }

  @Test
  public void testChangelogRangePolymorphism() {
    ChangelogRange version = new ChangelogRange.VersionRange(
        "1", Optional.of("10"), true, true);
    ChangelogRange timestamp = new ChangelogRange.TimestampRange(
        100L, Optional.of(200L), true, true);
    ChangelogRange unbounded = new ChangelogRange.Unbounded();

    assertInstanceOf(ChangelogRange.VersionRange.class, version);
    assertInstanceOf(ChangelogRange.TimestampRange.class, timestamp);
    assertInstanceOf(ChangelogRange.Unbounded.class, unbounded);
  }

  @Test
  public void testVersionRangeRecordEquality() {
    ChangelogRange.VersionRange a = new ChangelogRange.VersionRange(
        "10", Optional.of("20"), true, true);
    ChangelogRange.VersionRange b = new ChangelogRange.VersionRange(
        "10", Optional.of("20"), true, true);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testTimestampRangeRecordEquality() {
    ChangelogRange.TimestampRange a = new ChangelogRange.TimestampRange(
        100L, Optional.of(200L), true, false);
    ChangelogRange.TimestampRange b = new ChangelogRange.TimestampRange(
        100L, Optional.of(200L), true, false);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  // ========================== ChangelogInfo Tests ==========================

  @Test
  public void testChangelogInfoConstructor() {
    ChangelogRange range = new ChangelogRange.VersionRange(
        "1", Optional.of("10"), true, true);
    ChangelogInfo info = new ChangelogInfo(
        range, ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS, false);

    assertEquals(range, info.range());
    assertEquals(ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS, info.deduplicationMode());
    assertFalse(info.computeUpdates());
  }

  @Test
  public void testChangelogInfoWithComputeUpdates() {
    ChangelogRange range = new ChangelogRange.VersionRange(
        "1", Optional.of("10"), true, true);
    ChangelogInfo info = new ChangelogInfo(
        range, ChangelogInfo.DeduplicationMode.NONE, true);

    assertTrue(info.computeUpdates());
    assertEquals(ChangelogInfo.DeduplicationMode.NONE, info.deduplicationMode());
  }

  @Test
  public void testChangelogInfoOfWithDefaults() {
    ChangelogRange range = new ChangelogRange.TimestampRange(
        100L, Optional.of(200L), true, true);
    ChangelogInfo info = ChangelogInfo.of(range);

    assertEquals(range, info.range());
    assertEquals(ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS, info.deduplicationMode());
    assertFalse(info.computeUpdates());
  }

  @Test
  public void testChangelogInfoOfWithDeduplicationMode() {
    ChangelogRange range = new ChangelogRange.Unbounded();
    ChangelogInfo info = ChangelogInfo.of(range, ChangelogInfo.DeduplicationMode.NET_CHANGES);

    assertEquals(ChangelogInfo.DeduplicationMode.NET_CHANGES, info.deduplicationMode());
    assertFalse(info.computeUpdates());
  }

  @Test
  public void testDeduplicationModeValues() {
    ChangelogInfo.DeduplicationMode[] modes = ChangelogInfo.DeduplicationMode.values();
    assertEquals(3, modes.length);
    assertEquals(ChangelogInfo.DeduplicationMode.NONE, modes[0]);
    assertEquals(ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS, modes[1]);
    assertEquals(ChangelogInfo.DeduplicationMode.NET_CHANGES, modes[2]);
  }

  // ========================== Changelog Interface Tests ==========================

  @Test
  public void testChangelogDefaultRowId() {
    TestChangelog changelog = new TestChangelog("test");
    NamedReference[] rowId = changelog.rowId();
    assertNotNull(rowId);
    assertEquals(0, rowId.length);
  }

  @Test
  public void testChangelogDefaultRowVersion() {
    TestChangelog changelog = new TestChangelog("test");
    assertNull(changelog.rowVersion());
  }

  @Test
  public void testChangelogProperties() {
    TestChangelog changelog = new TestChangelog("my_table$changelog");
    assertEquals("my_table$changelog", changelog.name());

    Column[] columns = changelog.columns();
    assertEquals(5, columns.length);

    assertTrue(changelog.containsCarryoverRows());
    assertFalse(changelog.containsIntermediateChanges());
    assertTrue(changelog.representsUpdateAsDeleteAndInsert());
  }

  // ========================== ChangelogTable Tests ==========================

  @Test
  public void testChangelogTableWrapsChangelog() {
    TestChangelog changelog = new TestChangelog("test_table$changelog");
    ChangelogInfo info = ChangelogInfo.of(
        new ChangelogRange.VersionRange("1", Optional.of("10"), true, true));

    ChangelogTable table = new ChangelogTable(changelog, info);

    assertSame(changelog, table.changelog());
    assertSame(info, table.changelogInfo());
  }

  @Test
  public void testChangelogTableName() {
    TestChangelog changelog = new TestChangelog("my_table$changelog");
    ChangelogInfo info = ChangelogInfo.of(new ChangelogRange.Unbounded());

    ChangelogTable table = new ChangelogTable(changelog, info);
    assertEquals("my_table$changelog", table.name());
  }

  @Test
  public void testChangelogTableColumns() {
    TestChangelog changelog = new TestChangelog("test");
    ChangelogInfo info = ChangelogInfo.of(new ChangelogRange.Unbounded());

    ChangelogTable table = new ChangelogTable(changelog, info);

    Column[] columns = table.columns();
    assertEquals(5, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals("name", columns[1].name());
    assertEquals("_change_type", columns[2].name());
    assertEquals("_commit_version", columns[3].name());
    assertEquals("_commit_timestamp", columns[4].name());
  }

  @Test
  public void testChangelogTableCapabilities() {
    TestChangelog changelog = new TestChangelog("test");
    ChangelogInfo info = ChangelogInfo.of(new ChangelogRange.Unbounded());

    ChangelogTable table = new ChangelogTable(changelog, info);

    Set<TableCapability> capabilities = table.capabilities();
    assertTrue(capabilities.contains(TableCapability.BATCH_READ));
    assertTrue(capabilities.contains(TableCapability.MICRO_BATCH_READ));
    assertEquals(2, capabilities.size());
  }

  @Test
  public void testChangelogTableDelegatesScanBuilder() {
    TestChangelog changelog = new TestChangelog("test");
    ChangelogInfo info = ChangelogInfo.of(new ChangelogRange.Unbounded());

    ChangelogTable table = new ChangelogTable(changelog, info);

    ScanBuilder scanBuilder = table.newScanBuilder(CaseInsensitiveStringMap.empty());
    assertNotNull(scanBuilder);
    assertInstanceOf(TestChangelog.TestScanBuilder.class, scanBuilder);
  }

  // ========================== TableCatalog Extension Tests ==========================

  @Test
  public void testTableCatalogCapabilityHasSupportChangelog() {
    TableCatalogCapability cap = TableCatalogCapability.SUPPORT_CHANGELOG;
    assertNotNull(cap);
    assertEquals("SUPPORT_CHANGELOG", cap.name());
  }

  @Test
  public void testTableCatalogLoadChangelogDefaultThrows() {
    TableCatalog catalog = new MinimalTestCatalog();
    assertThrows(UnsupportedOperationException.class,
        () -> catalog.loadChangelog(
            Identifier.of(new String[]{"ns"}, "t"),
            ChangelogInfo.of(new ChangelogRange.Unbounded())));
  }

  // ========================== Test Helpers ==========================

  /**
   * A minimal Changelog implementation for testing.
   */
  static class TestChangelog implements Changelog {
    private final String tableName;

    TestChangelog(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public String name() { return tableName; }

    @Override
    public Column[] columns() {
      return new Column[] {
          Column.create("id", DataTypes.IntegerType),
          Column.create("name", DataTypes.StringType),
          Column.create("_change_type", DataTypes.StringType),
          Column.create("_commit_version", DataTypes.LongType),
          Column.create("_commit_timestamp", DataTypes.TimestampType)
      };
    }

    @Override
    public boolean containsCarryoverRows() { return true; }

    @Override
    public boolean containsIntermediateChanges() { return false; }

    @Override
    public boolean representsUpdateAsDeleteAndInsert() { return true; }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return new TestScanBuilder();
    }

    static class TestScanBuilder implements ScanBuilder {
      @Override
      public Scan build() {
        throw new UnsupportedOperationException("Test scan builder");
      }
    }
  }

  /**
   * A minimal TableCatalog that does not override loadChangelog,
   * used to test that the default throws UnsupportedOperationException.
   */
  static class MinimalTestCatalog implements TableCatalog {
    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {}

    @Override
    public String name() { return "minimal_test"; }

    @Override
    public Identifier[] listTables(String[] namespace) { return new Identifier[0]; }

    @Override
    public Table loadTable(Identifier ident) { return null; }

    @Override
    public Table createTable(Identifier ident, Column[] columns, org.apache.spark.sql.connector.expressions.Transform[] partitions, java.util.Map<String, String> properties) { return null; }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) { return null; }

    @Override
    public boolean dropTable(Identifier ident) { return false; }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {}
  }
}
