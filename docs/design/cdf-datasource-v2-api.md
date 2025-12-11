# Change Data Feed (CDF) Data Source V2 API

## Status

**State**: Proposal  
**Authors**: Gengliang Wang  
**Created**: December 2024  
**Target Version**: Apache Spark 4.2.0

---

## Table of Contents

1. [Overview](#overview)
2. [Motivation](#motivation)
3. [Goals](#goals)
4. [Non-Goals](#non-goals)
5. [Design](#design)
   - [API Overview](#api-overview)
   - [Batch CDF Support](#batch-cdf-support)
   - [Streaming CDF Support](#streaming-cdf-support)
   - [Logical Plan Expansion](#logical-plan-expansion)
   - [Physical Execution](#physical-execution)
6. [API Specification](#api-specification)
7. [Implementation Details](#implementation-details)
8. [SQL Syntax and DataFrame API](#sql-syntax-and-dataframe-api)
9. [Usage Examples](#usage-examples)
10. [Compatibility and Migration](#compatibility-and-migration)
11. [Future Work](#future-work)
12. [Appendix](#appendix)

---

## Overview

This proposal introduces a new Data Source V2 API for **Change Data Feed (CDF)** support in Apache Spark. CDF enables reading row-level changes (inserts, updates, deletes) from data sources that track modifications between versions or timestamps.

The design uses a **dual-batch join approach** where:
- **Added records batch**: Scans files containing newly added rows (inserts and post-update values)
- **Removed records batch**: Scans files containing removed rows (deletes and pre-update values)
- Spark performs a **full outer join** on row ID columns to determine change types

---

## Motivation

Many modern data lake formats (Delta Lake, Apache Iceberg, Apache Hudi) maintain transaction logs that track row-level changes. Users often need to:

1. **Audit and Compliance**: Track who changed what data and when
2. **Incremental Processing**: Process only changed data since the last run
3. **Data Synchronization**: Replicate changes to downstream systems
4. **Time-Travel Analysis**: Understand how data evolved over time

Currently, there's no standardized Data Source V2 API for reading these changes. Each format implements its own proprietary solution, leading to:

- Inconsistent APIs across formats
- Duplication of effort in Spark integration
- Limited optimization opportunities

This proposal provides a **unified, extensible API** that enables any data source to expose change data feeds through a consistent interface.

---

## Goals

1. **Standardized API**: Define a mixin interface for `Scan` that data sources can implement to support CDF reads
2. **Batch and Streaming Support**: Support both batch queries and structured streaming
3. **Change Type Detection**: Automatically determine INSERT, UPDATE, and DELETE operations through join semantics
4. **Metadata Exposure**: Provide commit version and timestamp metadata for each change
5. **Optimizer Integration**: Allow Spark's optimizer to push down filters and projections to CDF scans
6. **Extensibility**: Design for future enhancements without breaking changes

---

## Non-Goals

1. **Write-side CDF**: This proposal focuses only on reading CDF; writing CDC data is out of scope
2. **Cross-table CDC**: Joining changes across multiple tables is not addressed
3. **Schema Evolution in CDF**: Handling schema changes within a CDF range is left to data sources
4. **Exactly-once Semantics**: Transactional guarantees are the responsibility of data sources

---

## Design

### API Overview

The CDF API introduces the following components:

```
┌─────────────────────────────────────────────────────────────────┐
│                    SupportsChangeDataFeed                       │
│                    (Mixin Interface for Scan)                   │
├─────────────────────────────────────────────────────────────────┤
│ Batch Methods:                                                  │
│   • additionChangesBatch()                                      │
│   • deletionChangesBatch()                                      │
│                                                                 │
│ Streaming Methods:                                              │
│   • additionChangesMicroBatchStream(checkpointLocation)         │
│   • deletionChangesMicroBatchStream(checkpointLocation)         │
│                                                                 │
│ Common Methods:                                                 │
│   • rowIdColumns()                                              │
│   • cdfReadSchema()                                             │
└─────────────────────────────────────────────────────────────────┘
```

### Batch CDF Support

For batch CDF reads, the flow is:

```
                        User Query
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│           DataSourceV2Relation with CDFInfo                     │
│           (startVersion, endVersion, rowIdColumns)              │
└─────────────────────────────────────────────────────────────────┘
                            │
                 ExpandCDFRelation (Analyzer)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Full Outer Join                            │
│  ┌─────────────────────┐    ┌─────────────────────┐            │
│  │ DataSourceV2Relation │    │ DataSourceV2Relation │           │
│  │ cdfInfo.batchType =  │    │ cdfInfo.batchType =  │           │
│  │   CDFAddedBatch      │    │   CDFRemovedBatch    │           │
│  └─────────────────────┘    └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
                            │
                 V2ScanRelationPushDown (Optimizer)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ┌─────────────────────┐    ┌─────────────────────┐            │
│  │DataSourceV2ScanRel  │    │DataSourceV2ScanRel  │            │
│  │→ additionChangesBatch│    │→ deletionChangesBatch│           │
│  └─────────────────────┘    └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
                            │
                 DataSourceV2Strategy (Planner)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ┌─────────────────────┐    ┌─────────────────────┐            │
│  │   BatchScanExec     │    │   BatchScanExec     │            │
│  │  [CDF Added Batch]  │    │ [CDF Removed Batch] │            │
│  └─────────────────────┘    └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

### Streaming CDF Support

For streaming CDF reads in micro-batch mode:

```
┌─────────────────────────────────────────────────────────────────┐
│        StreamingDataSourceV2Relation with CDFInfo               │
│        (batchType = CDFAddedBatch or CDFRemovedBatch)           │
└─────────────────────────────────────────────────────────────────┘
                            │
                 MicroBatchExecution
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  │                  ▼
   CDFAddedBatch            │          CDFRemovedBatch
         │                  │                  │
         ▼                  ▼                  ▼
additionChangesMicroBatchStream    deletionChangesMicroBatchStream
         │                  │                  │
         └──────────────────┼──────────────────┘
                            │
                            ▼
                   MicroBatchScanExec
```

### Logical Plan Expansion

The `ExpandCDFRelation` analyzer rule transforms a single CDF relation into a join plan:

**Input:**
```
DataSourceV2Relation(
  table = myTable,
  cdfInfo = CDFInfo(startVersion=1, endVersion=5, rowIdColumns=["id"])
)
```

**Output:**
```
Project [
  coalesce(added.*, removed.*) as data_columns,
  CASE 
    WHEN added.id IS NOT NULL AND removed.id IS NULL THEN 'insert'
    WHEN added.id IS NULL AND removed.id IS NOT NULL THEN 'delete'
    ELSE 'update_postimage'
  END as _change_type,
  coalesce(added._commit_version, removed._commit_version) as _commit_version,
  coalesce(added._commit_timestamp, removed._commit_timestamp) as _commit_timestamp
]
└── FullOuterJoin (added.id <=> removed.id)
    ├── DataSourceV2Relation(cdfInfo.batchType = CDFAddedBatch) as added
    └── DataSourceV2Relation(cdfInfo.batchType = CDFRemovedBatch) as removed
```

### Physical Execution

`BatchScanExec` and `MicroBatchScanExec` check `cdfInfo.batchType` to determine which method to invoke:

```scala
val batch = cdfInfo.flatMap(_.batchType) match {
  case Some(CDFAddedBatch) =>
    scan.asInstanceOf[SupportsChangeDataFeed].additionChangesBatch()
  case Some(CDFRemovedBatch) =>
    scan.asInstanceOf[SupportsChangeDataFeed].deletionChangesBatch()
  case None =>
    scan.toBatch()  // Normal read path
}
```

---

## API Specification

### SupportsChangeDataFeed Interface

```java
/**
 * A mix-in interface for Scan that enables Change Data Feed reads.
 */
@Evolving
public interface SupportsChangeDataFeed extends Scan {

  // ==================== Batch Methods ====================
  
  /**
   * Returns a Batch for scanning addition changes.
   * Includes inserted rows and updated rows (post-update values).
   */
  Batch additionChangesBatch();

  /**
   * Returns a Batch for scanning deletion changes.
   * Includes deleted rows and updated rows (pre-update values).
   */
  Batch deletionChangesBatch();

  // ==================== Streaming Methods ====================
  
  /**
   * Returns a MicroBatchStream for streaming addition changes.
   */
  default MicroBatchStream additionChangesMicroBatchStream(String checkpointLocation);

  /**
   * Returns a MicroBatchStream for streaming deletion changes.
   */
  default MicroBatchStream deletionChangesMicroBatchStream(String checkpointLocation);

  // ==================== Common Methods ====================
  
  /**
   * Returns row identifier columns for joining addition and deletion batches.
   */
  NamedReference[] rowIdColumns();

  /**
   * Returns the schema for CDF output including metadata columns.
   */
  default StructType cdfReadSchema();
}
```

### CDFInfo Case Class

```scala
/**
 * Contains information for Change Data Feed reads.
 */
case class CDFInfo(
    startVersion: Option[Long] = None,
    endVersion: Option[Long] = None,
    startTimestamp: Option[Long] = None,
    endTimestamp: Option[Long] = None,
    rowIdColumns: Seq[String] = Seq.empty,
    batchType: Option[CDFBatchType] = None
)

sealed trait CDFBatchType
case object CDFAddedBatch extends CDFBatchType
case object CDFRemovedBatch extends CDFBatchType
```

### Table Capabilities

```java
public enum TableCapability {
  // ... existing capabilities ...
  
  /** Signals batch CDF read support */
  CDF_READ,
  
  /** Signals micro-batch streaming CDF read support */
  CDF_MICRO_BATCH_READ
}
```

### CDF Metadata Columns

| Column Name | Type | Description |
|-------------|------|-------------|
| `_change_type` | STRING | Type of change: `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | LONG | The commit/version number when the change occurred |
| `_commit_timestamp` | TIMESTAMP | The timestamp when the change was committed |

---

## Implementation Details

### New Files

| File | Description |
|------|-------------|
| `SupportsChangeDataFeed.java` | Mixin interface for Scan with CDF methods |
| `CDFInfo.scala` | Case class for CDF read configuration |
| `ExpandCDFRelation.scala` | Analyzer rule to expand CDF relations into joins |

### Modified Files

| File | Changes |
|------|---------|
| `TableCapability.java` | Added `CDF_READ`, `CDF_MICRO_BATCH_READ` |
| `DataSourceV2Relation.scala` | Added `cdfInfo` field to `DataSourceV2Relation` and `StreamingDataSourceV2Relation` |
| `BatchScanExec.scala` | Added `cdfInfo` field, logic to call appropriate batch method |
| `MicroBatchScanExec.scala` | Added `cdfInfo` field for streaming CDF |
| `DataSourceV2Strategy.scala` | Pass `cdfInfo` to exec nodes |
| `MicroBatchExecution.scala` | Select correct stream method based on `cdfInfo.batchType` |
| `BaseSessionStateBuilder.scala` | Register `ExpandCDFRelation` rule |

---

## SQL Syntax and DataFrame API

This section defines the SQL syntax and DataFrame API for querying Change Data Feed.

### SQL Syntax

The SQL syntax is inspired by the **ANSI SQL:2011 temporal table** syntax `FOR SYSTEM_TIME`, which provides a standardized way to query historical data. The SQL:2011 standard defines the following temporal query patterns:

```sql
-- ANSI SQL:2011 Temporal Table Syntax
SELECT * FROM table_name FOR SYSTEM_TIME AS OF '2025-01-01 00:00:00'
SELECT * FROM table_name FOR SYSTEM_TIME BETWEEN '2025-01-01' AND '2025-01-15'
SELECT * FROM table_name FOR SYSTEM_TIME FROM '2025-01-01' TO '2025-01-15'
```

While `FOR SYSTEM_TIME` is designed for time-travel queries (point-in-time snapshots), our `FOR CHANGES` clause extends this pattern to support Change Data Feed queries that return row-level change events instead of snapshots.

#### Grammar

**Primary Syntax:**

```sql
SELECT <columns>
FROM <table_name>
  FOR CHANGES FROM {VERSION <n> | TIMESTAMP '<ts>'} TO {VERSION <n> | TIMESTAMP '<ts>' | CURRENT | LATEST}
  OPTIONS (netChanges = {true | false}, computeUpdates = {true | false})
  IDENTIFY BY (<col1> [, <col2>, ...])
```

**Alternative Syntax (shorthand):**

```sql
-- Version-based changes
SELECT <columns>
FROM <table_name>
  FOR VERSION FROM <start_version> TO {<end_version> | LATEST}
  OPTIONS (netChanges = {true | false}, computeUpdates = {true | false})
  IDENTIFY BY (<col1> [, <col2>, ...])

-- Timestamp-based changes
SELECT <columns>
FROM <table_name>
  FOR TIMESTAMP FROM '<start_ts>' TO {'<end_ts>' | CURRENT}
  OPTIONS (netChanges = {true | false}, computeUpdates = {true | false})
  IDENTIFY BY (<col1> [, <col2>, ...])
```

The alternative syntax provides a more concise form when using a single range type (version or timestamp).

#### Clause Descriptions

| Clause | Required | Description |
|--------|----------|-------------|
| `FOR CHANGES` | Yes | Indicates a Change Data Feed query |
| `FROM VERSION <n>` | Yes (or `FROM TIMESTAMP`) | Starting version number (inclusive) |
| `FROM TIMESTAMP '<ts>'` | Yes (or `FROM VERSION`) | Starting timestamp (inclusive) |
| `TO VERSION <n>` | No | Ending version number (inclusive) |
| `TO TIMESTAMP '<ts>'` | No | Ending timestamp (inclusive) |
| `TO CURRENT` / `TO LATEST` | No | Query changes up to the current/latest version |
| `OPTIONS (...)` | No | Additional options for CDF behavior |
| `IDENTIFY BY (...)` | No | Columns used to identify rows for change detection |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `netChanges` | boolean | `false` | When `true`, collapses intermediate changes to show only the net result for each row |
| `computeUpdates` | boolean | `false` | When `true`, computes UPDATE change types by joining added and removed records |

**Note**: CDF always includes INSERT changes. DELETE and UPDATE changes are computed based on the `computeUpdates` option.

#### SQL Examples

```sql
-- Basic: Read all changes between version 1 and 10
SELECT * FROM my_table
  FOR CHANGES FROM VERSION 1 TO VERSION 10
  IDENTIFY BY (id)

-- Read changes from a timestamp to the current version
SELECT * FROM my_table
  FOR CHANGES FROM TIMESTAMP '2025-01-01 00:00:00' TO CURRENT
  IDENTIFY BY (id)

-- Read changes with net changes only (collapse intermediate states)
SELECT * FROM my_table
  FOR CHANGES FROM VERSION 1 TO VERSION 10
  OPTIONS (netChanges = true)
  IDENTIFY BY (id)

-- Read changes with update detection enabled
SELECT * FROM my_table
  FOR CHANGES FROM VERSION 5 TO LATEST
  OPTIONS (computeUpdates = true)
  IDENTIFY BY (id, name)

-- Full example with all options
SELECT id, name, value, _change_type, _commit_version
FROM orders
  FOR CHANGES FROM TIMESTAMP '2025-01-01' TO TIMESTAMP '2025-01-15'
  OPTIONS (netChanges = true, computeUpdates = true)
  IDENTIFY BY (order_id)
WHERE _change_type IN ('insert', 'update_postimage')
```

### DataFrame API

#### Batch CDF Read

```scala
// Basic CDF read with version range
val changes = spark.read
  .format("delta")  // or any CDF-supporting format
  .option("readChangeFeed", "true")
  .option("startingVersion", 1)
  .option("endingVersion", 10)
  .option("identifyByColumns", "id")
  .table("my_table")

// CDF read with timestamp range
val changes = spark.read
  .format("iceberg")
  .option("readChangeFeed", "true")
  .option("startingTimestamp", "2025-01-01 00:00:00")
  .option("endingTimestamp", "2025-01-15 00:00:00")
  .option("identifyByColumns", "id,name")
  .table("my_table")

// CDF read with net changes and update computation
val changes = spark.read
  .format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 1)
  .option("netChanges", "true")
  .option("computeUpdates", "true")
  .option("identifyByColumns", "id")
  .table("my_table")
```

#### Streaming CDF Read

```scala
// Basic streaming CDF read
val streamingChanges = spark.readStream
  .format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 1)
  .option("identifyByColumns", "id")
  .option("computeUpdates", "true")
  .table("my_table")

// Write streaming changes to console
streamingChanges.writeStream
  .format("console")
  .outputMode("append")
  .start()
  .awaitTermination()

// Streaming CDF with timestamp start
val streamingChanges = spark.readStream
  .format("iceberg")
  .option("readChangeFeed", "true")
  .option("startingTimestamp", "2025-01-01 00:00:00")
  .option("identifyByColumns", "order_id")
  .table("orders")
```

#### DataFrame API Options

| Option | Type | Description |
|--------|------|-------------|
| `readChangeFeed` | String ("true"/"false") | Enable Change Data Feed read mode |
| `startingVersion` | Long | Starting version number (inclusive) |
| `endingVersion` | Long | Ending version number (inclusive, batch only) |
| `startingTimestamp` | String | Starting timestamp (inclusive) |
| `endingTimestamp` | String | Ending timestamp (inclusive, batch only) |
| `identifyByColumns` | String | Comma-separated list of columns for row identification |
| `netChanges` | String ("true"/"false") | Collapse intermediate changes to net result |
| `computeUpdates` | String ("true"/"false") | Compute UPDATE change types |

**Note**: Option names use camelCase to align with Delta Lake conventions and provide consistency between SQL and DataFrame APIs.

---

## Usage Examples

### Batch CDF Read

```sql
-- Read changes between versions 1 and 10
SELECT * FROM myTable
  FOR CHANGES FROM VERSION 1 TO VERSION 10
  IDENTIFY BY (id)

-- Read changes since a specific timestamp to latest
SELECT * FROM myTable
  FOR CHANGES FROM TIMESTAMP '2024-01-01 00:00:00' TO LATEST
  IDENTIFY BY (id)
```

### Expected Output Schema

```
root
 |-- id: long (nullable = false)          -- Original column
 |-- name: string (nullable = true)       -- Original column
 |-- value: double (nullable = true)      -- Original column
 |-- _change_type: string (nullable = false)
 |-- _commit_version: long (nullable = false)
 |-- _commit_timestamp: timestamp (nullable = false)
```

### Sample Output

| id | name | value | _change_type | _commit_version | _commit_timestamp |
|----|------|-------|--------------|-----------------|-------------------|
| 1 | Alice | 100.0 | insert | 2 | 2024-01-15 10:30:00 |
| 2 | Bob | 200.0 | update_postimage | 3 | 2024-01-15 11:00:00 |
| 3 | Carol | 150.0 | delete | 4 | 2024-01-15 11:30:00 |

---

## Compatibility and Migration

### Backward Compatibility

- **Fully backward compatible**: All new interfaces use default methods or optional parameters
- Existing data sources continue to work without modification
- New capabilities are opt-in via `TableCapability`

### Data Source Implementation

Data sources that want to support CDF must:

1. Add `CDF_READ` (and optionally `CDF_MICRO_BATCH_READ`) to `Table.capabilities()`
2. Implement `SupportsChangeDataFeed` in their `Scan` implementation
3. Provide implementations for:
   - `additionChangesBatch()` / `deletionChangesBatch()` for batch
   - `additionChangesMicroBatchStream()` / `deletionChangesMicroBatchStream()` for streaming
   - `rowIdColumns()` to specify join keys

---

## Future Work

1. **Pre-image/Post-image Columns**: Optionally expose both old and new values for updates in a single row
2. **Continuous Stream Support**: Extend to `ContinuousStream` for lower latency
3. **Filter Pushdown**: Push CDF-specific filters (version range, change type) to data sources
4. **Statistics**: Report CDF-specific statistics for better query planning
5. **Merge Support**: Optimize CDF reads for MERGE INTO operations

---

## Appendix

### Change Type Semantics

| Scenario | Added Batch | Removed Batch | Result _change_type |
|----------|-------------|---------------|---------------------|
| INSERT | Row present | Row absent | `insert` |
| DELETE | Row absent | Row present | `delete` |
| UPDATE | New value | Old value | `update_postimage` (for added) / `update_preimage` (for removed) |

### Join Semantics for Change Detection

```
Full Outer Join on row ID columns:
┌─────────────────┬─────────────────┬──────────────────┐
│ Added Batch     │ Removed Batch   │ Change Type      │
├─────────────────┼─────────────────┼──────────────────┤
│ EXISTS          │ NULL            │ INSERT           │
│ NULL            │ EXISTS          │ DELETE           │
│ EXISTS          │ EXISTS          │ UPDATE           │
└─────────────────┴─────────────────┴──────────────────┘
```

### Related Work

- **Delta Lake Change Data Feed**: Proprietary implementation in Delta Lake
- **Apache Iceberg Incremental Reads**: Scan planning with snapshot ranges  
- **Apache Hudi Incremental Queries**: Timeline-based incremental processing
- **Debezium**: CDC for databases (different architecture)

---

## References

- [SPARK-XXXXX] Change Data Feed Data Source V2 API (JIRA - TBD)
- [Data Source V2 API](https://spark.apache.org/docs/latest/sql-data-sources.html)

### Change Data Feed Implementations

- [Delta Lake Change Data Feed](https://delta-docs-incubator.netlify.app/delta-change-data-feed/) - Delta Lake's CDF implementation using `readChangeFeed` option
- [Apache Iceberg CDC](https://www.dremio.com/blog/cdc-with-apache-iceberg/) - Change Data Capture patterns with Apache Iceberg
- [Snowflake CHANGES Clause](https://docs.snowflake.com/en/sql-reference/constructs/changes) - Snowflake's SQL syntax for querying change data
- [Oracle Flashback Query](https://docs.oracle.com/cd/B12037_01/appdev.101/b10795/adfns_fl.htm) - Oracle's flashback technology for historical data queries

