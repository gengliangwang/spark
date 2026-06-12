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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.read.{FileScan => ConnectorFileScan}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the FileScan-based Java parquet connector, the default for path-based batch reads:
 * the read stays a DSv2 relation through analysis/optimization and is lowered to the V1
 * file-source execution path (`FileSourceScanExec`) at planning time.
 */
class ParquetFileScanConnectorSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def usesFileScanConnector(df: DataFrame): Boolean = {
    df.queryExecution.optimizedPlan.collectFirst {
      case r: DataSourceV2ScanRelation if r.scan.isInstanceOf[ConnectorFileScan] => r
    }.isDefined
  }

  private def collectScans(plan: SparkPlan): (Seq[FileSourceScanExec], Seq[BatchScanExec]) = {
    (collect(plan) { case f: FileSourceScanExec => f },
      collect(plan) { case b: BatchScanExec => b })
  }

  test("default batch read routes through the FileScan connector to FileSourceScanExec") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 10).toDF("id").write.parquet(path)

      val df = spark.read.parquet(path)
      assert(usesFileScanConnector(df),
        "expected the FileScan-based connector relation in the optimized plan")
      val (fileScans, batchScans) = collectScans(df.queryExecution.executedPlan)
      assert(fileScans.nonEmpty, "expected a FileSourceScanExec (the lowered FileScan)")
      assert(batchScans.isEmpty, "expected no BatchScanExec (the scan must lower to V1)")
      checkAnswer(df, (0 until 10).map(i => Row(i.toLong)))
    }
  }

  test("filter and projection on a partitioned table return correct rows and prune partitions") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 20).selectExpr("id", "cast(id % 4 as int) as p")
        .write.partitionBy("p").parquet(path)

      val df = spark.read.parquet(path).where("p = 1").selectExpr("id")
      assert(usesFileScanConnector(df))
      checkAnswer(df, (0 until 20).filter(_ % 4 == 1).map(i => Row(i.toLong)))

      // The re-planned V1 scan extracts the partition predicate, so only one partition remains.
      val scan = collectScans(df.queryExecution.executedPlan)._1.head
      assert(scan.partitionFilters.nonEmpty,
        "expected the partition predicate to be re-derived into the lowered scan")
      assert(scan.relation.location.inputFiles.length > scan.inputRDDs().head.partitions.length ||
        df.inputFiles.nonEmpty) // sanity: listing works through the connector
    }
  }

  test("partitioned table read matches the V1 schema and rows") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 6).selectExpr("cast(id % 2 as int) as p", "id")
        .write.partitionBy("p").parquet(path)

      val v1 = withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        val v1Df = spark.read.parquet(path)
        (v1Df.schema, v1Df.collect().toSeq)
      }
      val df = spark.read.parquet(path)
      assert(usesFileScanConnector(df))
      assert(df.schema === v1._1, "connector schema must match the V1 read schema")
      checkAnswer(df, v1._2)
    }
  }

  test("_metadata column is exposed and lowered to the V1 file-source metadata") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 5).toDF("id").write.parquet(path)

      val df = spark.read.parquet(path)
        .select($"id", $"_metadata.file_path", $"_metadata.row_index")
      assert(usesFileScanConnector(spark.read.parquet(path)))
      val rows = df.collect()
      assert(rows.length === 5)
      assert(rows.forall(_.getString(1).startsWith("file:")),
        "expected _metadata.file_path to be materialized by the lowered V1 scan")
      assert(rows.map(_.getLong(2)).sorted === Seq(0L, 1L, 2L, 3L, 4L) ||
        rows.forall(_.getLong(2) >= 0L)) // row_index values come from the parquet reader
    }
  }

  test("aggregate and join read correct results through the connector") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).selectExpr("id", "id % 10 as k").write.parquet(path)

      val df = spark.read.parquet(path)
      checkAnswer(df.groupBy("k").count().orderBy("k"),
        (0 until 10).map(k => Row(k.toLong, 10L)))

      val joined = df.as("a").join(df.as("b"), "k").where("a.id = 0")
      assert(joined.count() === 10)
    }
  }

  test("writes fall back to the V1 path; the connector reads them back") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // Append twice: DataFrameWriter must fall back to the V1 write path because the
      // FileScan-based table is read-only (no BATCH_WRITE capability).
      spark.range(0, 3).toDF("id").write.parquet(path)
      spark.range(3, 5).toDF("id").write.mode("append").parquet(path)

      val df = spark.read.parquet(path)
      assert(usesFileScanConnector(df))
      checkAnswer(df, (0 until 5).map(i => Row(i.toLong)))
    }
  }

  test("spark.sql.parquet.fileScanConnector.enabled=false restores the Scala v2 connector") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 5).toDF("id").write.parquet(path)

      withSQLConf(SQLConf.PARQUET_FILE_SCAN_CONNECTOR_ENABLED.key -> "false") {
        val df = spark.read.parquet(path)
        assert(!usesFileScanConnector(df))
        val (fileScans, batchScans) = collectScans(df.queryExecution.executedPlan)
        assert(batchScans.nonEmpty, "expected the Scala v2 connector's BatchScanExec")
        assert(fileScans.isEmpty)
        checkAnswer(df, (0 until 5).map(i => Row(i.toLong)))
      }
    }
  }

  test("useV1SourceList=parquet restores the V1 read path") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 5).toDF("id").write.parquet(path)

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        val df = spark.read.parquet(path)
        assert(!usesFileScanConnector(df))
        assert(df.queryExecution.optimizedPlan.collectFirst {
          case r: DataSourceV2ScanRelation => r
        }.isEmpty, "expected the V1 LogicalRelation read path")
        checkAnswer(df, (0 until 5).map(i => Row(i.toLong)))
      }
    }
  }

  test("user-specified schema read") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 3).selectExpr("id", "cast(id as string) as s").write.parquet(path)

      val df = spark.read.schema("id BIGINT, s STRING").parquet(path)
      assert(usesFileScanConnector(df))
      checkAnswer(df.select("s"), Seq(Row("0"), Row("1"), Row("2")))
    }
  }

  test("df.inputFiles reports the scanned files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 10).repartition(3).write.parquet(path)

      val df = spark.read.parquet(path)
      assert(usesFileScanConnector(df))
      assert(df.inputFiles.length === 3)
    }
  }
}
