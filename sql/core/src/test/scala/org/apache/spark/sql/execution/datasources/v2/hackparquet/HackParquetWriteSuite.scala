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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.{AppendFilesExec, OverwriteFilesByExpressionExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * End-to-end tests for the hackparquet DSv2 connector — proves the new
 * `FileWrite` + `FileWriteExec` path writes Parquet files that the built-in `parquet` reader
 * can read back, and that the planner lowers through the new exec nodes.
 */
class HackParquetWriteSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def captureExecutedPlan(thunk: => Unit): QueryExecution = {
    @volatile var captured: QueryExecution = null
    val listener = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured = qe
      }
      override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = ()
    }
    spark.listenerManager.register(listener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.listenerManager.unregister(listener)
    }
    assert(captured != null, "expected at least one QueryExecution to be observed")
    captured
  }

  test("append writes parquet files via FileWrite") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("k", "v")
        .write.format("hackparquet").mode("append").save(path)

      checkAnswer(
        spark.read.parquet(path),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("overwrite truncates the output directory") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("k", "v")
        .write.format("hackparquet").mode("append").save(path)
      Seq((10, "x")).toDF("k", "v")
        .write.format("hackparquet").mode("overwrite").save(path)

      checkAnswer(spark.read.parquet(path), Seq(Row(10, "x")))
    }
  }

  test("append routes through AppendFilesExec") {
    withTempDir { dir =>
      val qe = captureExecutedPlan {
        Seq((1, "a")).toDF("k", "v")
          .write.format("hackparquet").mode("append").save(dir.getCanonicalPath)
      }
      assert(qe.executedPlan.exists(_.isInstanceOf[AppendFilesExec]),
        s"expected AppendFilesExec in executed plan:\n${qe.executedPlan}")
    }
  }

  test("overwrite routes through OverwriteFilesByExpressionExec") {
    withTempDir { dir =>
      // Seed the directory so overwrite has something to truncate.
      Seq((1, "a")).toDF("k", "v")
        .write.format("hackparquet").mode("append").save(dir.getCanonicalPath)
      val qe = captureExecutedPlan {
        Seq((2, "b")).toDF("k", "v")
          .write.format("hackparquet").mode("overwrite").save(dir.getCanonicalPath)
      }
      assert(qe.executedPlan.exists(_.isInstanceOf[OverwriteFilesByExpressionExec]),
        s"expected OverwriteFilesByExpressionExec in executed plan:\n${qe.executedPlan}")
    }
  }
}
