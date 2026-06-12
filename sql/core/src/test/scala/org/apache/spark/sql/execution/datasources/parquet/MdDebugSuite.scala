package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class MdDebugSuite extends QueryTest with SharedSparkSession {
  test("md debug2") {
    withTempPath { dir =>
      val p = dir.getCanonicalPath
      spark.range(0, 5).toDF("id").write.parquet(p)
      val q = spark.read.parquet(p)
        .select(col("id"), col("_metadata.file_path"), col("_metadata.row_index"))
      // scalastyle:off println
      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
      q.queryExecution.optimizedPlan.foreach {
        case r: DataSourceV2ScanRelation =>
          println("=== SCANREL readSchema: " + r.scan.readSchema().json)
          r.output.foreach(a => println("=== SCANREL attr " + a.name + " md=" + a.metadata.json))
        case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation =>
          r.output.foreach(a => println("=== REL attr " + a.name + " md=" + a.metadata.json))
        case _ =>
      }
      println("=== analyzed:\n" + q.queryExecution.analyzed.treeString)
      println("=== optimized:\n" + q.queryExecution.optimizedPlan.treeString)
      try {
        println("=== spark plan:\n" + q.queryExecution.sparkPlan.treeString)
      } catch { case e: Throwable => println("PLAN FAILED: " + e) }
      // scalastyle:on println
    }
  }
}
