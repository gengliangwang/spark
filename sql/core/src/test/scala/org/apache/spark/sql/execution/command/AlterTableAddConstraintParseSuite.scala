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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AlterTableAddConstraint
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableAddConstraintParseSuite extends AnalysisTest with SharedSparkSession {

  test("Add check constraint") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT c1 CHECK (d > 0)
        |""".stripMargin
    val parsed = parsePlan(sql)
    val expected = AlterTableAddConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... ADD CONSTRAINT"),
      "c1",
      GreaterThan(UnresolvedAttribute("d"), Literal(0)))
    comparePlans(parsed, expected)
  }

  test("Add invalid check constraint name") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT c1-c3 CHECK (d > 0)
        |""".stripMargin
    val msg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(msg.contains("Syntax error at or near '-'."))
  }

  test("Add invalid check constraint expression") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT c1 CHECK (d >)
        |""".stripMargin
    val msg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(msg.contains("Syntax error at or near ')'"))
  }
}
