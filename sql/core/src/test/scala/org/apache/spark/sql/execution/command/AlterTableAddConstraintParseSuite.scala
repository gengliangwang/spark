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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions.{CheckConstraint, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AddCheckConstraint

class AlterTableAddConstraintParseSuite extends ConstraintParseSuiteBase {

  test("Add check constraint") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT c1 CHECK (d > 0)
        |""".stripMargin
    val parsed = parsePlan(sql)
    val expected = AddCheckConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... ADD CONSTRAINT"),
      CheckConstraint(
        child = GreaterThan(UnresolvedAttribute("d"), Literal(0)),
        condition = "d > 0",
        name = "c1"
      ))
    comparePlans(parsed, expected)
  }

  test("Add invalid check constraint name") {
    val sql =
      """
        |ALTER TABLE a.b.c ADD CONSTRAINT c1-c3 CHECK (d > 0)
        |""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    checkError(e, "INVALID_IDENTIFIER", "42602", Map("ident" -> "c1-c3"))
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

  test("Add check constraint with valid characteristic") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql =
        s"""
           |ALTER TABLE a.b.c ADD CONSTRAINT c1 CHECK (d > 0) $enforcedStr $relyStr
           |""".stripMargin
      val parsed = parsePlan(sql)
      val expected = AddCheckConstraint(
        UnresolvedTable(
          Seq("a", "b", "c"),
          "ALTER TABLE ... ADD CONSTRAINT"),
        CheckConstraint(
          child = GreaterThan(UnresolvedAttribute("d"), Literal(0)),
          condition = "d > 0",
          name = "c1",
          characteristic = characteristic
        ))
      comparePlans(parsed, expected)
    }
  }


  test("Add check constraint with invalid characteristic") {
    val combinations = Seq(
      ("ENFORCED", "ENFORCED"),
      ("ENFORCED", "NOT ENFORCED"),
      ("NOT ENFORCED", "ENFORCED"),
      ("NOT ENFORCED", "NOT ENFORCED"),
      ("RELY", "RELY"),
      ("RELY", "NORELY"),
      ("NORELY", "RELY"),
      ("NORELY", "NORELY")
    )

    combinations.foreach { case (characteristic1, characteristic2) =>
      val sql =
        s"ALTER TABLE a.b.c ADD CONSTRAINT c1 CHECK (d > 0) $characteristic1 $characteristic2"

      val e = intercept[ParseException] {
        parsePlan(sql)
      }
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (d > 0) $characteristic1 $characteristic2",
        start = 22,
        stop = 50 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = e,
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }
}
