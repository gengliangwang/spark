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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreateTable, OptionList, UnresolvedTableSpec}
import org.apache.spark.sql.types.{IntegerType, StringType}

class CreateTableConstraintParseSuite extends ConstraintParseSuiteBase {

  def createExpectedPlan(
      columns: Seq[ColumnDefinition],
      constraints: Seq[TableConstraint]): CreateTable = {
    val tableId = UnresolvedIdentifier(Seq("t"))
    val tableSpec = UnresolvedTableSpec(
      Map.empty[String, String], Some("parquet"), OptionList(Seq.empty),
      None, None, None, None, false, constraints)
    CreateTable(tableId, columns, Seq.empty, tableSpec, false)
  }

  def verifyConstraints(sql: String, constraints: Seq[TableConstraint]): Unit = {
    val parsed = parsePlan(sql)
    val columns = Seq(
      ColumnDefinition("a", IntegerType),
      ColumnDefinition("b", StringType)
    )
    val expected = createExpectedPlan(columns = columns, constraints = constraints)
    comparePlans(parsed, expected)
  }

  test("Create table with one check constraint - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0)) USING parquet"
    val constraint = CheckConstraint(
      child = GreaterThan(UnresolvedAttribute("a"), Literal(0)),
      condition = "a > 0",
      name = "c1")
    val constraints = Seq(constraint)
    verifyConstraints(sql, constraints)
  }

  test("Create table with two check constraints - table level") {
    val sql = "CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0), " +
      "CONSTRAINT c2 CHECK (b = 'foo')) USING parquet"
    val constraint1 = CheckConstraint(
      child = GreaterThan(UnresolvedAttribute("a"), Literal(0)),
      condition = "a > 0",
      name = "c1")
    val constraint2 = CheckConstraint(
      child = EqualTo(UnresolvedAttribute("b"), Literal("foo")),
      condition = "b = 'foo'",
      name = "c2")
    val constraints = Seq(constraint1, constraint2)
    verifyConstraints(sql, constraints)
  }

  test("Create table with valid characteristic - table level") {
    validConstraintCharacteristics.foreach { case (enforcedStr, relyStr, characteristic) =>
      val sql = s"CREATE TABLE t (a INT, b STRING, CONSTRAINT c1 CHECK (a > 0) " +
        s"$enforcedStr $relyStr) USING parquet"
      val constraint = CheckConstraint(
        child = GreaterThan(UnresolvedAttribute("a"), Literal(0)),
        condition = "a > 0",
        name = "c1",
        characteristic = characteristic)
      val constraints = Seq(constraint)
      verifyConstraints(sql, constraints)
    }
  }

  test("Create table with invalid characteristic") {
    invalidConstraintCharacteristics.foreach { case (characteristic1, characteristic2) =>
      val constraintStr = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2"
      val expectedContext = ExpectedContext(
        fragment = s"CONSTRAINT c1 CHECK (a > 0) $characteristic1 $characteristic2",
        start = 33,
        stop = 61 + characteristic1.length + characteristic2.length
      )
      checkError(
        exception = intercept[ParseException] {
          parsePlan(s"CREATE TABLE t (a INT, b STRING, $constraintStr ) USING parquet")
        },
        condition = "INVALID_CONSTRAINT_CHARACTERISTICS",
        parameters = Map("characteristics" -> s"$characteristic1, $characteristic2"),
        queryContext = Array(expectedContext))
    }
  }

  test("Create table with column 'constraint'") {
    val sql = "CREATE TABLE t (constraint STRING) USING parquet"
    val columns = Seq(ColumnDefinition("constraint", StringType))
    val expected = createExpectedPlan(columns, Seq.empty)
    comparePlans(parsePlan(sql), expected)
  }
}
