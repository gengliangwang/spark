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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{CheckConstraint, Constraints, EqualTo, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreateTable, OptionList, UnresolvedTableSpec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

class CreateTableConstraintParseSuite extends AnalysisTest with SharedSparkSession {
  val createTablePrefix = "CREATE TABLE t (a INT, b STRING) USING parquet"
  val tableId = UnresolvedIdentifier(Seq("t"))
  val columns = Seq(
    ColumnDefinition("a", IntegerType),
    ColumnDefinition("b", StringType)
  )

  def verifyConstraints(constraintStr: String, constraints: Constraints): Unit = {
    val sql =
      s"""
         |$createTablePrefix
         |$constraintStr
         |""".stripMargin

    val parsed = parsePlan(sql)
    val tableSpec = UnresolvedTableSpec(
      Map.empty[String, String], Some("parquet"), OptionList(Seq.empty),
      None, None, None, None, false, constraints)
    val expected = CreateTable(tableId, columns, Seq.empty, tableSpec, false)
    comparePlans(parsed, expected)
  }

  test("Create table with one check constraint") {
    val constraintStr = "CONSTRAINT c1 CHECK (a > 0)"
    val constraint = CheckConstraint("c1", "a>0", GreaterThan(UnresolvedAttribute("a"), Literal(0)))
    val constraints = Constraints(Seq(constraint))
    verifyConstraints(constraintStr, constraints)
  }

  test("Create table with two check constraints") {
    val constraintStr = "CONSTRAINT c1 CHECK (a > 0) CONSTRAINT c2 CHECK (b = 'foo')"
    val constraint1 =
      CheckConstraint("c1", "a>0", GreaterThan(UnresolvedAttribute("a"), Literal(0)))
    val constraint2 =
      CheckConstraint("c2", "b='foo'", EqualTo(UnresolvedAttribute("b"), Literal("foo")))
    val constraints = Constraints(Seq(constraint1, constraint2))
    verifyConstraints(constraintStr, constraints)
  }
}
