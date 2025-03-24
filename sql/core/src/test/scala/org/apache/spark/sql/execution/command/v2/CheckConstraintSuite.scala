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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.execution.command.DDLCommandTestUtils

class CheckConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "ALTER TABLE .. ADD CONSTRAINT"

  test("Nondeterministic expression") {
    withTable("t") {
      sql("create table t(i double) using parquet")
      val query =
        """
          |ALTER TABLE t ADD CONSTRAINT c1 CHECK (i > rand(0))
          |""".stripMargin
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "INVALID_CHECK_CONSTRAINT.NONDETERMINISTIC",
        sqlState = "42621",
        parameters = Map.empty,
        context = ExpectedContext(
          fragment = "i > rand(0)",
          start = 40,
          stop = 50
        )
      )
    }
  }

  test("Expression referring a column of another table") {
    withTable("t", "t2") {
      sql("create table t(i double) using parquet")
      sql("create table t2(j string) using parquet")
      val query =
        """
          |ALTER TABLE t ADD CONSTRAINT c1 CHECK (len(t2.j) > 0)
          |""".stripMargin
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`t2`.`j`", "proposal" -> "`t`.`i`"),
        context = ExpectedContext(
          fragment = "t2.j",
          start = 44,
          stop = 47
        )
      )
    }
  }

  private def getCheckConstraint(table: Table): Check = {
    assert(table.constraints.length == 1)
    assert(table.constraints.head.isInstanceOf[Check])
    table.constraints.head.asInstanceOf[Check]
    val constraint = table.constraints.head.asInstanceOf[Check]
    assert(constraint.rely())
    assert(constraint.enforced())
    constraint
  }

  test("Predicate should be null if it can't be converted to V2 predicate") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, j string) $defaultUsing")
      sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (from_json(j, 'a INT').a > 1)")
      val table = loadTable(catalog, "ns", "tbl")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "c1")
      assert(constraint.toDDL ==
        "CONSTRAINT c1 CHECK from_json(j, 'a INT').a > 1 ENFORCED VALID RELY")
      assert(constraint.sql() == "from_json(j, 'a INT').a > 1")
      assert(constraint.predicate() == null)
    }
  }

  test("Add check constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (id > 0)")
      val table = loadTable(catalog, "ns", "tbl")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "c1")
      assert(constraint.toDDL == "CONSTRAINT c1 CHECK id > 0 ENFORCED VALID RELY")
    }
  }

  test("Add duplicated check constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT abc CHECK (id > 0)")
      // Constraint names are case-insensitive
      Seq("abc", "ABC").foreach { name =>
        val error = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT $name CHECK (id>0)")
        }
        checkError(
          exception = error,
          condition = "CONSTRAINT_ALREADY_EXISTS",
          sqlState = "42710",
          parameters = Map("constraintName" -> "abc",
            "oldConstraint" -> "CONSTRAINT abc CHECK id > 0 ENFORCED VALID RELY")
        )
      }
    }
  }
}
