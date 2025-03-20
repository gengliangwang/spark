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
import org.apache.spark.sql.connector.catalog.constraints.Constraint.Check
import org.apache.spark.sql.execution.command.DDLCommandTestUtils

class CreateTableConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "CREATE TABLE .. CONSTRAINT"

  test("Create table with one check constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string) $defaultUsing
           | CONSTRAINT c1 CHECK (id > 0)""".stripMargin)
      val constraints = loadTable(catalog, "ns", "tbl").constraints
      assert(constraints.length == 1)
      assert(constraints.head.isInstanceOf[Check])
      val constraint = constraints.head.asInstanceOf[Check]

      assert(constraint.name == "c1")
      assert(constraint.sql == "id>0")
      assert(constraint.predicate().toString() == "id > CAST(0 AS long)")
    }
  }

  test("Create table with two check constraints") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string) $defaultUsing
           | CONSTRAINT c1 CHECK (id > 0)
           | CONSTRAINT c2 CHECK (data = 'foo')""".stripMargin)
      val constraints = loadTable(catalog, "ns", "tbl").constraints
      assert(constraints.length == 2)
      assert(constraints.head.isInstanceOf[Check])
      val constraint = constraints.head.asInstanceOf[Check]

      assert(constraint.name == "c1")
      assert(constraint.sql == "id>0")
      assert(constraint.predicate().toString() == "id > CAST(0 AS long)")

      assert(constraints(1).isInstanceOf[Check])
      val constraint2 = constraints(1).asInstanceOf[Check]

      assert(constraint2.name == "c2")
      assert(constraint2.sql == "data='foo'")
      assert(constraint2.predicate().toString() == "data = 'foo'")
    }
  }

  test("Create table with UnresolvedAttribute in check constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      val query =
        s"""
           |CREATE TABLE $t (id bigint, data string) $defaultUsing
           | CONSTRAINT c2 CHECK (abc = 'foo')""".stripMargin
      val e = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = e,
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`abc`", "proposal" -> "`id`, `data`"),
        sqlState = "42703",
        context = ExpectedContext("abc", 89, 91)) // UnresolvedAttribute abc
    }
  }
}
