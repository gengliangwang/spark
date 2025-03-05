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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.Constraint.Check
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
      assert(constraint.predicate().toString() == "id > 0")
    }
  }
}
