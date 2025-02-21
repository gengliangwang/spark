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

class CheckConstraintSuite extends QueryTest with CommandSuiteBase {
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

  test("Can't convert expression to V2 predicate") {
    withTable("t") {
      sql("create table t(i string) using parquet")
      val query =
        """
          |ALTER TABLE t ADD CONSTRAINT c1 CHECK (from_json(i, 'a INT').a > 1)
          |""".stripMargin
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "INVALID_CHECK_CONSTRAINT.INVALID_V2_PREDICATE",
        sqlState = "42621",
        parameters = Map.empty,
        context = ExpectedContext(
          fragment = "from_json(i, 'a INT').a > 1",
          start = 40,
          stop = 66
        )
      )
    }
  }
}
