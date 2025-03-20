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
package org.apache.spark.sql.connector.catalog.constraints;

import org.apache.spark.sql.connector.expressions.filter.Predicate;

public interface Constraint {
    String name(); // either assigned by the user or auto generated
    String description(); // used in toString()
    String toDDL(); // used in EXPLAIN/DESCRIBE/SHOW CREATE TABLE
    boolean rely(); // indicates whether the constraint is believed to be true
    boolean enforced(); // indicates whether the constraint must be enforced

    static Constraint check(String name, String sql, Predicate predicate) {
      return new Check(name, sql, predicate);
    }

    final class Check implements Constraint {
        private final String name;
        private final String sql;
        private final Predicate predicate;
        private Check(String name, String sql, Predicate predicate) {
          this.name = name;
          this.sql = sql;
          this.predicate = predicate;
        }

        @Override public String name() {
            return name;
        }

        @Override public String description() {
            return "check constraint";
        }

        @Override
        public String toDDL() {
            return "CHECK (" + sql + ")";
        }

        @Override public boolean rely() {
            return true;
        }

        @Override public boolean enforced() {
            return true;
        }

        public String sql() { return sql; }

        public Predicate predicate() { return predicate; }
    }
}
