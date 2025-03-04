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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.Constraint
import org.apache.spark.sql.types.{DataType, StringType}

trait ConstraintExpression extends Expression with Unevaluable {
  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  def asConstraint: Constraint
}

case class CheckConstraint(
    name: String,
    override val sql: String,
    child: Expression) extends ConstraintExpression
  with UnaryLike[Expression] {

  def asConstraint: Constraint = {
    val predicate = new V2ExpressionBuilder(child, true).buildPredicate().orNull
    Constraint.check(name, sql, predicate)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

