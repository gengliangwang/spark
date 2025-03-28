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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.types.{DataType, StringType}

trait TableConstraint {
  // Convert to a data source v2 constraint
  def asConstraint: Constraint

  def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic): TableConstraint

  def name: String

  def characteristic: ConstraintCharacteristic

  def defaultName: String

  def defaultConstraintCharacteristic: ConstraintCharacteristic

  protected def getCharacteristicValues: (Boolean, Boolean) = {
    val rely = characteristic.rely.getOrElse(defaultConstraintCharacteristic.rely.get)
    val enforced = characteristic.enforced.getOrElse(defaultConstraintCharacteristic.enforced.get)
    (rely, enforced)
  }
}

case class ConstraintCharacteristic(enforced: Option[Boolean], rely: Option[Boolean])

object ConstraintCharacteristic {
  val empty: ConstraintCharacteristic = ConstraintCharacteristic(None, None)
}

case class CheckConstraint(
    child: Expression,
    condition: String,
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends UnaryExpression
  with Unevaluable
  with TableConstraint {

  def asConstraint: Constraint = {
    val predicate = new V2ExpressionBuilder(child, true).buildPredicate().orNull
    val (rely, enforced) = getCharacteristicValues
    val constraintName = if (name == null) defaultName else name
    Constraint
      .check(constraintName)
      .sql(condition)
      .predicate(predicate)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.VALID)
      .build()
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic): TableConstraint = {
    copy(name = name, characteristic = c)
  }

  override def defaultName: String =
    throw new AnalysisException(
      errorClass = "INVALID_CHECK_CONSTRAINT.MISSING_NAME",
      messageParameters = Map.empty)

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(true), rely = Some(true))

  override def sql: String = s"CONSTRAINT $name CHECK ($condition)"

  override def dataType: DataType = StringType
}

case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {

  override def asConstraint: Constraint = {
    val (rely, enforced) = getCharacteristicValues
    val constraintName = if (name == null) defaultName else name
    Constraint
      .primaryKey(constraintName, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic): TableConstraint = {
    copy(name = name, characteristic = c)
  }

  override def defaultName: String = "pk"

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}

case class UniqueConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
    extends TableConstraint {

  override def asConstraint: Constraint = {
    val (rely, enforced) = getCharacteristicValues
    val constraintName = if (name == null) defaultName else name
    Constraint
      .unique(constraintName, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override def withNameAndCharacteristic(
    name: String,
    c: ConstraintCharacteristic): TableConstraint = {
    copy(name = name, characteristic = c)
  }

  override def defaultName: String =
    throw new AnalysisException(
      errorClass = "INVALID_UNIQUE_CONSTRAINT.MISSING_NAME",
      messageParameters = Map.empty)

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}

case class ForeignKeyConstraint(
    override val name: String = null,
    childColumns: Seq[String] = Seq.empty,
    parentTableId: Seq[String] = Seq.empty,
    parentColumns: Seq[String] = Seq.empty,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def asConstraint: Constraint = {
    val (rely, enforced) = getCharacteristicValues
    val constraintName = if (name == null) defaultName else name
    Constraint
      .foreignKey(constraintName,
        childColumns.map(FieldReference.column).toArray,
        parentTableId.asIdentifier,
        parentColumns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic): TableConstraint = {
    copy(name = name, characteristic = c)
  }

  override def defaultName: String = "fk"

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}
