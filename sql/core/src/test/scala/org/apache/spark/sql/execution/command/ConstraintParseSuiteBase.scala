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

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.expressions.ConstraintCharacteristic
import org.apache.spark.sql.test.SharedSparkSession

abstract class ConstraintParseSuiteBase extends AnalysisTest with SharedSparkSession {
  protected val validConstraintCharacteristics = Seq(
    ("", "", ConstraintCharacteristic(enforced = None, rely = None)),
    ("ENFORCED", "", ConstraintCharacteristic(enforced = Some(true), rely = None)),
    ("NOT ENFORCED", "", ConstraintCharacteristic(enforced = Some(false), rely = None)),
    ("", "RELY", ConstraintCharacteristic(enforced = None, rely = Some(true))),
    ("", "NORELY", ConstraintCharacteristic(enforced = None, rely = Some(false))),
    ("ENFORCED", "RELY", ConstraintCharacteristic(enforced = Some(true), rely = Some(true))),
    ("ENFORCED", "NORELY", ConstraintCharacteristic(enforced = Some(true), rely = Some(false))),
    ("NOT ENFORCED", "RELY",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(true))),
    ("NOT ENFORCED", "NORELY",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(false)))
  )

  protected val invalidConstraintCharacteristics = Seq(
    ("ENFORCED", "ENFORCED"),
    ("ENFORCED", "NOT ENFORCED"),
    ("NOT ENFORCED", "ENFORCED"),
    ("NOT ENFORCED", "NOT ENFORCED"),
    ("RELY", "RELY"),
    ("RELY", "NORELY"),
    ("NORELY", "RELY"),
    ("NORELY", "NORELY")
  )


}
