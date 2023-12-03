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

package org.apache.spark.status.protobuf

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.status._
import org.apache.spark.util.Utils.tryWithResource

class KVStoreProtobufSerializerSuite extends KVStoreSerializerSuite {
  override val serializer = new KVStoreProtobufSerializer()

  test("All the string fields must be optional to avoid NPE") {
    val protoFile = getWorkspaceFilePath(
      "core", "src", "main", "protobuf", "org", "apache", "spark", "status", "protobuf",
      "store_types.proto")

    val containsStringRegex = "\\s*string .*"
    val invalidDefinition = new mutable.ArrayBuffer[(String, Int)]()
    var lineNumber = 1
    tryWithResource(Source.fromFile(protoFile.toFile.getCanonicalPath)) { file =>
      file.getLines().foreach { line =>
        if (line.matches(containsStringRegex)) {
          invalidDefinition.append((line, lineNumber))
        }
        lineNumber += 1
      }
    }
    val errorMessage = new StringBuilder()
    errorMessage.append(
      """
        |All the string fields should be defined as `optional string` for handling null string.
        |Please update the following fields:
        |""".stripMargin)
    invalidDefinition.foreach { case (line, num) =>
      errorMessage.append(s"line #$num: $line\n")
    }
    assert(invalidDefinition.isEmpty, errorMessage)
  }
}
