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

package org.apache.spark.sql.connector.files

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex}

/**
 * A planned unit of work produced by a [[FileBatch]] or [[FileMicroBatchStream]]: enough
 * information to materialize a single `HadoopFsRelation` and route it through Spark's V1 file
 * scan path (i.e. `FileSourceScanExec`).
 *
 * Connectors that mix DSv2 logical planning with V1 file execution return a sequence of these
 * from their batch/stream, so each `FileSet` produces one V1 scan that the lowering strategy
 * stitches into the larger physical plan.
 */
case class FileSet(
    index: FileIndex,
    format: FileFormat,
    options: Map[String, String],
    partitionColumns: Seq[NamedReference],
    dataColumns: Seq[NamedReference])
