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
package org.apache.spark.sql.execution.datasources.v2.csv

import org.apache.hadoop.fs.FileStatus
import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSource, CSVOptions, CSVUtils, UnivocityParser}
import org.apache.spark.sql.execution.datasources.v2.FileSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

class CSVDatasourceV2 extends DataSourceV2 with ReadSupport with ReadSupportWithSchema {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new CSVDataSourceReader(options, None)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new CSVDataSourceReader(options, Some(schema))
  }
}

case class CSVDataSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends FileSourceReader {
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions =
      new CSVOptions(options.asMap().asScala.toMap,
        sparkSession.sessionState.conf.sessionLocalTimeZone)
    CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override def readFunction: (PartitionedFile) => Iterator[InternalRow] = {
    val dataSchema = this.dataSchema
    val requiredSchema = this.readSchema()
    val requiredOutput = requiredSchema.toAttributes
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val parsedOptions =
      new CSVOptions(options.asMap().asScala.toMap,
        sparkSession.sessionState.conf.sessionLocalTimeZone)
    CSVUtils.verifySchema(dataSchema)
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    dataSchema.getFieldIndex(parsedOptions.columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = dataSchema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new AnalysisException(
          "The field for corrupt records must be string type and nullable")
      }
    }

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new AnalysisException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
          "referenced columns only include the internal corrupt record column\n" +
          s"(named _corrupt_record by default). For example:\n" +
          "spark.read.schema(schema).csv(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
          "and spark.read.schema(schema).csv(file).select(\"_corrupt_record\").show().\n" +
          "Instead, you can cache or save the parsed results and then send the same query.\n" +
          "For example, val df = spark.read.schema(schema).csv(file).cache() and then\n" +
          "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val parser = new UnivocityParser(
        StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        parsedOptions)
      val unsafeProjection = GenerateUnsafeProjection.generate(requiredOutput, requiredOutput)
      CSVDataSource(parsedOptions)
        .readFile(conf, file, parser, requiredSchema)
        .map(unsafeProjection)
    }
  }
}
