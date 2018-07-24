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
package org.apache.spark.sql.sources.v2

import java.util.Optional

import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetTest}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    throw new AnalysisException("Dummy file reader")
  }

  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"
}

class DummyWriteOnlyFileDataSourceV2 extends FileDataSourceV2 with WriteSupport {
  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {
    throw new AnalysisException("Dummy file writer")
  }

  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"
}

class SimpleFileDataSourceV2 extends SimpleDataSourceV2 with FileDataSourceV2 {
  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"
}

class FileDataSourceV2FallBackSuite extends QueryTest with ParquetTest with SharedSQLContext {
  import testImplicits._

  private val dummyParquetReaderV2 = classOf[DummyReadOnlyFileDataSourceV2].getName
  private val dummyParquetWriterV2 = classOf[DummyWriteOnlyFileDataSourceV2].getName
  private val simpleFileDataSourceV2 = classOf[SimpleFileDataSourceV2].getName

  test("Fall back to v1 when writing to file with read only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Writing file should fall back to v1 and succeed.
      df.write.format(dummyParquetReaderV2).save(path)

      // Validate write result with [[ParquetFileFormat]].
      checkAnswer(spark.read.parquet(path), df)

      // Dummy File reader should fail as expected.
      val exception = intercept[AnalysisException] {
        spark.read.format(dummyParquetReaderV2).load(path)
      }
      assert(exception.message.equals("Dummy file reader"))
    }
  }

  test("Fall back to v1 when reading file with write only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath

      // Dummy File writer should fail as expected.
      val exception = intercept[AnalysisException] {
        df.write.format(dummyParquetWriterV2).save(path)
      }
      assert(exception.message.equals("Dummy file writer"))

      df.write.parquet(path)
      // Reading file should fall back to v1 and succeed.
      checkAnswer(spark.read.format(dummyParquetWriterV2).load(path), df)
    }
  }

  test("Fall back read path to v1 with configuration DISABLED_V2_FILE_DATA_SOURCE_READERS") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      withSQLConf(SQLConf.DISABLED_V2_FILE_DATA_SOURCE_READERS.key -> "foo,parquet,bar") {
        // Reading file should fall back to v1 and succeed.
        checkAnswer(spark.read.format(dummyParquetReaderV2).load(path), df)
      }

      withSQLConf(SQLConf.DISABLED_V2_FILE_DATA_SOURCE_READERS.key -> "foo,bar") {
        // Dummy File reader should fail as DISABLED_V2_FILE_DATA_SOURCE_READERS doesn't include it.
        val exception = intercept[AnalysisException] {
          spark.read.format(dummyParquetReaderV2).load(path)
        }
        assert(exception.message.equals("Dummy file reader"))
      }
    }
  }

  test("Fall back write path to v1 with configuration DISABLED_V2_FILE_DATA_SOURCE_READERS") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath

      withSQLConf(SQLConf.DISABLED_V2_FILE_DATA_SOURCE_WRITERS.key -> "foo,bar") {
        // Dummy File writer should fail as expected.
        val exception = intercept[AnalysisException] {
          df.write.format(dummyParquetWriterV2).save(path)
        }
        assert(exception.message.equals("Dummy file writer"))
      }

      withSQLConf(SQLConf.DISABLED_V2_FILE_DATA_SOURCE_WRITERS.key -> "foo,parquet,bar") {
        // Writing file should fall back to v1 and succeed.
        df.write.format(dummyParquetWriterV2).save(path)
      }

      checkAnswer(spark.read.format(dummyParquetWriterV2).load(path), df)
    }
  }

  test("InsertIntoTable: Fall back to V1") {
    val df1 = (100 until 105).map(i => (i, -i)).toDF("i", "j")
    val df2 = (5 until 10).map(i => (i, -i)).toDF("i", "j")
    withTempPath { file =>
      val path = file.getCanonicalPath
      withTempView("tmp", "tbl") {
        df1.createOrReplaceTempView("tmp")
        df2.write.parquet(path)
        // Create temporary view with FileDataSourceV2
        spark.read.format(simpleFileDataSourceV2).load(path).createOrReplaceTempView("tbl")
        sql("INSERT INTO TABLE tbl SELECT * FROM tmp")
        checkAnswer(spark.read.parquet(path), df1.union(df2))
      }
    }
  }
}
