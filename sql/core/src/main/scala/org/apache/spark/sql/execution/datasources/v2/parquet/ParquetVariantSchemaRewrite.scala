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

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.sql.connector.read.VariantExtraction
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.types.{BooleanType, DataType, StructField, StructType, VariantType}

/**
 * Rewrites a read schema for pushed-down variant extractions: each extracted variant column is
 * replaced by a "variant struct" whose fields carry [[VariantMetadata]], which is how the parquet
 * reader is told to shred the variant. This mirrors `ParquetScan.rewriteVariantPushdownSchema`
 * and is used by the Parquet FileScan connector ([[ParquetFileScan]]) so it produces the same
 * shredded schema as the V2 batch path. Keep the two in sync.
 */
private[parquet] object ParquetVariantSchemaRewrite {

  def rewrite(schema: StructType, extractions: Array[VariantExtraction]): StructType = {
    if (extractions.isEmpty) {
      return schema
    }
    // Group extractions by column name and build the extracted (variant struct) schema for each.
    val variantSchemaMap: Map[Seq[String], StructType] = extractions
      .groupBy(e => e.columnName().toSeq)
      .map { case (colName, columnExtractions) =>
        var fields = columnExtractions.zipWithIndex.map { case (extraction, idx) =>
          // Attach VariantMetadata so the Parquet reader knows this is a variant extraction.
          StructField(idx.toString, extraction.expectedDataType(), nullable = true,
            extraction.metadata())
        }
        // Avoid producing an empty struct of requested fields. This happens if the variant is not
        // used, or only used in `IsNotNull/IsNull`. The placeholder field's value does not matter.
        if (fields.length == 1 && fields.head.dataType.isInstanceOf[VariantType]) {
          val placeholder = VariantMetadata(
            "$.__placeholder_field__", failOnError = false, timeZoneId = "UTC")
          fields = Array(StructField("0", BooleanType, metadata = placeholder.toMetadata))
        }
        colName -> StructType(fields)
      }.toMap

    rewriteType(schema, Seq.empty, variantSchemaMap).asInstanceOf[StructType]
  }

  private def rewriteType(
      dataType: DataType,
      path: Seq[String],
      mapping: Map[Seq[String], StructType]): DataType = {
    dataType match {
      case structType: StructType if !VariantMetadata.isVariantStruct(structType) =>
        val fields = structType.fields.map { field =>
          mapping.get(path :+ field.name) match {
            case Some(extractedSchema) =>
              field.copy(dataType = extractedSchema)
            case None =>
              field.copy(dataType = rewriteType(field.dataType, path :+ field.name, mapping))
          }
        }
        StructType(fields)
      case otherType => otherType
    }
  }
}
