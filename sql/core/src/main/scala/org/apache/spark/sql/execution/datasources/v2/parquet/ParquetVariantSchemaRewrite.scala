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
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Rewrites a read schema for pushed-down variant extractions: each extracted variant column is
 * replaced by a "variant struct" whose fields carry [[VariantMetadata]], which is how the parquet
 * reader is told to shred the variant. Used by the Parquet FileScan connector ([[ParquetFileScan]])
 * so the schema it lowers to a V1 `HadoopFsRelation` matches the attributes the planner derives
 * from `VariantInRelation.rewriteType` for the pushed extractions.
 */
private[parquet] object ParquetVariantSchemaRewrite {

  // The path of a full-variant request (the whole value, e.g. a bare `SELECT v`).
  private val FULL_VARIANT_PATH = "$"

  /**
   * Decides which pushed variant extractions the connector accepts, returning a per-extraction
   * flag in input order (the `SupportsPushDownVariantExtractions.pushVariantExtractions` contract).
   *
   * A column requested *solely* as the full variant (path `"$"`, with no sub-field request) is
   * declined: shredding the whole value yields no benefit (it is read in full regardless), and --
   * more importantly -- such a request can come from a bare relation read with no Project above it.
   * In that case `V2ScanRelationPushDown`, having rewritten the scan output to the shredded struct,
   * re-derives the post-scan projection from that rewritten output, so there is no surviving
   * reference to reconstruct the original VARIANT and the rewritten plan's output type diverges
   * from the original. When a sub-field is also requested, an explicit Project/Filter exists that
   * references the original attribute id, so the full variant reconstructs correctly.
   *
   * Acceptance is all-or-nothing per variant column: the rule rebuilds a column's shredded struct
   * from its full requested-field set (via `VariantInRelation.rewriteType`), so accepting only some
   * of a column's fields would make the schema this scan reports disagree with the rule's rewritten
   * output. Hence a column is accepted in full (including any `"$"` request) iff it has at least
   * one sub-field request.
   */
  def acceptExtractions(extractions: Array[VariantExtraction]): Array[Boolean] = {
    val columnsWithSubfield = extractions.iterator
      .filter(e => VariantMetadata.fromMetadata(e.metadata()).path != FULL_VARIANT_PATH)
      .map(_.columnName().toSeq)
      .toSet
    extractions.map(e => columnsWithSubfield.contains(e.columnName().toSeq))
  }

  def rewrite(schema: StructType, extractions: Array[VariantExtraction]): StructType = {
    if (extractions.isEmpty) {
      return schema
    }
    // Group extractions by column name and build the extracted (variant struct) schema for each.
    // `V2ScanRelationPushDown` only pushes genuinely requested fields (variants that are unused or
    // used only in `IsNull`/`IsNotNull` are filtered out before pushdown), so each group here has
    // at least one extraction and the ordinal-named fields line up 1:1 -- in the same order -- with
    // the attributes `VariantInRelation.rewriteType` produces for the rule's rewritten output. The
    // two must stay identical: the planner aliases this schema's attributes onto the rewritten
    // output's expr ids, so any type divergence yields a plan with conflicting types per expr id.
    val variantSchemaMap: Map[Seq[String], StructType] = extractions
      .groupBy(e => e.columnName().toSeq)
      .map { case (colName, columnExtractions) =>
        val fields = columnExtractions.zipWithIndex.map { case (extraction, idx) =>
          // Attach VariantMetadata so the Parquet reader knows this is a variant extraction.
          StructField(idx.toString, extraction.expectedDataType(), nullable = true,
            extraction.metadata())
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
