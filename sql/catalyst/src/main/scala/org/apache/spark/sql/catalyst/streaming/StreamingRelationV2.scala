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

package org.apache.spark.sql.catalyst.streaming

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{ExposesMetadataColumns, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, SupportsMetadataColumns, Table, TableProvider}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogReadInfo, DataSourceV2Implicits}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// We have to pack in the V1 data source as a shim, for the case when a source implements
// continuous processing (which is always V2) but only has V1 microbatch support. We don't
// know at read time whether the query is continuous or not, so we need to be able to
// swap a V1 relation back in.
/**
 * Used to link a [[Table]] into a streaming [[LogicalPlan]].
 */
case class StreamingRelationV2(
    source: Option[TableProvider],
    sourceName: String,
    table: Table,
    extraOptions: CaseInsensitiveStringMap,
    output: Seq[AttributeReference],
    catalog: Option[CatalogPlugin],
    identifier: Option[Identifier],
    v1Relation: Option[LogicalPlan],
    sourceIdentifyingName: StreamingSourceIdentifyingName = Unassigned,
    changelogInfo: Option[ChangelogReadInfo] = None)
  extends LeafNode with MultiInstanceRelation with ExposesMetadataColumns
  with HasStreamingSourceIdentifyingName {
  override lazy val resolved = v1Relation.forall(_.resolved)
  override def isStreaming: Boolean = true
  override def toString: String = sourceName

  import DataSourceV2Implicits._

  // Keep source identity independent of the connector's derived Changelog allocation.
  private lazy val comparisonKey =
    (source, sourceName, changelogInfo.map(_.baseTable).getOrElse(table), extraOptions, output,
      catalog, identifier, v1Relation, sourceIdentifyingName, changelogInfo)

  private lazy val comparisonHash = comparisonKey.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: StreamingRelationV2 =>
      that.canEqual(this) && comparisonKey == that.comparisonKey
    case _ => false
  }

  override def hashCode(): Int = comparisonHash

  override lazy val metadataOutput: Seq[AttributeReference] = table match {
    case hasMeta: SupportsMetadataColumns =>
      metadataOutputWithOutConflicts(
        hasMeta.metadataColumns.toAttributes, hasMeta.canRenameConflictingMetadataColumns)
    case _ =>
      Nil
  }

  def withMetadataColumns(): StreamingRelationV2 = {
    val newMetadata = metadataOutput.filterNot(outputSet.contains)
    if (newMetadata.nonEmpty) {
      copy(output = output ++ newMetadata)
    } else {
      this
    }
  }

  override def withSourceIdentifyingName(name: StreamingSourceIdentifyingName): LogicalPlan = {
    copy(sourceIdentifyingName = name)
  }

  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))
}
