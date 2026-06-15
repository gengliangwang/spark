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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.EXPR
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier, ResolvedNamespace, ResolvedPartitionSpec, ResolvedPersistentView, ResolvedTable, ResolvedTempView}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, DynamicPruning, EmptyRow, Expression, Literal, MetadataAttribute, MetadataAttributeWithLogicalName, NamedExpression, Not, Or, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, toPrettySQL, GeneratedColumn, IdentityColumn, ResolveDefaultColumns, ResolveTableConstraints, V2ExpressionBuilder}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Dependency, DependencyList, Identifier, StagingTableCatalog, SupportsDeleteV2, SupportsNamespaces, SupportsPartitionManagement, SupportsWrite, TableCapability, TableCatalog, TableSummary, TruncatableTable, V1Table, V1ViewInfo, ViewCatalog}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.index.SupportsIndex
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.connector.read.{FileScan => ConnectorFileScan, FileSet, LocalScan}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream, SupportsRealTimeMode}
import org.apache.spark.sql.connector.write.{V1Write, Write}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{FilterExec, InSubqueryExec, LeafExecNode, LocalTableScanExec, ProjectExec, RowDataSourceScanExec, ScalarSubquery => ExecScalarSubquery, SparkPlan, SparkStrategy => Strategy, UnionExec}
import org.apache.spark.sql.execution.command.{CommandUtils, MetricViewHelper}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileFormat, FileSourceStrategy, HadoopFsRelation, LogicalRelation, LogicalRelationWithTable, PushableColumnAndNestedColumn}
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.metricview.logical.CreateMetricView
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SparkStringUtils

class DataSourceV2Strategy(session: SparkSession) extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private def cacheManager = session.sharedState.cacheManager

  private def hadoopConf = session.sessionState.newHadoopConf()

  // recaches all cache entries without time travel for the given table
  // after a write operation that moves the state of the table forward (e.g. append, overwrite)
  // this method accounts for V2 tables loaded via TableProvider (no catalog/identifier)
  private def refreshCache(r: DataSourceV2Relation)(): Unit = r match {
    case ExtractV2CatalogAndIdentifier(catalog, ident) =>
      val nameParts = ident.toQualifiedNameParts(catalog)
      cacheManager.recacheTableOrView(session, nameParts, includeTimeTravel = false)
    case _ =>
      cacheManager.recacheByPlan(session, r)
  }

  private def recacheTable(r: ResolvedTable, includeTimeTravel: Boolean)(): Unit = {
    val nameParts = r.identifier.toQualifiedNameParts(r.catalog)
    cacheManager.recacheTableOrView(session, nameParts, includeTimeTravel)
  }

  // Invalidates the cache associated with the given table. If the invalidated cache matches the
  // given table, the cache's storage level is returned.
  private def invalidateTableCache(r: ResolvedTable)(): Option[StorageLevel] = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    val cache = cacheManager.lookupCachedData(session, v2Relation)
    invalidateCache(r.catalog, r.identifier)
    if (cache.isDefined) {
      val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel
      Some(cacheLevel)
    } else {
      None
    }
  }

  private def invalidateCache(catalog: TableCatalog, ident: Identifier): Unit = {
    val nameParts = ident.toQualifiedNameParts(catalog)
    cacheManager.uncacheTableOrView(session, nameParts, cascade = true)
  }

  private def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(session.sharedState.conf.get(WAREHOUSE_PATH),
      location, session.sharedState.hadoopConf)
  }

  // Strategy cases that target v2 views read `ResolvedPersistentView.info` directly. For
  // session-catalog (v1) views the payload is a `V1ViewInfo` wrapping the original
  // `CatalogTable`; v2 catalogs supply a regular `ViewInfo` from the catalog.
  // `ResolveSessionCatalog` rewrites session-catalog views to v1 commands before this strategy
  // fires, so v2 cases that don't expect a `V1ViewInfo` won't see one.

  private def qualifyLocInTableSpec(tableSpec: TableSpec): TableSpec = {
    val newLoc = tableSpec.location.map { loc =>
      val locationUri = CatalogUtils.stringToURI(loc)
      val qualified = if (locationUri.isAbsolute) {
        locationUri
      } else if (new Path(locationUri).isAbsolute) {
        CatalogUtils.makeQualifiedPath(locationUri, hadoopConf)
      } else {
        // Leave it to the catalog implementation to qualify relative paths.
        locationUri
      }
      CatalogUtils.URIToString(qualified)
    }
    tableSpec.withNewLocation(newLoc)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, ExtractV2ScanInfo(
      v2Relation, V1ScanWrapper(scan, pushed, pushedDownOperators), output)) =>
      val v1Relation = scan.toV1TableScan[BaseRelation with TableScan](session.sqlContext)
      if (v1Relation.schema != scan.readSchema()) {
        throw QueryExecutionErrors.fallbackV1RelationReportsInconsistentSchemaError(
          scan.readSchema(), v1Relation.schema)
      }
      val rdd = v1Relation.buildScan()
      val unsafeRowRDD = DataSourceStrategy.toCatalystRDD(v1Relation, output, rdd)

      val catalogName = v2Relation.catalog.map(_.name())
      val tableIdentifier = v2Relation.identifier.flatMap(_.asTableIdentifierOpt(catalogName))

      val dsScan = RowDataSourceScanExec(
        output,
        output.toStructType,
        Set.empty,
        pushed.toSet,
        pushedDownOperators,
        unsafeRowRDD,
        v1Relation,
        None,
        tableIdentifier)
      DataSourceV2Strategy.withProjectAndFilter(
        project, filters, dsScan, needsUnsafeConversion = false) :: Nil

    case PhysicalOperation(project, filters,
        ExtractV2ScanInfo(_, scan: LocalScan, output)) =>
      val localScanExec = LocalTableScanExec(output, scan.rows().toImmutableArraySeq, None)
      DataSourceV2Strategy.withProjectAndFilter(
        project, filters, localScanExec, needsUnsafeConversion = false) :: Nil

    // FileScan: bridge a DSv2 scan back to the V1 file-source pipeline so the V1-only
    // execution paths (FileSourceScanExec, vectorized file readers, file-source optimizer
    // and planner rules) light up. The scan exposes its file structure (FileIndex +
    // FileFormat + partition/data columns) via the FileBatch / FileSet shape; we rebuild a
    // HadoopFsRelation, wrap it in a LogicalRelation, and delegate to FileSourceStrategy.
    // Matches before the generic DataSourceV2ScanRelation arm below.
    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation)
        if relation.scan.isInstanceOf[ConnectorFileScan] =>
      DataSourceV2Strategy.planFileScan(
        session, relation.relation.schema, relation.scan.asInstanceOf[ConnectorFileScan],
        project, filters, relation.output)

    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val (dynamicFilters, postScanFilters) = filters.partition {
        case _: DynamicPruning => true
        case _ => false
      }

      // Extract scalar subquery filters on runtime-filterable columns for runtime pushdown.
      // These filters stay in postScanFilters for correctness (FilterExec above scan),
      // but are also routed into runtimeFilters so BatchScanExec can use them for
      // partition pruning via SupportsRuntimeV2Filtering.filter().
      val scalarSubqueryFilters = if (relation.runtimeFilterAttrs.nonEmpty) {
        postScanFilters.filter { f =>
          f.containsPattern(SCALAR_SUBQUERY) &&
            f.references.nonEmpty &&
            f.references.subsetOf(relation.runtimeFilterAttrs)
        }
      } else {
        Seq.empty
      }
      val runtimeFilters = dynamicFilters ++ scalarSubqueryFilters

      val batchExec = BatchScanExec(relation.output, relation.scan, runtimeFilters,
        relation.ordering, relation.relation.table, relation.keyGroupedPartitioning)
      DataSourceV2Strategy.withProjectAndFilter(
        project, postScanFilters, batchExec, !batchExec.supportsColumnar) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2ScanRelation)
      if r.startOffset.isDefined && r.endOffset.isDefined =>

      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)

      // Add a Project here to make sure we produce unsafe rows.
      DataSourceV2Strategy.withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2ScanRelation)
      if r.startOffset.isDefined && r.endOffset.isEmpty =>

        val scanExec = if (r.relation.realTimeModeDuration.isDefined) {
          if (!r.stream.isInstanceOf[SupportsRealTimeMode]) {
            throw new SparkIllegalArgumentException(
              errorClass = "STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED",
              messageParameters = Map("className" -> r.stream.getClass.getName)
            )
          }
          val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
          new RealTimeStreamScanExec(
            r.output,
            r.scan,
            microBatchStream,
            r.startOffset.get,
            r.relation.realTimeModeDuration.get
          )
        } else {
          val continuousStream = r.stream.asInstanceOf[ContinuousStream]
          val s = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)
          // initialize partitions
          s.inputPartitions
          s
        }

      // Add a Project here to make sure we produce unsafe rows.
      DataSourceV2Strategy.withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar) :: Nil

    case WriteToDataSourceV2(relationOpt, writer, query, customMetrics) =>
      val invalidateCacheFunc: () => Unit = () => relationOpt match {
        case Some(r) => session.sharedState.cacheManager.uncacheQuery(session, r, cascade = true)
        case None => ()
      }
      WriteToDataSourceV2Exec(writer, invalidateCacheFunc, planLater(query), customMetrics) :: Nil

    case c @ CreateTable(ResolvedIdentifier(catalog, ident), columns, partitioning,
        tableSpec: TableSpec, ifNotExists) =>
      val tableCatalog = catalog.asTableCatalog
      ResolveDefaultColumns.validateCatalogForDefaultValue(columns, tableCatalog, ident)
      ResolveTableConstraints.validateCatalogForTableConstraint(
        tableSpec.constraints, tableCatalog, ident)
      val statementType = "CREATE TABLE"
      GeneratedColumn.validateCatalogForGeneratedColumn(columns, tableCatalog, ident)
      IdentityColumn.validateIdentityColumn(c.tableSchema, tableCatalog, ident)

      CreateTableExec(
        catalog.asTableCatalog,
        ident,
        columns.map(_.toV2Column(statementType)).toArray,
        partitioning,
        qualifyLocInTableSpec(tableSpec),
        ifNotExists) :: Nil

    case CreateTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
        options, ifNotExists, true) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(staging, ident, parts, query,
            qualifyLocInTableSpec(tableSpec), options, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(catalog.asTableCatalog, ident, parts, query,
            qualifyLocInTableSpec(tableSpec), options, ifNotExists) :: Nil
      }

    // CREATE TABLE ... LIKE ... for a v2 catalog target.
    // Source is an already-resolved Table object; no extra catalog round-trip is needed.
    // Views are wrapped in V1Table so the exec can extract schema and provider uniformly --
    // session-catalog (v1) views unwrap to their original `CatalogTable`; non-session v2
    // views go through `V1Table.toCatalogTable` to synthesize an equivalent `CatalogTable`
    // from the resolved `ViewInfo`.
    case CreateTableLike(
        ResolvedIdentifier(catalog, ident), source,
        locationStr, provider, serdeInfo, properties, ifNotExists) =>
      val table = source match {
        case ResolvedTable(_, _, t, _) => t
        case ResolvedPersistentView(_, _, info: V1ViewInfo) => V1Table(info.v1Table)
        case rpv @ ResolvedPersistentView(viewCatalog, viewIdent, _) =>
          V1Table(V1Table.toCatalogTable(viewCatalog, viewIdent, rpv.info))
        case ResolvedTempView(_, meta) => V1Table(meta)
      }
      val location = locationStr.map { loc =>
        val uri = CatalogUtils.stringToURI(loc)
        if (uri.isAbsolute) uri
        else if (new Path(uri).isAbsolute) CatalogUtils.makeQualifiedPath(uri, hadoopConf)
        else uri
      }
      CreateTableLikeExec(catalog.asTableCatalog, ident, table,
        location, provider, serdeInfo, properties, ifNotExists) :: Nil

    case RefreshTable(r: ResolvedTable) =>
      RefreshTableExec(r.catalog, r.identifier, recacheTable(r, includeTimeTravel = true)) :: Nil

    case c @ ReplaceTable(
        ResolvedIdentifier(catalog, ident), columns, parts, tableSpec: TableSpec, orCreate) =>
      val tableCatalog = catalog.asTableCatalog
      ResolveDefaultColumns.validateCatalogForDefaultValue(columns, tableCatalog, ident)
      ResolveTableConstraints.validateCatalogForTableConstraint(
        tableSpec.constraints, tableCatalog, ident)
      val statementType = "REPLACE TABLE"
      GeneratedColumn.validateCatalogForGeneratedColumn(columns, tableCatalog, ident)
      IdentityColumn.validateIdentityColumn(c.tableSchema, tableCatalog, ident)

      val v2Columns = columns.map(_.toV2Column(statementType)).toArray
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(staging, ident, v2Columns, parts,
            qualifyLocInTableSpec(tableSpec), orCreate = orCreate, invalidateCache) :: Nil
        case _ =>
          ReplaceTableExec(tableCatalog, ident, v2Columns, parts,
            qualifyLocInTableSpec(tableSpec), orCreate = orCreate, invalidateCache) :: Nil
      }

    // CheckViewReferences guarantees the catalog is a ViewCatalog by the time these strategy
    // cases fire (it throws MISSING_CATALOG_ABILITY.VIEWS otherwise).
    case CreateView(ResolvedIdentifier(catalog, ident), userSpecifiedColumns, comment,
        collation, properties, originalText, child, allowExisting, replace, viewSchemaMode,
        _, _) =>
      val sqlText = originalText.getOrElse {
        throw QueryCompilationErrors.createPersistedViewFromDatasetAPINotAllowedError()
      }
      CreateV2ViewExec(catalog.asInstanceOf[ViewCatalog], ident, userSpecifiedColumns, comment,
        collation, properties, sqlText, child, allowExisting, replace, viewSchemaMode) :: Nil

    // CREATE VIEW ... WITH METRICS on a non-session v2 catalog. Routes the metric-view path
    // through `CreateV2MetricViewExec`, which extends `V2ViewPreparation` to share the
    // `IF NOT EXISTS` short-circuit, `OR REPLACE`, and cross-type-collision decoding with
    // `CreateV2ViewExec`. Session-catalog dispatch happens earlier in `ResolveSessionCatalog`,
    // which rewrites `CreateMetricView` (the parser's v1/v2-agnostic logical plan) to
    // `CreateMetricViewCommand` for v1 execution.
    case CreateMetricView(
        ResolvedIdentifier(catalog, ident), userSpecifiedColumns, comment, properties,
        originalText, allowExisting, replace) if !CatalogV2Util.isSessionCatalog(catalog) =>
      val viewCatalog = catalog match {
        case vc: ViewCatalog => vc
        case _ => throw QueryCompilationErrors.missingCatalogViewsAbilityError(catalog)
      }
      // Parse + analyze the YAML body here (during planning). This mirrors the v1 path's
      // late analysis in `CreateMetricViewCommand.run` -- the metric-view source plan is not
      // a SQL string, so it can't ride along as a regular `query` `LogicalPlan` field on the
      // logical command the way `CreateView` does. Pass the full multi-part name so v2 metric
      // views with multi-level-namespace targets analyze correctly (`asTableIdentifier` would
      // throw `requiresSinglePartNamespaceError` for namespace arity > 1).
      val nameParts = (catalog.name() +: ident.namespace().toIndexedSeq) :+ ident.name()
      val (analyzed, metricView) = MetricViewHelper.analyzeMetricViewText(
        session, nameParts, originalText)
      val mergedProps = properties ++ metricView.getProperties
      val depParts = MetricViewHelper.collectTableDependencies(analyzed)
      // Always emit a `Some(DependencyList)` for metric views (even when `depParts` is empty,
      // e.g. `SQLSource("SELECT 1 AS x")`): per `DependencyList`'s contract, `null` means
      // "no dependency list was supplied" while an empty list means "supplied but the
      // object has none". Metric-view CREATE always *computes* deps, so the right empty
      // representation is `Some(empty list)`, not `None`.
      val sparkDeps: Array[Dependency] =
        depParts.map(parts => Dependency.table(parts.toArray): Dependency).toArray
      val deps = Some(DependencyList.of(sparkDeps))
      CreateV2MetricViewExec(viewCatalog, ident, userSpecifiedColumns, comment, mergedProps,
        originalText, analyzed, allowExisting, replace, deps) :: Nil

    case AlterViewAs(rpv @ ResolvedPersistentView(catalog, ident, _),
        originalText, query, _, _) =>
      AlterV2ViewExec(catalog.asInstanceOf[ViewCatalog], ident, rpv.info,
        originalText, query) :: Nil

    // View DDL / inspection on a non-session v2 catalog that the v1 rewrite in
    // `ResolveSessionCatalog` can't handle (its `ResolvedViewIdentifier` matcher is gated on
    // `isSessionCatalog`). Routed to dedicated v2 execs that read the typed `ViewInfo`
    // resolved at analysis time directly from `ResolvedPersistentView.info` -- no re-loading
    // at exec time.
    case SetViewProperties(rpv @ ResolvedPersistentView(catalog, ident, _), props) =>
      AlterV2ViewSetPropertiesExec(
        catalog.asInstanceOf[ViewCatalog], ident, rpv.info, props) :: Nil

    case UnsetViewProperties(rpv @ ResolvedPersistentView(catalog, ident, _), keys, _) =>
      AlterV2ViewUnsetPropertiesExec(
        catalog.asInstanceOf[ViewCatalog], ident, rpv.info, keys) :: Nil

    case AlterViewSchemaBinding(rpv @ ResolvedPersistentView(catalog, ident, _), schemaMode) =>
      AlterV2ViewSchemaBindingExec(
        catalog.asInstanceOf[ViewCatalog], ident, rpv.info, schemaMode) :: Nil

    case RenameTable(ResolvedPersistentView(catalog, ident, _), newName, isView) =>
      // Reject `ALTER TABLE <view> RENAME TO ...` -- the syntax says TABLE, but the resolved
      // child is a view. Matches the v1 runtime check in `DDLUtils.verifyAlterTableType`.
      if (!isView) {
        throw QueryCompilationErrors.cannotAlterViewWithAlterTableError(ident.name())
      }
      RenameV2ViewExec(
        catalog.asInstanceOf[ViewCatalog], ident, newName.asIdentifier) :: Nil

    case ShowCreateTable(rpv @ ResolvedPersistentView(catalog, ident, _), _, _)
        if rpv.info.properties.get(TableCatalog.PROP_TABLE_TYPE) ==
          TableSummary.METRIC_VIEW_TABLE_TYPE =>
      // SHOW CREATE TABLE on a metric view is explicitly unsupported: `ShowCreateV2ViewExec`
      // would emit a plain `CREATE VIEW <ident> AS <yaml>`, which is not a round-trippable
      // metric-view DDL form (the right form is `CREATE VIEW <ident> WITH METRICS LANGUAGE
      // YAML AS $$ <yaml> $$`). Reject up front with the same dedicated error class the v1
      // path uses (`UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW`) so users see the same
      // actionable message regardless of catalog kind.
      val quoted = (catalog.name() +: ident.asMultipartIdentifier)
        .map(quoteIfNeeded).mkString(".")
      throw QueryCompilationErrors.showCreateTableNotSupportedOnMetricViewError(quoted)

    case ShowCreateTable(rpv @ ResolvedPersistentView(catalog, ident, _), _, output) =>
      val quoted = (catalog.name() +: ident.asMultipartIdentifier).map(quoteIfNeeded).mkString(".")
      ShowCreateV2ViewExec(output, quoted, rpv.info) :: Nil

    case ShowTableProperties(rpv @ ResolvedPersistentView(catalog, ident, _),
        propertyKey, output) =>
      val quoted = (catalog.name() +: ident.asMultipartIdentifier).map(quoteIfNeeded).mkString(".")
      ShowV2ViewPropertiesExec(output, quoted, rpv.info, propertyKey) :: Nil

    case ShowColumns(rpv @ ResolvedPersistentView(_, ident, _), ns, output) =>
      // If `SHOW COLUMNS IN <view> FROM <ns>` was written with both the view's namespace and
      // an explicit `FROM <ns>`, validate they agree -- mirrors the v1 rewrite in
      // `ResolveSessionCatalog`. For multi-level v2 namespaces we compare the full namespace
      // sequence (case-insensitively) rather than v1's single-part `database` check.
      ns.foreach { nsSeq =>
        val resolver = session.sessionState.conf.resolver
        val viewNs = ident.namespace().toSeq
        val mismatch = viewNs.length != nsSeq.length ||
          viewNs.zip(nsSeq).exists { case (a, b) => !resolver(a, b) }
        if (mismatch) {
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(nsSeq, viewNs)
        }
      }
      ShowV2ViewColumnsExec(output, rpv.info) :: Nil

    case DescribeRelation(rpv @ ResolvedPersistentView(catalog, ident, _), isExtended, output) =>
      DescribeV2ViewExec(output, catalog.name(), ident, rpv.info, isExtended) :: Nil

    case DescribeColumn(rpv @ ResolvedPersistentView(_, _, _), column, isExtended, output) =>
      // `ResolvedPersistentView.output` exposes the view's schema, so `ResolveReferences`
      // resolves the column against it -- meaning we typically receive an `Attribute` here.
      // Accept the legacy `UnresolvedAttribute` form too. The unwrap logic is shared with the
      // v1 rewrite for session-catalog views in `ResolveSessionCatalog`.
      DescribeV2ViewColumnExec(
        output, rpv.info, DescribeColumn.extractColumnNameParts(column), isExtended) :: Nil

    // Plans that resolve through `UnresolvedTableOrView` reach here with a
    // `ResolvedPersistentView` child for non-session v2 views (the v1 rewrite in
    // `ResolveSessionCatalog` no longer matches them because `ResolvedViewIdentifier` is gated
    // on `isSessionCatalog`). Pin each with `UNSUPPORTED_FEATURE.TABLE_OPERATION` so users get
    // a clean `AnalysisException` instead of a generic "No plan for ..." assertion from the
    // planner. Tracked for follow-up real handlers in SPARK-52729.
    case RefreshTable(ResolvedPersistentView(catalog, ident, _)) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "REFRESH TABLE")

    case AnalyzeTable(ResolvedPersistentView(catalog, ident, _), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ANALYZE TABLE")

    case AnalyzeColumn(ResolvedPersistentView(catalog, ident, _), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ANALYZE TABLE ... FOR COLUMNS")

    // SHOW PARTITIONS on a view is already rejected during analysis: the parser uses
    // `UnresolvedTable` (not `UnresolvedTableOrView`), so `CheckAnalysis` surfaces
    // `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` before planning. No strategy case needed.

    // DROP VIEW on a non-session ViewCatalog. The v1 rewrite in `ResolveSessionCatalog` skips
    // ViewCatalog catalogs, so they fall through here. `DropViewExec` calls
    // `ViewCatalog.dropView` and surfaces `EXPECT_VIEW_NOT_TABLE` if the identifier resolves to
    // a table in a mixed catalog.
    case DropView(r @ ResolvedIdentifier(catalog: ViewCatalog, ident), ifExists) =>
      val invalidateFunc = () => CommandUtils.uncacheTableOrView(session, r)
      DropViewExec(catalog, ident, ifExists, invalidateFunc) :: Nil

    case ReplaceTableAsSelect(ResolvedIdentifier(catalog, ident),
        parts, query, tableSpec: TableSpec, options, orCreate, true) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            qualifyLocInTableSpec(tableSpec),
            options,
            orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog.asTableCatalog,
            ident,
            parts,
            query,
            qualifyLocInTableSpec(tableSpec),
            options,
            orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case AppendWrite(r @ ExtractV2Table(v1: SupportsWrite), Some(write), analyzedQuery)
        if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          assert(analyzedQuery.isDefined)
          AppendDataExecV1(v1, analyzedQuery.get, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case AppendData(r: DataSourceV2Relation, query, _, _, _, Some(write), _) =>
      AppendDataExec(planLater(query), refreshCache(r), write, r.name) :: Nil

    case InsertOnlyMerge(r: DataSourceV2Relation, query, Some(write), _) =>
      InsertOnlyMergeExec(planLater(query), refreshCache(r), write, r.name) :: Nil

    case OverwriteByExpression(r @ ExtractV2Table(v1: SupportsWrite), _, _,
        _, _, _, Some(write), analyzedQuery) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          assert(analyzedQuery.isDefined)
          OverwriteByExpressionExecV1(v1, analyzedQuery.get, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case OverwriteByExpression(
        r: DataSourceV2Relation, _, query, _, _, _, Some(write), _) =>
      OverwriteByExpressionExec(planLater(query), refreshCache(r), write, r.name) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, _, _, _, Some(write)) =>
      OverwritePartitionsDynamicExec(planLater(query), refreshCache(r), write, r.name) :: Nil

    case DeleteFromTableWithFilters(r: DataSourceV2Relation, filters) =>
      DeleteFromTableExec(r.table.asDeletable, filters.toArray, refreshCache(r)) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case ExtractV2ScanInfo(r, _, output) =>
          val table = r.table
          if (SubqueryExpression.hasSubquery(condition)) {
            throw QueryCompilationErrors.unsupportedDeleteByConditionWithSubqueryError(condition)
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(Seq(condition), output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => DataSourceV2Strategy.translateFilterV2(f).getOrElse(
                  throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(f))
              }).toArray

          table match {
            case t: SupportsDeleteV2 if t.canDeleteWhere(filters) =>
              DeleteFromTableExec(t, filters, refreshCache(r)) :: Nil
            case t: SupportsDeleteV2 =>
              throw QueryCompilationErrors.cannotDeleteTableWhereFiltersError(t, filters)
            case t: TruncatableTable if condition == TrueLiteral =>
              TruncateTableExec(t, refreshCache(r)) :: Nil
            case _ =>
              throw QueryCompilationErrors.tableDoesNotSupportDeletesError(table)
          }
        case LogicalRelationWithTable(_, Some(catalogTable)) =>
          val tableIdentifier = catalogTable.identifier
          throw QueryCompilationErrors.unsupportedTableOperationError(
            tableIdentifier,
            "DELETE")
        case other =>
          throw SparkException.internalError("Unexpected table relation: " + other)
      }

    case rd @ ReplaceData(_: DataSourceV2Relation, _, query, r: DataSourceV2Relation, projections,
        _, Some(write)) =>
      ReplaceDataExec(
        planLater(query),
        refreshCache(r), // use the original relation to refresh the cache
        projections,
        write,
        rd.operation.command,
        r.name) :: Nil

    case wd @ WriteDelta(_: DataSourceV2Relation, _, query, r: DataSourceV2Relation, projections,
        _, Some(write)) =>
      WriteDeltaExec(
        planLater(query),
        refreshCache(r), // use the original relation to refresh the cache
        projections,
        write,
        wd.operation.command,
        r.name) :: Nil

    case MergeRows(isSourceRowPresent, isTargetRowPresent, matchedInstructions,
        notMatchedInstructions, notMatchedBySourceInstructions, checkCardinality, output, child) =>
      MergeRowsExec(isSourceRowPresent, isTargetRowPresent, matchedInstructions,
        notMatchedInstructions, notMatchedBySourceInstructions, checkCardinality,
        output, planLater(child)) :: Nil

    case WriteToContinuousDataSource(writer, query, customMetrics) =>
      WriteToContinuousDataSourceExec(writer, planLater(query), customMetrics) :: Nil

    case DescribeNamespace(ResolvedNamespace(catalog, ns, _), extended, output) =>
      DescribeNamespaceExec(output, catalog.asNamespaceCatalog, ns, extended) :: Nil

    case DescribeRelation(r: ResolvedTable, isExtended, output) =>
      DescribeTableExec(output, r.catalog.name(), r.identifier, r.table, isExtended) :: Nil

    case DescribeTablePartition(r: ResolvedTable, part, isExtended, output) =>
      DescribeTablePartitionExec(output, r.table.asPartitionable, r.identifier,
        Seq(part).asResolvedPartitionSpecs.head, isExtended) :: Nil

    case DescribeColumn(r: ResolvedTable, column, isExtended, output) =>
      column match {
        case c: Attribute =>
          DescribeColumnExec(output, c, isExtended, r.table) :: Nil
        case nested =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(nested))
      }

    case DropTable(r: ResolvedIdentifier, ifExists, purge) =>
      val invalidateFunc = () => CommandUtils.uncacheTableOrView(session, r)
      DropTableExec(
        r.catalog.asTableCatalog, r.identifier, ifExists, purge, invalidateFunc) :: Nil

    case _: NoopCommand =>
      LocalTableScanExec(Nil, Nil, None) :: Nil

    case RenameTable(r @ ResolvedTable(catalog, oldIdent, _, _), newIdent, isView) =>
      if (isView) {
        throw QueryCompilationErrors.cannotRenameTableWithAlterViewError()
      }
      RenameTableExec(
        catalog,
        oldIdent,
        newIdent.asIdentifier,
        invalidateTableCache(r),
        session.sharedState.cacheManager.cacheQuery) :: Nil

    case SetNamespaceProperties(ResolvedNamespace(catalog, ns, _), properties) =>
      AlterNamespaceSetPropertiesExec(catalog.asNamespaceCatalog, ns, properties) :: Nil

    case SetNamespaceLocation(ResolvedNamespace(catalog, ns, _), location) =>
      if (SparkStringUtils.isBlank(location)) {
        throw QueryExecutionErrors.invalidEmptyLocationError(location)
      }
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_LOCATION -> makeQualifiedDBObjectPath(location))) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, ns, _), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CreateNamespace(ResolvedNamespace(catalog, ns, _), ifNotExists, properties) =>
      val location = properties.get(SupportsNamespaces.PROP_LOCATION)
      if (location.exists(SparkStringUtils.isBlank)) {
        throw QueryExecutionErrors.invalidEmptyLocationError(location.get)
      }
      val finalProperties = properties.get(SupportsNamespaces.PROP_LOCATION).map { loc =>
        properties + (SupportsNamespaces.PROP_LOCATION -> makeQualifiedDBObjectPath(loc))
      }.getOrElse(properties)
      CreateNamespaceExec(catalog.asNamespaceCatalog, ns, ifNotExists, finalProperties) :: Nil

    case DropNamespace(ResolvedNamespace(catalog, ns, _), ifExists, cascade) =>
      DropNamespaceExec(catalog, ns, ifExists, cascade) :: Nil

    case ShowTables(ResolvedNamespace(catalog, ns, _), pattern, output) =>
      ShowTablesExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    // SHOW VIEWS on a v2 ViewCatalog. `ResolveSessionCatalog` rewrites the SHOW VIEWS plan to
    // v1 `ShowViewsCommand` only when the catalog is NOT a `ViewCatalog`; non-`ViewCatalog`
    // catalogs (session or not) are rejected with `MISSING_CATALOG_ABILITY.VIEWS` there. So
    // this case sees `ViewCatalog` catalogs (typically non-session, since the default
    // `V2SessionCatalog` is not a `ViewCatalog`; a session-catalog override that mixes in
    // `ViewCatalog` would also reach here).
    case ShowViews(ResolvedNamespace(catalog: ViewCatalog, ns, _), pattern, output) =>
      ShowViewsExec(output, catalog, ns, pattern) :: Nil

    case ShowTablesExtended(
        ResolvedNamespace(catalog, ns, _),
        pattern,
        output) =>
      ShowTablesExtendedExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    case ShowTablePartition(r: ResolvedTable, part, output) =>
      ShowTablePartitionExec(output, r.catalog, r.identifier,
        r.table.asPartitionable, Seq(part).asResolvedPartitionSpecs.head) :: Nil

    case SetCatalogAndNamespace(ResolvedNamespace(catalog, ns, _)) =>
      val catalogManager = session.sessionState.catalogManager
      val namespace = if (ns.nonEmpty) Some(ns) else None
      SetCatalogAndNamespaceExec(catalogManager, Some(catalog.name()), namespace) :: Nil

    case ShowTableProperties(rt: ResolvedTable, propertyKey, output) =>
      ShowTablePropertiesExec(output, rt.table, rt.name, propertyKey) :: Nil

    case AnalyzeTable(_: ResolvedTable, _, _) | AnalyzeColumn(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.analyzeTableNotSupportedForV2TablesError()

    case AddPartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), parts, ignoreIfExists) =>
      AddPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfExists,
        recacheTable(r, includeTimeTravel = false)) :: Nil

    case DropPartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _),
        parts,
        ignoreIfNotExists,
        purge) =>
      DropPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfNotExists,
        purge,
        recacheTable(r, includeTimeTravel = false)) :: Nil

    case RenamePartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), from, to) =>
      RenamePartitionExec(
        table,
        Seq(from).asResolvedPartitionSpecs.head,
        Seq(to).asResolvedPartitionSpecs.head,
        recacheTable(r, includeTimeTravel = false)) :: Nil

    case RecoverPartitions(_: ResolvedTable) =>
      throw QueryCompilationErrors.alterTableRecoverPartitionsNotSupportedForV2TablesError()

    case SetTableSerDeProperties(_: ResolvedTable, _, _, _) =>
      throw QueryCompilationErrors.alterTableSerDePropertiesNotSupportedForV2TablesError()

    case LoadData(_: ResolvedTable, _, _, _, _) =>
      throw QueryCompilationErrors.loadDataNotSupportedForV2TablesError()

    case ShowCreateTable(rt: ResolvedTable, asSerde, output) =>
      if (asSerde) {
        throw QueryCompilationErrors.showCreateTableAsSerdeNotSupportedForV2TablesError()
      }
      ShowCreateTableExec(output, rt) :: Nil

    case TruncateTable(r: ResolvedTable) =>
      TruncateTableExec(
        r.table.asTruncatable,
        recacheTable(r, includeTimeTravel = false)) :: Nil

    case TruncatePartition(r: ResolvedTable, part) =>
      TruncatePartitionExec(
        r.table.asPartitionable,
        Seq(part).asResolvedPartitionSpecs.head,
        recacheTable(r, includeTimeTravel = false)) :: Nil

    case ShowColumns(resolvedTable: ResolvedTable, ns, output) =>
      ns match {
        case Some(namespace) =>
          val tableNamespace = resolvedTable.identifier.namespace()
          if (namespace.length != tableNamespace.length ||
            !namespace.zip(tableNamespace).forall(SQLConf.get.resolver.tupled)) {
            throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
              namespace, tableNamespace.toSeq)
          }
        case _ =>
      }
      ShowColumnsExec(output, resolvedTable) :: Nil

    case r @ ShowPartitions(
        ResolvedTable(catalog, _, table: SupportsPartitionManagement, _),
        pattern @ (None | Some(_: ResolvedPartitionSpec)), output) =>
      ShowPartitionsExec(
        output,
        catalog,
        table,
        pattern.map(_.asInstanceOf[ResolvedPartitionSpec])) :: Nil

    case RepairTable(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.repairTableNotSupportedForV2TablesError()

    case r: CacheTable =>
      CacheTableExec(r.table, r.multipartIdentifier, r.isLazy, r.options) :: Nil

    case r: CacheTableAsSelect =>
      CacheTableAsSelectExec(
        r.tempViewNameString, r.plan, r.originalText, r.isLazy, r.options,
        r.referredTempFunctions) :: Nil

    case r: UncacheTable =>
      def isTempView(table: LogicalPlan): Boolean = table match {
        case SubqueryAlias(_, v: View) => v.isTempView
        case _ => false
      }
      UncacheTableExec(r.table, cascade = !isTempView(r.table)) :: Nil

    case a @ AddCheckConstraint(PhysicalOperation(_, _, d: DataSourceV2ScanRelation), check) =>
      assert(d.relation.catalog.isDefined, "Catalog should be defined after analysis")
      assert(d.relation.identifier.isDefined, "Identifier should be defined after analysis")
      val catalog = d.relation.catalog.get.asTableCatalog
      val ident = d.relation.identifier.get
      val condition = a.checkConstraint.condition
      val change = TableChange.addConstraint(
        check.toV2Constraint,
        d.relation.table.version)
      ResolveTableConstraints.validateCatalogForTableChange(Seq(change), catalog, ident)
      AddCheckConstraintExec(catalog, ident, change, condition, planLater(a.child)) :: Nil

    case a: AlterTableCommand if a.table.resolved =>
      val table = a.table.asInstanceOf[ResolvedTable]
      ResolveTableConstraints.validateCatalogForTableChange(
        a.changes, table.catalog, table.identifier)
      AlterTableExec(
        table.catalog,
        table.identifier,
        a.changes,
        recacheTable(table, includeTimeTravel = false)) :: Nil

    case CreateIndex(ResolvedTable(_, _, table, _),
        indexName, indexType, ifNotExists, columns, properties) =>
      table match {
        case s: SupportsIndex =>
          val namedRefs = columns.map { case (field, prop) =>
            FieldReference(field.name) -> prop
          }
          CreateIndexExec(s, indexName, indexType, ifNotExists, namedRefs, properties) :: Nil
        case _ => throw QueryCompilationErrors.tableIndexNotSupportedError(
          s"CreateIndex is not supported in this table ${table.name}.")
      }

    case DropIndex(ResolvedTable(_, _, table, _), indexName, ifNotExists) =>
      table match {
        case s: SupportsIndex =>
          DropIndexExec(s, indexName, ifNotExists) :: Nil
        case _ => throw QueryCompilationErrors.tableIndexNotSupportedError(
          s"DropIndex is not supported in this table ${table.name}.")
      }

    case ShowFunctions(
      ResolvedNamespace(catalog, ns, _), userScope, systemScope, pattern, output) =>
      ShowFunctionsExec(
        output,
        catalog.asFunctionCatalog,
        ns,
        userScope,
        systemScope,
        pattern) :: Nil

    case c: Call =>
      ExplainOnlySparkPlan(c) :: Nil

    case _ => Nil
  }
}

/**
 * Pattern that matches either an [[AppendData]] or an [[InsertOnlyMerge]] and exposes the
 * fields needed to plan the v1 batch-write fallback path.
 */
private object AppendWrite {
  def unapply(
      plan: LogicalPlan
  ): Option[(NamedRelation, Option[Write], Option[LogicalPlan])] = plan match {
    case a: AppendData => Some((a.table, a.write, a.analyzedQuery))
    case m: InsertOnlyMerge => Some((m.table, m.write, m.analyzedQuery))
    case _ => None
  }
}

private[sql] object DataSourceV2Strategy extends Logging {

  private def translateLeafNodeFilterV2(predicate: Expression): Option[Predicate] = {
    predicate match {
      case PushablePredicate(expr) => Some(expr)
      case _ => None
    }
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2(predicate: Expression): Option[Predicate] = {
    translateFilterV2WithMapping(predicate, None)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @param predicate The input [[Expression]] to be translated as [[Filter]]
   * @param translatedFilterToExpr An optional map from leaf node filter expressions to its
   *                               translated [[Filter]]. The map is used for rebuilding
   *                               [[Expression]] from [[Filter]].
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2WithMapping(
      predicate: Expression,
      translatedFilterToExpr: Option[mutable.HashMap[Predicate, Expression]])
  : Option[Predicate] = {
    predicate match {
      case And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield new V2And(leftFilter, rightFilter)

      case Or(left, right) =>
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield new V2Or(leftFilter, rightFilter)

      case Not(child) =>
        translateFilterV2WithMapping(child, translatedFilterToExpr).map(new V2Not(_))

      case other =>
        val filter = translateLeafNodeFilterV2(other)
        if (filter.isDefined && translatedFilterToExpr.isDefined) {
          translatedFilterToExpr.get(filter.get) = predicate
        }
        filter
    }
  }

  protected[sql] def rebuildExpressionFromFilter(
      predicate: Predicate,
      translatedFilterToExpr: mutable.HashMap[Predicate, Expression]): Expression = {
    predicate match {
      case and: V2And =>
        expressions.And(
          rebuildExpressionFromFilter(and.left(), translatedFilterToExpr),
          rebuildExpressionFromFilter(and.right(), translatedFilterToExpr))
      case or: V2Or =>
        expressions.Or(
          rebuildExpressionFromFilter(or.left(), translatedFilterToExpr),
          rebuildExpressionFromFilter(or.right(), translatedFilterToExpr))
      case not: V2Not =>
        expressions.Not(rebuildExpressionFromFilter(not.child(), translatedFilterToExpr))
      case _ =>
        translatedFilterToExpr.getOrElse(predicate,
          throw SparkException.internalError(
            "Failed to rebuild Expression for filter: " + predicate))
    }
  }

  /**
   * Translates a runtime filter into a data source v2 Predicate.
   *
   * Runtime filters usually contain a subquery that must be evaluated before the translation.
   * If the underlying subquery hasn't completed yet, this method will throw an exception.
   */
  protected[sql] def translateRuntimeFilterV2(expr: Expression): Option[Predicate] = expr match {
    case in @ InSubqueryExec(PushableColumnAndNestedColumn(name), _, _, _, _, _) =>
      val values = in.values().getOrElse {
        throw SparkException.internalError(
          s"Can't translate $in to v2 Predicate, no subquery result")
      }
      val literals = values.map(LiteralValue(_, in.child.dataType))
      Some(new Predicate("IN", FieldReference(name) +: literals))

    case other =>
      logWarning(log"Can't translate ${MDC(EXPR, other)} to source filter, unsupported expression")
      None
  }

  /**
   * Literalizes scalar subqueries in the given expression and translates the result to a V2
   * [[Predicate]]. Used at runtime in [[BatchScanExec]] after scalar subqueries have been
   * evaluated.
   */
  protected[sql] def translateScalarSubqueryFilterV2(
      expr: Expression): Option[Predicate] = {
    val literalized = expr.transform {
      case s: ExecScalarSubquery => s.toLiteral
    }
    translateFilterV2(literalized)
  }


  /**
   * Rewrites a [[DataSourceV2ScanRelation]] whose scan is a [[ConnectorFileScan]] into V1
   * [[LogicalRelation]]s backed by [[HadoopFsRelation]]s (one per [[FileSet]]) and re-plans each
   * through [[FileSourceStrategy]]. This unlocks the V1 file-source execution path
   * (`FileSourceScanExec`, vectorized readers, file-source planner rules) for DSv2 connectors
   * that can expose their file layout.
   *
   * A [[ConnectorFileScan]] may return more than one [[FileSet]] to model a hybrid scan; the
   * resulting per-set plans are combined with a [[UnionExec]]. All file sets are expected to
   * produce the scan's output schema.
   *
   * The `postScanFilters` from [[PhysicalOperation]] (i.e. the predicates the connector left as
   * post-scan) and the `project` list are pushed into each per-set branch so
   * [[FileSourceStrategy]] re-derives partition/data filters (re-pushing them to the file index
   * and the file reader) and prunes columns. Connector-side `partitionFilters` / `dataFilters`
   * reported on [[ConnectorFileScan]] are informational only and not re-added here -- if the scan
   * reported them as accepted but did not remove them from `postScanFilters`, adding them again
   * would cause each predicate to evaluate twice.
   *
   * `tableSchema` is the connector table's schema (used to split partition vs. data columns); the
   * method does not otherwise depend on the [[DataSourceV2Relation]].
   */
  private[v2] def planFileScan(
      session: SparkSession,
      tableSchema: StructType,
      fileScan: ConnectorFileScan,
      project: Seq[NamedExpression],
      postScanFilters: Seq[Expression],
      output: Seq[AttributeReference]): Seq[SparkPlan] = {
    val fileSets = fileScan.planFileBatch().planFileSets()
    if (fileSets.isEmpty) {
      throw SparkException.internalError("FileScan returned no FileSets to plan.")
    }

    def buildRelation(fs: FileSet, branchOutput: Seq[AttributeReference]): LogicalRelation = {
      val partitionNames = fs.partitionColumns().iterator.flatMap(_.fieldNames()).toIndexedSeq
      val partitionSchema = StructType(partitionNames.flatMap(n => tableSchema.find(_.name == n)))
      // Pass the full table schema as `dataSchema` so `HadoopFsRelation` preserves the original
      // partition-column positions: `PartitioningUtils.mergeDataAndPartitionSchema` substitutes
      // the overlapping partition columns in place rather than appending them at the end.
      // Dropping the partition columns from `dataSchema` would reorder the relation schema to
      // (data ++ partitions) and misalign it with `relOutput`, which follows table-schema order
      // -- producing wrong results for any table whose partition columns are not already
      // trailing.
      //
      // Exclude metadata columns (e.g. `_metadata`): when the query references them, the
      // connector relation's schema (passed in as `tableSchema`) carries them, but they are not
      // physical data columns. Leaving `_metadata` in `dataSchema` makes FileSourceStrategy
      // treat it as a data column and the file reader fails with "Required column is missing in
      // data file". `_metadata` is re-exposed separately via `exposeMetadata`.
      val dataSchema = StructType(tableSchema.filterNot(f => MetadataAttribute.isValid(f.metadata)))
      val hfsr = HadoopFsRelation(
        location = fs.index(),
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = fs.format(),
        options = fs.options().asScala.toMap)(session)
      // A V1 LogicalRelation is never partial: FileSourceStrategy resolves the relation's
      // dataSchema and partitionSchema against its output, so the synthesized relation must
      // expose the full table schema even when the DSv2 scan pruned columns. Reuse the branch's
      // attribute ids for the columns the scan kept -- the re-planned project/filters bind to
      // them, and they may carry nested-pruned struct types -- and mint fresh attributes for the
      // pruned-away columns: nothing references them, and the branch is restricted to the scan
      // relation's output below.
      val attrByName = branchOutput.iterator.map(a => a.name -> a).toMap
      val fullOutput = dataSchema.map { f =>
        attrByName.getOrElse(f.name, DataTypeUtils.toAttribute(f))
      }
      LogicalRelation(
        relation = hfsr,
        output = fullOutput,
        catalogTable = None,
        isStreaming = false,
        stream = None)
    }

    // Re-expose the connector's `_metadata` column (declared via `SupportsMetadataColumns`) by
    // backing it with the V1 file-source `_metadata` the synthesized `HadoopFsRelation`
    // produces, so every subfield (file_path / file_name / ... / row_index) is materialized by
    // the lowered `FileSourceScanExec` exactly as in the V1 read path. The connector's
    // `rewriteMetadataColumn` hook materializes subfields the file format does not produce on
    // its own; identity when nothing needs rebuilding. Returns the plan to splice in.
    def exposeMetadata(
        relation: LogicalRelation,
        dsv2Metadata: AttributeReference): LogicalPlan = {
      val v1Metadata = relation.metadataOutput.collectFirst {
        // Match by the logical metadata-column name: with a conflicting `_metadata` DATA column
        // both the connector's and the synthesized v1 relation's metadata attribute are renamed
        // (e.g. `__metadata`) while keeping the logical name. Carry the connector's expr id AND
        // display name so the parent plan's references resolve against this branch's output.
        case MetadataAttributeWithLogicalName(a, FileFormat.METADATA_NAME) =>
          // The DSv2 column pruning may have pruned the metadata struct to the referenced
          // subfields (and rewritten the plan's GetStructField ordinals accordingly), so expose
          // the scan relation's struct type, not the relation's full one: FileSourceStrategy
          // flattens exactly the fields present in the attribute's type.
          a.withExprId(dsv2Metadata.exprId).withName(dsv2Metadata.name)
            .withDataType(dsv2Metadata.dataType)
      }.getOrElse {
        throw SparkException.internalError(
          "Synthesized HadoopFsRelation did not expose a _metadata column for a FileScan.")
      }
      val withMetadata = relation.copy(output = relation.output :+ v1Metadata)
      withMetadata.copyTagsFrom(relation)
      val rebuilt = fileScan.rewriteMetadataColumn(v1Metadata)
      if (rebuilt eq v1Metadata) {
        // No rebuild; `withMetadata` already exposes `_metadata` under the connector's expr id.
        withMetadata
      } else {
        // Project the rebuilt struct in place of the metadata attribute, re-aliased to the
        // connector's expr id so the parent plan's references resolve against this branch.
        val projectList = withMetadata.output.map {
          case a if a.exprId == v1Metadata.exprId =>
            rebuilt match {
              case Alias(child, name) => Alias(child, name)(exprId = dsv2Metadata.exprId)
              case other => Alias(other, v1Metadata.name)(exprId = dsv2Metadata.exprId)
            }
          case other => other
        }
        Project(projectList, withMetadata)
      }
    }

    def planBranch(branchLogical: LogicalPlan): SparkPlan = {
      val planned = FileSourceStrategy(branchLogical)
      if (planned.isEmpty) {
        throw SparkException.internalError(
          "FileSourceStrategy did not plan the LogicalRelation synthesized for a FileScan " +
            "FileSet.")
      }
      planned.head
    }

    // Prunable partition predicates the scan reported via SupportsPushDownCatalystFilters were
    // consumed from the post-scan predicates, so re-apply them here (rebased per branch) for
    // FileSourceStrategy to perform partition pruning, matching the V1 read path.
    val pushedPartitionFilters = fileScan.partitionFilters().toImmutableArraySeq
    val branchPlans = fileSets.toSeq.zipWithIndex.map { case (fs, i) =>
      // The first branch keeps the original output attributes (and their expr ids) because
      // UnionExec derives its output from the first child, and the parent plan references
      // those ids. Later branches get fresh attribute ids so the union children are
      // independent; the project/filter expressions over them are rebased onto those ids.
      val (relOutput, branchProject, branchFilters) = if (i == 0) {
        (output, project, postScanFilters)
      } else {
        val rebased = output.map(_.newInstance())
        val attrMap = output.iterator.map(_.exprId).zip(rebased.iterator).toMap
        def rebase(e: Expression): Expression = e.transform {
          case a: AttributeReference => attrMap.getOrElse(a.exprId, a)
        }
        (rebased,
          project.map(p => rebase(p).asInstanceOf[NamedExpression]),
          postScanFilters.map(rebase))
      }

      // `_metadata` (if the query references it) is not a data column of the synthesized
      // relation; separate it out and re-expose it via `exposeMetadata`, leaving the
      // relation's output to the table's columns. When absent, identical to prior behavior.
      // Match by the attribute's LOGICAL metadata-column name, not its display name: when
      // the table has a data column literally named `_metadata`, the connector's metadata
      // column is renamed (e.g. `__metadata`), and matching by display name would both miss
      // the metadata column and wrongly treat the conflicting DATA column as metadata.
      val metadataAttr = relOutput.collectFirst {
        case MetadataAttributeWithLogicalName(a, FileFormat.METADATA_NAME) => a
      }
      val dataOutput = relOutput.filterNot(a => metadataAttr.exists(_.exprId == a.exprId))
      val relation = buildRelation(fs, dataOutput)
      val scanInput = metadataAttr match {
        case Some(md) => exposeMetadata(relation, md)
        case None => relation
      }
      // Connector-derived filters (e.g. partition predicates implied by generated-column
      // expressions): re-derive the filters implied by this branch's filters and include them
      // in the re-planned Filter, so FileSourceStrategy extracts the same partition pruning as
      // the connector's own listing would. Resolved against this branch's relation so the
      // references carry the branch's attribute ids.
      val generatedFilters = if (branchFilters.nonEmpty) {
        fileScan.derivePartitionFilters(branchFilters.toArray, relation.output.toArray[Attribute])
          // The V1 path generates such filters in the optimizer, where ConstantFolding
          // subsequently folds the literal-only subexpressions. Here the optimizer has already
          // run, so fold them explicitly.
          .map(_.transformUp {
            case e if e.foldable && !e.isInstanceOf[Literal] =>
              Literal.create(e.eval(EmptyRow), e.dataType)
          }).toSeq
      } else {
        Nil
      }
      // Pushed partition filters reference the scan's partition attributes, which may have been
      // pruned from the scan output and re-minted with fresh ids in the synthesized relation.
      // Rebind them to this relation's attributes by name so FileSourceStrategy can prune.
      val resolver = session.sessionState.conf.resolver
      val boundPartitionFilters = pushedPartitionFilters.map(_.transform {
        case a: AttributeReference =>
          // Keep the original qualifier so the filter's display (e.g. partition-pruning checks)
          // matches the V1 path; bind to the relation's (possibly re-minted) attribute id.
          relation.output.find(o => resolver(o.name, a.name))
            .map(_.withQualifier(a.qualifier)).getOrElse(a)
      })
      val allBranchFilters = branchFilters ++ boundPartitionFilters ++ generatedFilters
      val withFilter =
        if (allBranchFilters.isEmpty) scanInput
        else Filter(allBranchFilters.reduce(And), scanInput)
      val branchLogical: LogicalPlan = if (branchProject.nonEmpty) {
        Project(branchProject, withFilter)
      } else if (withFilter.output != relOutput) {
        // The synthesized relation exposes the full table schema; restrict the branch to the
        // (possibly pruned) DSv2 scan relation output it stands in for.
        Project(relOutput, withFilter)
      } else {
        withFilter
      }
      planBranch(branchLogical)
    }
    val result = if (branchPlans.length == 1) branchPlans.head else UnionExec(branchPlans)
    result :: Nil
  }

  /**
   * Creates new spark plan that should apply given filters and projections to given scan node
   * @param project Projection list that should be output of returned spark plan
   * @param filters Filter list that should be applied to scan node
   * @param scan Scan node
   * @param needsUnsafeConversion Value that indicates whether unsafe conversion is needed
   * @return SparkPlan tree composed of scan node and eventually filter/project nodes
   */
  protected[sql] def withProjectAndFilter(
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      scan: LeafExecNode,
      needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }
}

/**
 * Get the expression of DS V2 to represent catalyst predicate that can be pushed down.
 */
object PushablePredicate extends Logging {
  def unapply(e: Expression): Option[Predicate] = new V2ExpressionBuilder(e, true).buildPredicate()
}
