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

package org.apache.spark.sql.execution.datasources.v2.parquet;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.v2.FileTable;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.AtomicType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.GeometryType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampLTZNanosType;
import org.apache.spark.sql.types.TimestampNTZNanosType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Parquet table for BATCH READS, written in Java against the DSv2 {@code Table} /
 * {@code ScanBuilder} APIs plus the {@link org.apache.spark.sql.connector.read.FileScan}
 * interface. Its scan exposes the table's file layout ({@code FileIndex} + Parquet
 * {@code FileFormat}), which the planner lowers back to the V1 file-source execution path
 * ({@code FileSourceScanExec}, vectorized parquet reader).
 *
 * <p>Read only: it advertises only {@code BATCH_READ}, so batch writes
 * ({@code DataFrameWriter.save}) and streaming reads/writes fall back to the V1 paths
 * exactly as before.
 */
public class ParquetFileScanTable extends FileTable implements SupportsMetadataColumns {

  private final String tableName;
  private final SparkSession session;
  private final CaseInsensitiveStringMap tableOptions;
  private final scala.collection.immutable.Seq<String> tablePaths;
  private final scala.Option<StructType> userSpecifiedSchema;

  public ParquetFileScanTable(
      String tableName,
      SparkSession session,
      CaseInsensitiveStringMap options,
      scala.collection.immutable.Seq<String> paths,
      scala.Option<StructType> userSpecifiedSchema) {
    super(session, options, paths, userSpecifiedSchema);
    this.tableName = tableName;
    this.session = session;
    this.tableOptions = options;
    this.tablePaths = paths;
    this.userSpecifiedSchema = userSpecifiedSchema;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
    return new ParquetFileScanBuilder(
      session, fileIndex(), schema(), dataSchema(), mergedOptions(scanOptions));
  }

  @Override
  public scala.Option<StructType> inferSchema(
      scala.collection.immutable.Seq<FileStatus> files) {
    return ParquetUtils.inferSchema(session, scalaOptions(), files);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private scala.collection.immutable.Map<String, String> scalaOptions() {
    scala.collection.mutable.Map<String, String> mutable =
      scala.jdk.javaapi.CollectionConverters.asScala(tableOptions.asCaseSensitiveMap());
    return (scala.collection.immutable.Map<String, String>)
      scala.collection.immutable.Map$.MODULE$.from((scala.collection.IterableOnce) mutable);
  }

  // Read only: BATCH_WRITE is intentionally not advertised, so DataFrameWriter falls back to
  // the V1 write path (InsertIntoHadoopFsRelationCommand) and never calls newWriteBuilder.
  @Override
  public Set<TableCapability> capabilities() {
    return EnumSet.of(TableCapability.BATCH_READ);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    throw new UnsupportedOperationException(
      "ParquetFileScanTable is read-only: writes fall back to the V1 path.");
  }

  // Expose the V1 file-source `_metadata` column with the parquet-specific shape (base file
  // fields plus `row_index`), so `_metadata` resolves on this table exactly as on the V1 path.
  // Values are materialized by the lowering: the planner substitutes this attribute with the
  // synthesized V1 relation's own `_metadata`.
  @Override
  public MetadataColumn[] metadataColumns() {
    DataType metadataType = new ParquetFileFormat().createFileMetadataCol().dataType();
    return new MetadataColumn[] {
      new MetadataColumn() {
        @Override
        public String name() {
          return FileFormat$.MODULE$.METADATA_NAME();
        }

        @Override
        public DataType dataType() {
          return metadataType;
        }

        @Override
        public boolean isNullable() {
          return false;
        }
      }
    };
  }

  // Allow renaming `_metadata` when the table also has a DATA column named `_metadata`,
  // matching the V1 file source (which renames its metadata column, e.g. to `__metadata`, and
  // resolves it by logical name).
  @Override
  public boolean canRenameConflictingMetadataColumns() {
    return true;
  }

  @Override
  public boolean supportsDataType(DataType dataType) {
    // GeoSpatial data types in Parquet are limited only to types with supported SRIDs.
    if (dataType instanceof GeometryType) {
      return GeometryType.isSridSupported(((GeometryType) dataType).srid());
    } else if (dataType instanceof GeographyType) {
      return GeographyType.isSridSupported(((GeographyType) dataType).srid());
    } else if (dataType instanceof TimestampNTZNanosType ||
        dataType instanceof TimestampLTZNanosType) {
      // Nanosecond-capable timestamps are not yet supported by this datasource.
      return false;
    } else if (dataType instanceof AtomicType) {
      return true;
    } else if (dataType instanceof StructType) {
      for (StructField field : ((StructType) dataType).fields()) {
        if (!supportsDataType(field.dataType())) {
          return false;
        }
      }
      return true;
    } else if (dataType instanceof ArrayType) {
      return supportsDataType(((ArrayType) dataType).elementType());
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      return supportsDataType(mapType.keyType()) && supportsDataType(mapType.valueType());
    } else if (dataType instanceof UserDefinedType) {
      return supportsDataType(((UserDefinedType<?>) dataType).sqlType());
    } else {
      return false;
    }
  }

  @Override
  public String formatName() {
    return "Parquet";
  }

  @Override
  public Class<? extends FileFormat> fallbackFileFormat() {
    return ParquetFileFormat.class;
  }

  // Value-based identity, mirroring the case-class equality of the Scala ParquetTable: two
  // loads of the same paths with the same options are the same table, so plan equality (and
  // with it CacheManager lookups) behaves as it does on the other read paths.
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ParquetFileScanTable)) {
      return false;
    }
    ParquetFileScanTable that = (ParquetFileScanTable) other;
    return tablePaths.equals(that.tablePaths) &&
      tableOptions.asCaseSensitiveMap().equals(that.tableOptions.asCaseSensitiveMap()) &&
      userSpecifiedSchema.equals(that.userSpecifiedSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablePaths, tableOptions.asCaseSensitiveMap(), userSpecifiedSchema);
  }
}
