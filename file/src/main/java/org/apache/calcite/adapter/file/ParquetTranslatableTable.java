/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Table based on a Parquet file that implements TranslatableTable.
 * This allows the file adapter to properly convert it to EnumerableRel.
 */
public class ParquetTranslatableTable extends AbstractTable implements TranslatableTable {

  private final File parquetFile;
  private RelDataType rowType;

  public ParquetTranslatableTable(File parquetFile) {
    this.parquetFile = parquetFile;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = deriveRowType(typeFactory);
    }
    return rowType;
  }

  private RelDataType deriveRowType(RelDataTypeFactory typeFactory) {
    try {
      Path hadoopPath = new Path(parquetFile.getAbsolutePath());
      Configuration conf = new Configuration();

      // Read Parquet schema
      @SuppressWarnings("deprecation")
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, hadoopPath);
      MessageType messageType = metadata.getFileMetaData().getSchema();

      // Convert Parquet schema to Calcite schema
      List<String> names = new ArrayList<>();
      List<RelDataType> types = new ArrayList<>();
      List<Type> parquetFields = messageType.getFields();

      for (Type field : parquetFields) {
        SqlTypeName sqlType = convertParquetTypeToSql(field);
        names.add(field.getName());
        types.add(typeFactory.createSqlType(sqlType));
      }

      return typeFactory.createStructType(Pair.zip(names, types));

    } catch (IOException e) {
      throw new RuntimeException("Failed to read Parquet schema", e);
    }
  }

  private SqlTypeName convertParquetTypeToSql(Type parquetType) {
    switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
      case INT32:
        return SqlTypeName.INTEGER;
      case INT64:
        return SqlTypeName.BIGINT;
      case FLOAT:
        return SqlTypeName.FLOAT;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case BINARY:
      default:
        return SqlTypeName.VARCHAR;
    }
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new ParquetTableScan(context.getCluster(), relOptTable, this, relOptTable.getRowType());
  }

  /**
   * Relational expression representing a scan of a Parquet file.
   */
  public static class ParquetTableScan extends TableScan implements EnumerableRel {
    private final ParquetTranslatableTable parquetTable;
    private final RelDataType projectRowType;

    public ParquetTableScan(RelOptCluster cluster, RelOptTable table,
        ParquetTranslatableTable parquetTable, RelDataType projectRowType) {
      super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
      this.parquetTable = parquetTable;
      this.projectRowType = projectRowType;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new ParquetTableScan(getCluster(), table, parquetTable, projectRowType);
    }

    @Override public RelDataType deriveRowType() {
      return projectRowType != null ? projectRowType : table.getRowType();
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      PhysType physType =
          PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

      String path = parquetTable.parquetFile.getAbsolutePath();
      Expression parquetFileExpression = Expressions.constant(path);

      // Generate enumerable expression
      BlockBuilder builder = new BlockBuilder();

      // Create expression to scan the Parquet file
      Expression enumerable =
          builder.append("enumerable",
          Expressions.call(
              ParquetEnumerableFactory.class,
              "enumerable",
              parquetFileExpression));

      builder.add(Expressions.return_(null, enumerable));

      return implementor.result(physType, builder.toBlock());
    }

  }
}
