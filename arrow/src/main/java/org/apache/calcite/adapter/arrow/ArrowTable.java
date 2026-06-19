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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Table backed by an Apache Arrow file.
 *
 * <p>Reads data from an Arrow IPC file on disk. Projections and filters read
 * directly from Arrow value-vectors.
 *
 * <p>Implements {@link TranslatableTable} so that it can be converted into
 * an {@link ArrowTableScan} for query planning, and {@link QueryableTable}
 * so that it can be used via the {@link org.apache.calcite.linq4j} API.
 */
public class ArrowTable extends AbstractTable
    implements TranslatableTable, QueryableTable {
  private final @Nullable RelProtoDataType protoRowType;
  /** Arrow schema. (In Calcite terminology, more like a row type than a Schema.) */
  private final Schema schema;
  private final File arrowFile;

  /** Creates an ArrowTable.
   *
   * @param protoRowType Optional row type override; if null, the row type is
   *                     deduced from the Arrow schema
   * @param arrowFile    Arrow IPC file on disk
   * @param schema       Arrow schema of the file
   */
  ArrowTable(@Nullable RelProtoDataType protoRowType, File arrowFile,
      Schema schema) {
    this.protoRowType = protoRowType;
    this.arrowFile = arrowFile;
    this.schema = schema;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /** Called via code generation; see uses of
   * {@link org.apache.calcite.adapter.arrow.ArrowMethod#ARROW_QUERY}. */
  @SuppressWarnings("unused")
  public Enumerable<Object> query(DataContext root, ImmutableIntList fields,
      List<List<List<String>>> conditions) {
    requireNonNull(fields, "fields");

    FileInputStream fis = null;
    try {
      fis = new FileInputStream(arrowFile);
      final ArrowFileReader reader =
          new ArrowFileReader(new SeekableReadChannel(fis.getChannel()),
              new RootAllocator());
      final FileInputStream fisRef = fis;
      final Runnable onClose = () -> closeSilently(fisRef);
      fis = null; // ownership transferred to onClose
      return new ArrowEnumerable(reader, fields, conditions, schema, onClose);
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    } finally {
      if (fis != null) {
        closeSilently(fis);
      }
    }
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new ArrowQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final ImmutableIntList fields =
        ImmutableIntList.copyOf(Util.range(fieldCount));
    final RelOptCluster cluster = context.getCluster();
    return new ArrowTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION),
        relOptTable, this, fields);
  }

  private static RelDataType deduceRowType(Schema schema,
      JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(),
          ArrowFieldTypeFactory.toType(field, typeFactory));
    }
    return builder.build();
  }

  /** Closes an {@link AutoCloseable} without throwing. */
  private static void closeSilently(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      // ignore
    }
  }

  /**
   * Implementation of {@link Queryable} based on a {@link ArrowTable}.
   *
   * @param <T> element type
   */
  public static class ArrowQueryable<T> extends AbstractTableQueryable<T> {
    ArrowQueryable(QueryProvider queryProvider, SchemaPlus schema,
        ArrowTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      throw new UnsupportedOperationException("enumerator");
    }

    private ArrowTable getTable() {
      return (ArrowTable) table;
    }

    /**
     * Executes a query with projection and filter conditions.
     *
     * @param fields projection fields
     * @param conditions filter conditions
     * @return result as enumerable
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Integer> fields,
        List<List<List<String>>> conditions) {
      final ImmutableIntList fieldList = ImmutableIntList.copyOf(fields);
      return getTable().query(null, fieldList, conditions);
    }
  }
}
