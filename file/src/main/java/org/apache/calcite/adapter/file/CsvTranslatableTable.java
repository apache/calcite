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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file.
 *
 * <p>Copied from {@code CsvTranslatableTable} in demo CSV adapter,
 * with more advanced features.
 */
public class CsvTranslatableTable extends CsvTable
    implements QueryableTable, TranslatableTable {
  /** Creates a CsvTranslatableTable with a custom separator. */
  CsvTranslatableTable(Source source, @Nullable RelProtoDataType protoRowType,
      char separator) {
    super(source, protoRowType, separator);
  }

  @Override public String toString() {
    return "CsvTranslatableTable";
  }

  /** Returns an enumerable over a given projection of the fields. */
  @SuppressWarnings("unused") // called from generated code
  public Enumerable<Object> project(final DataContext root,
      final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        return new CsvEnumerator<>(source, cancelFlag,
            getFieldTypes(typeFactory), ImmutableIntList.of(fields),
            separator);
      }
    };
  }

  /** Returns an enumerable over a given projection of the fields, with
   * filter values applied during the scan to skip non-matching rows.
   *
   * <p>This method is called from generated code (via
   * {@link CsvTableScan#implement}) when filter predicates have been pushed
   * down into the scan by {@link CsvFilterTableScanRule}.
   *
   * @param root         Data context (provides type factory and cancel flag)
   * @param fields       Indices of the fields to project (into full table schema)
   * @param filterValues Per-column equality filter values; null means no filter
   *                     for that column. Indexed by full-table column index.
   */
  @SuppressWarnings({"unchecked", "unused"}) // called from generated code
  public Enumerable<Object> scan(final DataContext root,
      final int[] fields, final @Nullable String @Nullable [] filterValues) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
        return (Enumerator<Object>) (Enumerator<?>)
            new CsvEnumerator<>(source, cancelFlag, false, filterValues,
                fieldTypes,
                CsvEnumerator.converter(fieldTypes,
                    ImmutableIntList.of(fields)),
                separator);
      }
    };
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = CsvEnumerator.identityList(fieldCount);
    return new CsvTableScan(context.getCluster(), relOptTable, this, fields);
  }
}
