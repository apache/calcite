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
package net.hydromatic.optiq.impl.interpreter;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.FilterableTable;
import net.hydromatic.optiq.ProjectableFilterableTable;
import net.hydromatic.optiq.QueryableTable;
import net.hydromatic.optiq.ScannableTable;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.runtime.Enumerables;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.eigenbase.rel.TableAccessRelBase}.
 */
public class ScanNode implements Node {
  private final Sink sink;
  private final TableAccessRelBase rel;
  private final ImmutableList<RexNode> filters;
  private final DataContext root;
  private final int[] projects;

  public ScanNode(Interpreter interpreter, TableAccessRelBase rel,
      ImmutableList<RexNode> filters, ImmutableIntList projects) {
    this.rel = rel;
    this.filters = Preconditions.checkNotNull(filters);
    this.projects = projects == null ? null : projects.toIntArray();
    this.sink = interpreter.sink(rel);
    this.root = interpreter.getDataContext();
  }

  public void run() throws InterruptedException {
    final Enumerable<Row> iterable = iterable();
    final Enumerator<Row> enumerator = iterable.enumerator();
    while (enumerator.moveNext()) {
      sink.send(enumerator.current());
    }
    enumerator.close();
    sink.end();
  }

  private Enumerable<Row> iterable() {
    final RelOptTable table = rel.getTable();
    final ProjectableFilterableTable pfTable =
        table.unwrap(ProjectableFilterableTable.class);
    if (pfTable != null) {
      final List<RexNode> filters1 = Lists.newArrayList(filters);
      final Enumerable<Object[]> enumerator =
          pfTable.scan(root, filters1, projects);
      assert filters1.isEmpty()
          : "table could not handle a filter it earlier said it could";
      return Enumerables.toRow(enumerator);
    }
    if (projects != null) {
      throw new AssertionError("have projects, but table cannot handle them");
    }
    final FilterableTable filterableTable =
        table.unwrap(FilterableTable.class);
    if (filterableTable != null) {
      final List<RexNode> filters1 = Lists.newArrayList(filters);
      final Enumerable<Object[]> enumerator =
          filterableTable.scan(root, filters1);
      assert filters1.isEmpty()
          : "table could not handle a filter it earlier said it could";
      return Enumerables.toRow(enumerator);
    }
    if (!filters.isEmpty()) {
      throw new AssertionError("have filters, but table cannot handle them");
    }
    //noinspection unchecked
    Enumerable<Row> iterable = table.unwrap(Enumerable.class);
    if (iterable != null) {
      return iterable;
    }
    final QueryableTable queryableTable = table.unwrap(QueryableTable.class);
    if (queryableTable != null) {
      final Type elementType = queryableTable.getElementType();
      SchemaPlus schema = root.getRootSchema();
      for (String name : Util.skipLast(table.getQualifiedName())) {
        schema = schema.getSubSchema(name);
      }
      if (elementType instanceof Class) {
        //noinspection unchecked
        final Queryable<Object> queryable = Schemas.queryable(root,
            (Class) elementType, table.getQualifiedName());
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        Class type = (Class) elementType;
        for (Field field : type.getFields()) {
          if (Modifier.isPublic(field.getModifiers())
              && !Modifier.isStatic(field.getModifiers())) {
            fieldBuilder.add(field);
          }
        }
        final List<Field> fields = fieldBuilder.build();
        return queryable.select(
            new Function1<Object, Row>() {
              public Row apply(Object o) {
                final Object[] values = new Object[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                  Field field = fields.get(i);
                  try {
                    values[i] = field.get(o);
                  } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                }
                return new Row(values);
              }
            });
      } else {
        return Schemas.queryable(root, Row.class,
            table.getQualifiedName());
      }
    }
    final ScannableTable scannableTable =
        table.unwrap(ScannableTable.class);
    if (scannableTable != null) {
      return Enumerables.toRow(scannableTable.scan(root));
    }
    throw new AssertionError("cannot convert table " + table
        + " to iterable");
  }
}

// End ScanNode.java
