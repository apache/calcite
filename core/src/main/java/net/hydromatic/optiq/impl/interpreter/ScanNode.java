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
import net.hydromatic.optiq.QueryableTable;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Schemas;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

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
  private final DataContext root;

  public ScanNode(Interpreter interpreter, TableAccessRelBase rel) {
    this.rel = rel;
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
    //noinspection unchecked
    Enumerable<Row> iterable = rel.getTable().unwrap(Enumerable.class);
    if (iterable != null) {
      return iterable;
    }
    final QueryableTable queryableTable =
        rel.getTable().unwrap(QueryableTable.class);
    if (queryableTable != null) {
      final Type elementType = queryableTable.getElementType();
      SchemaPlus schema = root.getRootSchema();
      for (String name : Util.skipLast(rel.getTable().getQualifiedName())) {
        schema = schema.getSubSchema(name);
      }
      if (elementType instanceof Class) {
        //noinspection unchecked
        final Queryable<Object> queryable = Schemas.queryable(root,
            (Class) elementType, rel.getTable().getQualifiedName());
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
            rel.getTable().getQualifiedName());
      }
    }
    throw new AssertionError("cannot convert table " + rel.getTable()
        + " to iterable");
  }
}

// End ScanNode.java
