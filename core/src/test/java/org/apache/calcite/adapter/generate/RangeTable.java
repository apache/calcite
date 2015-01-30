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
package org.apache.calcite.adapter.generate;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Table that returns a range of integers.
 */
public class RangeTable extends AbstractQueryableTable {
  private final String columnName;
  private final int start;
  private final int end;

  protected RangeTable(Class<?> elementType, String columnName, int start,
      int end) {
    super(elementType);
    this.columnName = columnName;
    this.start = start;
    this.end = end;
  }

  /** Creates a RangeTable. */
  public static RangeTable create(Class<?> elementType, String columnName,
      int start, int end) {
    return new RangeTable(elementType, columnName, start, end);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add(columnName, SqlTypeName.INTEGER)
        .build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) RangeTable.this.enumerator();
      }
    };
  }

  public Enumerator<Integer> enumerator() {
    return new Enumerator<Integer>() {
      int current = start - 1;

      public Integer current() {
        if (current >= end) {
          throw new NoSuchElementException();
        }
        return current;
      }

      public boolean moveNext() {
        ++current;
        return current < end;
      }

      public void reset() {
        current = start - 1;
      }

      public void close() {
      }
    };
  }

  /** Implementation of {@link org.apache.calcite.schema.TableFactory} that
   * allows a {@link RangeTable} to be included as a custom table in a Calcite
   * model file. */
  public static class Factory implements TableFactory<RangeTable> {
    public RangeTable create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final String columnName = (String) operand.get("column");
      final int start = (Integer) operand.get("start");
      final int end = (Integer) operand.get("end");
      final String elementType = (String) operand.get("elementType");
      Class<?> type;
      if ("array".equals(elementType)) {
        type = Object[].class;
      } else if ("object".equals(elementType)) {
        type = Object.class;
      } else if ("integer".equals(elementType)) {
        type = Integer.class;
      } else {
        throw new IllegalArgumentException(
            "Illegal 'elementType' value: " + elementType);
      }
      return RangeTable.create(type, columnName, start, end);
    }
  }
}

// End RangeTable.java
