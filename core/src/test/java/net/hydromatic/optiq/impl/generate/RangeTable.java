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
package net.hydromatic.optiq.impl.generate;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TableFactory;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Table that returns a range of integers.
 */
public class RangeTable extends AbstractQueryableTable {
  private final String columnName;
  private final int start;
  private final int end;

  protected RangeTable(String columnName, String tableName, int start,
      int end) {
    super(Object[].class);
    this.columnName = columnName;
    this.start = start;
    this.end = end;
  }

  /** Creates a RangeTable. */
  public static RangeTable create(String tableName, String columnName,
      int start, int end) {
    return new RangeTable(columnName, tableName, start, end);
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

  /** Implementation of {@link net.hydromatic.optiq.TableFactory} that allows
   * a {@link RangeTable} to be included as a custom table in an Optiq model
   * file. */
  public static class Factory implements TableFactory<RangeTable> {
    public RangeTable create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final String columnName = (String) operand.get("column");
      final int start = (Integer) operand.get("start");
      final int end = (Integer) operand.get("end");
      return RangeTable.create(name, columnName, start, end);
    }
  }
}

// End RangeTable.java
