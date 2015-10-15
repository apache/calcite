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
package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMultiset;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** Test case for issue 85. */
public class TableInRootSchemaTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-85">[CALCITE-85]
   * Adding a table to the root schema causes breakage in
   * CalcitePrepareImpl</a>. */
  @Test public void testAddingTableInRootSchema() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);

    calciteConnection.getRootSchema().add("SAMPLE", new SimpleTable());
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select A, SUM(B) from SAMPLE group by A");

    assertThat(
        ImmutableMultiset.of(
            "A=foo; EXPR$1=8",
            "A=bar; EXPR$1=4"),
        equalTo(CalciteAssert.toSet(resultSet)));

    final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnName(1), equalTo("A"));
    assertThat(resultSetMetaData.getTableName(1), equalTo("SAMPLE"));
    assertThat(resultSetMetaData.getSchemaName(1), nullValue());
    assertThat(resultSetMetaData.getColumnClassName(1),
        equalTo("java.lang.String"));
    // Per JDBC, column name should be null. But DBUnit requires every column
    // to have a name, so the driver uses the label.
    assertThat(resultSetMetaData.getColumnName(2), equalTo("EXPR$1"));
    assertThat(resultSetMetaData.getTableName(2), nullValue());
    assertThat(resultSetMetaData.getSchemaName(2), nullValue());
    assertThat(resultSetMetaData.getColumnClassName(2),
        equalTo("java.lang.Integer"));
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Table with columns (A, B). */
  public static class SimpleTable extends AbstractQueryableTable
      implements TranslatableTable {
    private String[] columnNames = { "A", "B" };
    private Class[] columnTypes = { String.class, Integer.class };
    private Object[][] rows = new Object[3][];

    SimpleTable() {
      super(Object[].class);

      rows[0] = new Object[] { "foo", 5 };
      rows[1] = new Object[] { "bar", 4 };
      rows[2] = new Object[] { "foo", 3 };
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      int columnCount = columnNames.length;
      final List<Pair<String, RelDataType>> columnDesc =
          new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        final RelDataType colType = typeFactory
            .createJavaType(columnTypes[i]);
        columnDesc.add(Pair.of(columnNames[i], colType));
      }
      return typeFactory.createStructType(columnDesc);
    }

    public Iterator<Object[]> iterator() {
      return Linq4j.enumeratorIterator(enumerator());
    }

    public Enumerator<Object[]> enumerator() {
      return enumeratorImpl(null);
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) enumeratorImpl(null);
        }
      };
    }

    private Enumerator<Object[]> enumeratorImpl(final int[] fields) {
      return new Enumerator<Object[]>() {
        private Object[] current;
        private Iterator<Object[]> iterator = Arrays.asList(rows)
            .iterator();

        public Object[] current() {
          return current;
        }

        public boolean moveNext() {
          if (iterator.hasNext()) {
            Object[] full = iterator.next();
            current = fields != null ? convertRow(full) : full;
            return true;
          } else {
            current = null;
            return false;
          }
        }

        public void reset() {
          throw new UnsupportedOperationException();
        }

        public void close() {
          // noop
        }

        private Object[] convertRow(Object[] full) {
          final Object[] objects = new Object[fields.length];
          for (int i = 0; i < fields.length; i++) {
            objects[i] = full[fields[i]];
          }
          return objects;
        }
      };
    }

    public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return EnumerableTableScan.create(context.getCluster(), relOptTable);
    }
  }
}

// End TableInRootSchemaTest.java
