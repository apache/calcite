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
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableMultiset;

import org.junit.Test;

import java.sql.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class TableInRootSchemaTest {
  /** Test case for issue 85, "Adding a table to the root schema causes breakage
   * in OptiqPrepareImpl". */
  @Test public void testAddingTableInRootSchema() throws Exception {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);

    optiqConnection.getRootSchema().add("SAMPLE", new SimpleTable());
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select A, SUM(B) from SAMPLE group by A");

    assertThat(
        ImmutableMultiset.of(
            "A=foo; EXPR$1=8",
            "A=bar; EXPR$1=4"),
        equalTo(OptiqAssert.toSet(resultSet)));

    final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnName(1), equalTo("A"));
    assertThat(resultSetMetaData.getTableName(1), equalTo("SAMPLE"));
    assertThat(resultSetMetaData.getSchemaName(1), nullValue());
    // Per JDBC, column name should be null. But DBUnit requires every column
    // to have a name, so the driver uses the label.
    assertThat(resultSetMetaData.getColumnName(2), equalTo("EXPR$1"));
    assertThat(resultSetMetaData.getTableName(2), nullValue());
    assertThat(resultSetMetaData.getSchemaName(2), nullValue());
    resultSet.close();
    statement.close();
    connection.close();
  }

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
          new ArrayList<Pair<String, RelDataType>>(columnCount);
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

    // keep
    public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return new JavaRules.EnumerableTableAccessRel(context.getCluster(),
          context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
          relOptTable, (Class) getElementType());
    }
  }
}

// End TableInRootSchemaTest.java
