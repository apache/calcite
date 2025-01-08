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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Smalls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test case for issue 85. */
class TableInRootSchemaTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-85">[CALCITE-85]
   * Adding a table to the root schema causes breakage in
   * CalcitePrepareImpl</a>. */
  @Test void testAddingTableInRootSchema() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);

    calciteConnection.getRootSchema().add("SAMPLE", new Smalls.SimpleTable());
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

  /**
   * Helper class for the test for [CALCITE-6764] below.
   */
  private static class RowTable extends AbstractQueryableTable {
    protected RowTable() {
      super(Object[].class);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      final PairList<String, RelDataType> columnDesc = PairList.withCapacity(1);
      // Schema contains a column whose type is MAP<VARCHAR, ROW(VARCHAR)>, but
      // the ROW type can be nullable.
      final RelDataType colType =
          typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR),
            new RelRecordType(
                StructKind.PEEK_FIELDS,
                  ImmutableList.of(
                      new RelDataTypeFieldImpl("K", 0,
                          typeFactory.createSqlType(SqlTypeName.VARCHAR))), true));
      columnDesc.add("P", colType);
      return typeFactory.createStructType(columnDesc);
    }

    @Override public <T> Queryable<T> asQueryable(
        QueryProvider queryProvider, SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @Override public Enumerator<T> enumerator() {
          return new Enumerator<T>() {
            @Override public T current() {
              return null;
            }

            @Override public boolean moveNext() {
              // Table is empty
              return false;
            }

            @Override public void reset() {}

            @Override public void close() {}
          };
        }
      };
    }
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6764">[CALCITE-6764]
   * Field access from a nullable ROW should be nullable</a>. */
  @Test void testNullableValue() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    calciteConnection.getRootSchema().add("T", new RowTable());
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT P['a'].K FROM T");
    resultSet.close();
    statement.close();
    connection.close();
  }
}
