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
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.calcite.config.CalciteSystemProperty.JOIN_SELECTOR_COMPACT_CODE_THRESHOLD;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-3094">[CALCITE-3094]
 * Code of method grows beyond 64 KB when joining two tables with many fields</a>.
 */
public class LargeGeneratedJoinTest {

  /**
   * Marker interface for Field.
   */
  interface FieldT extends BiConsumer<RelDataTypeFactory, RelDataTypeFactory.Builder> {
  }

  /**
   * Marker interface for Row.
   */
  interface RowT extends Function<RelDataTypeFactory, RelDataType> {
  }

  static FieldT field(String name) {
    return (tf, b) -> b.add(name, SqlTypeName.VARCHAR);
  }

  static RowT row(FieldT... fields) {
    return tf -> {
      RelDataTypeFactory.Builder builder = tf.builder();
      for (FieldT f : fields) {
        f.accept(tf, builder);
      }
      return builder.build();
    };
  }

  private static QueryableTable tab(String table, int fieldCount) {
    List<Row> lRow = new ArrayList<>();
    for (int r = 0; r < 2; r++) {
      Object[] current = new Object[fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        current[i] = "v" + i;
      }
      lRow.add(Row.of(current));
    }

    List<FieldT> fields = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      fields.add(field(table + "_F_" + i));
    }

    final Enumerable<?> enumerable = Linq4j.asEnumerable(lRow);
    return new AbstractQueryableTable(Row.class) {

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return row(fields.toArray(new FieldT[fieldCount])).apply(typeFactory);
      }

      @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
          String tableName) {
        return (Queryable<T>) enumerable.asQueryable();
      }
    };
  }

  private static int getBaseTableSize() {
    // If compact code generation is turned off, we generate tables that
    // will cause the issue. Otherwise, to avoid impacting the test duration,
    // we only generate tables wide enough to enable the compact code generation.
    int compactCodeThreshold = JOIN_SELECTOR_COMPACT_CODE_THRESHOLD.value();
    return compactCodeThreshold < 0 ? 3000 : Math.max(100, compactCodeThreshold);
  }

  private static int getT0Size() {
    return getBaseTableSize();
  }
  private static int getT1Size() {
    return getBaseTableSize() + 1;
  }

  private static CalciteAssert.AssertQuery assertQuery(String sql) {
    Schema rootSchema = new AbstractSchema() {
      @Override protected Map<String, Table> getTableMap() {
        return ImmutableMap.of("T0", tab("T0", getT0Size()),
            "T1", tab("T1", getT1Size()));
      }
    };

    final CalciteSchema sp = CalciteSchema.createRootSchema(false, true);
    sp.add("ROOT", rootSchema);

    final CalciteAssert.AssertThat ca = CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("ROOT", rootSchema)
        .withDefaultSchema("ROOT");

    return ca.query(sql);
  }

  @Test public void test() {
    String sql = "SELECT * \n"
        + "FROM ROOT.T0 \n"
        + "JOIN ROOT.T1 \n"
        + "ON TRUE";

    sql = "select T0_F_0||T0_F_1, * from (" + sql + ")";

    final CalciteAssert.AssertQuery query = assertQuery(sql);
    query.returns(rs -> {
      try {
        assertTrue(rs.next());
        assertThat(rs.getMetaData().getColumnCount(),
            is(1 + getT0Size() + getT1Size()));
        long row = 0;
        do {
          ++row;
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            // Rows have the format: v0v1, v0, v1, v2, ..., v99, v0, v1, v2, ..., v99, v100
            final String reason = "Error at row: " + row + ", column: " + i;
            if (i == 1) {
              assertThat(reason, rs.getString(i), is("v0v1"));
            } else if (i <= getT0Size() + 1) {
              assertThat(reason, rs.getString(i), is("v" + (i - 2)));
            } else {
              assertThat(reason, rs.getString(i),
                  is("v" + ((i - 2) - getT0Size())));
            }
          }
        } while (rs.next());
        assertThat(row, is(4L));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6593">[CALCITE-6593]
   * NPE when outer joining tables with many fields and unmatching rows</a>.
   */
  @Test public void testLeftJoinWithEmptyRightSide() {
    String sql = "SELECT * \n"
        + "FROM ROOT.T0 \n"
        + "LEFT JOIN (SELECT * FROM ROOT.T1 WHERE T1_F_0 = 'xyz') \n"
        + "ON TRUE";

    sql = "select T0_F_0||T0_F_1, * from (" + sql + ")";

    final CalciteAssert.AssertQuery query = assertQuery(sql);
    query.returns(rs -> {
      try {
        assertTrue(rs.next());
        assertThat(rs.getMetaData().getColumnCount(),
            is(1 + getT0Size() + getT1Size()));
        long row = 0;
        do {
          ++row;
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            // Rows have the format: v0v1, v0, v1, v2, ..., v99, null, ..., null
            final String reason = "Error at row: " + row + ", column: " + i;
            if (i == 1) {
              assertThat(reason, rs.getString(i), is("v0v1"));
            } else if (i <= getT0Size() + 1) {
              assertThat(reason, rs.getString(i), is("v" + (i - 2)));
            } else {
              assertThat(reason, rs.getString(i), nullValue());
            }
          }
        } while (rs.next());
        assertThat(row, is(2L));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6593">[CALCITE-6593]
   * NPE when outer joining tables with many fields and unmatching rows</a>.
   */
  @Test public void testRightJoinWithEmptyLeftSide() {
    String sql = "SELECT * \n"
        + "FROM (SELECT * FROM ROOT.T0 WHERE T0_F_0 = 'xyz') \n"
        + "RIGHT JOIN ROOT.T1 \n"
        + "ON TRUE";

    sql = "select T1_F_0||T1_F_1, * from (" + sql + ")";

    final CalciteAssert.AssertQuery query = assertQuery(sql);
    query.returns(rs -> {
      try {
        assertTrue(rs.next());
        assertThat(rs.getMetaData().getColumnCount(),
            is(1 + getT0Size() + getT1Size()));
        long row = 0;
        do {
          ++row;
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            // Rows have the format: v0v1, null, ..., null, v0, v1, v2, ..., v100
            final String reason = "Error at row: " + row + ", column: " + i;
            if (i == 1) {
              assertThat(reason, rs.getString(i), is("v0v1"));
            } else if (i <= getT0Size() + 1) {
              assertThat(reason, rs.getString(i), nullValue());
            } else {
              assertThat(reason, rs.getString(i),
                  is("v" + (i - 2 - getT0Size())));
            }
          }
        } while (rs.next());
        assertThat(row, is(2L));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6593">[CALCITE-6593]
   * NPE when outer joining tables with many fields and unmatching rows</a>.
   */
  @Test public void testFullJoinWithUnmatchedRows() {
    String sql = "SELECT * \n"
        + "FROM ROOT.T0 \n"
        + "FULL JOIN ROOT.T1 \n"
        + "ON T0_F_0 <> T1_F_0";

    sql = "select T0_F_0||T0_F_1, T1_F_0||T1_F_1, * from (" + sql + ")";

    final CalciteAssert.AssertQuery query = assertQuery(sql);
    query.returns(rs -> {
      try {
        assertTrue(rs.next());
        assertThat(rs.getMetaData().getColumnCount(),
            is(1 + 1 + getT0Size() + getT1Size()));
        long row = 0;
        do {
          ++row;
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            final String reason = "Error at row: " + row + ", column: " + i;
            if (row <= 2) {
              // First 2 rows have the format: v0v1, null, v0, v1, v2, ..., v99, null, ..., null
              if (i == 1) {
                assertThat(reason, rs.getString(i), is("v0v1"));
              } else if (i == 2) {
                assertThat(reason, rs.getString(i), nullValue());
              } else if (i <= getT0Size() + 2) {
                assertThat(reason, rs.getString(i), is("v" + (i - 3)));
              } else {
                assertThat(reason, rs.getString(i), nullValue());
              }
            } else {
              // Last 2 rows have the format: null, v0v1, null, ..., null, v0, v1, v2, ..., v100
              if (i == 1) {
                assertThat(reason, rs.getString(i), nullValue());
              } else if (i == 2) {
                assertThat(reason, rs.getString(i), is("v0v1"));
              } else if (i <= getT0Size() + 2) {
                assertThat(reason, rs.getString(i), nullValue());
              } else {
                assertThat(reason, rs.getString(i),
                    is("v" + (i - 3 - getT0Size())));
              }
            }
          }
        } while (rs.next());
        assertThat(row, is(4L));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
