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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Hook;
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
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  private static QueryableTable tab(int fieldCount) {
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
      fields.add(field("F_" + i));
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

  @Test public void test() throws SQLException {
    Schema rootSchema = new AbstractSchema() {
      @Override protected Map<String, Table> getTableMap() {
        return ImmutableMap.of("T0", tab(100),
            "T1", tab(101));
      }
    };

    final CalciteSchema sp = CalciteSchema.createRootSchema(false, true);
    sp.add("ROOT", rootSchema);

    String sql = "SELECT * \n"
        + "FROM ROOT.T0 \n"
        + "JOIN ROOT.T1 \n"
        + "ON TRUE";

    sql = "select F_0||F_1, * from (" + sql + ")";


    final CalciteAssert.AssertThat ca = CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("ROOT", rootSchema)
        .withDefaultSchema("ROOT");

    final CalciteAssert.AssertQuery query = ca.query(sql);
    query.withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) pl -> {
      pl.removeRule(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
      pl.addRule(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);
    });

    try {
      query.returns(rs -> {
        try {
          assertTrue(rs.next());
          assertEquals(101 + 100 + 1, rs.getMetaData().getColumnCount());
          long row = 0;
          do {
            ++row;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
              // Rows have the format: v0v1, v0, v1, v2, ..., v99, v0, v1, v2, ..., v99, v100
              if (i == 1) {
                assertEquals("v0v1", rs.getString(i),
                    "Error at row: " + row + ", column: " + i);
              } else if (i == rs.getMetaData().getColumnCount()) {
                assertEquals("v100", rs.getString(i),
                    "Error at row: " + row + ", column: " + i);
              } else {
                assertEquals("v" + ((i - 2) % 100), rs.getString(i),
                    "Error at row: " + row + ", column: " + i);
              }
            }
          } while (rs.next());
          assertEquals(4, row);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RuntimeException ex) {
      throw (SQLException) ex.getCause();
    }
  }
}
