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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for bounded expression-lineage expansion. */
class RelMdExpressionLineageTest {
  @ParameterizedTest
  @CsvSource({"2, 2, false", "10, 4, true", "11, 4, true", "11, 4, false"})
  void testUnionLineage(int branches, int columns, boolean window) throws Exception {
    RelNode rel = ExpressionLineageProgram.rel(branches, columns, window);
    RexNode computed = RexInputRef.of(columns, rel.getRowType().getFieldList());
    Set<RexNode> lineage = rel.getCluster().getMetadataQuery().getExpressionLineage(rel, computed);
    int candidates = (int) Math.pow(branches, columns);
    int budget = CalciteSystemProperty.EXPRESSION_LINEAGE_MAX_EXPANSIONS.value();
    if (budget >= 0 && candidates > budget) {
      // Metadata was not computed.
      assertNull(lineage);
    } else {
      assertNotNull(lineage);
      assertEquals(candidates, lineage.size());
    }
  }

  @Test void testOverflowDoesNotStartExpansion() {
    RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl());
    RelDataType type = builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    RexInputRef a = builder.makeInputRef(type, 0);
    RexInputRef b = builder.makeInputRef(type, 1);
    Set<RexNode> origins = new AbstractSet<RexNode>() {
      @Override public int size() {
        return 65_536;
      }

      @Override public Iterator<RexNode> iterator() {
        throw new AssertionError("Over-budget origins must not be enumerated");
      }
    };
    Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    mapping.put(a, origins);
    mapping.put(b, origins);
    // 65,536 squared wraps to zero as an int.
    assertNull(
        RelMdExpressionLineage.createAllPossibleExpressions(builder,
            builder.makeCall(SqlStdOperatorTable.MINUS, a, b), mapping, Integer.MAX_VALUE));
  }

  /** Generates UNION ALL plans for expression-lineage tests. */
  private static final class ExpressionLineageProgram {
    private ExpressionLineageProgram() {
    }

    /** Generates a query whose final column references every column of a UNION ALL.
     * For example, {@code sql(2, 2, true)} generates:
     * <pre>{@code
     * SELECT c0, c1, ROW_NUMBER() OVER (PARTITION BY c0 ORDER BY c1) AS computed_value
     * FROM (SELECT c0, c1 FROM events UNION ALL SELECT c0, c1 FROM events) u
     * }</pre> */
    static String sql(int branches, int columns, boolean window) {
      checkArgument(branches >= 1, "branches must be positive");
      checkArgument(columns >= 1, "columns must be positive");
      String fields = IntStream.range(0, columns).mapToObj(i -> "c" + i)
          .collect(Collectors.joining(", "));
      String expression;
      if (window) {
        String partition = columns == 1 ? "" : "partition by "
            + IntStream.range(0, columns - 1).mapToObj(i -> "c" + i)
                .collect(Collectors.joining(", ")) + " ";
        expression = "row_number() over (" + partition + "order by c" + (columns - 1) + ")";
      } else {
        expression = IntStream.range(0, columns).mapToObj(i -> "c" + i)
            .collect(Collectors.joining(" + "));
      }
      String inputs = IntStream.range(0, branches)
          .mapToObj(i -> "select " + fields + " from events")
          .collect(Collectors.joining(" union all "));
      return "select " + fields + ", " + expression + " as computed_value from (" + inputs + ") u";
    }

    /** Returns the relational plan for {@link #sql(int, int, boolean)}. The lineage of
     * its computed column has up to {@code branches ^ columns} candidate expressions. */
    static RelNode rel(int branches, int columns, boolean window) throws Exception {
      String sql = sql(branches, columns, window);
      SchemaPlus schema = Frameworks.createRootSchema(true);
      schema.add("EVENTS", new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          RelDataTypeFactory.Builder builder = typeFactory.builder();
          for (int i = 0; i < columns; i++) {
            builder.add("C" + i, SqlTypeName.INTEGER);
          }
          return builder.build();
        }
      });
      try (Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
          .defaultSchema(schema).build())) {
        SqlNode validated = planner.validate(planner.parse(sql));
        return planner.rel(validated).project();
      }
    }
  }
}
