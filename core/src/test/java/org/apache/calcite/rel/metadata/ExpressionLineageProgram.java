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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

/** Generates storage-free SQL programs whose expression lineage has
 * {@code branches ^ columns} candidate substitutions. Shared by regression
 * tests and standalone measurements. */
final class ExpressionLineageProgram {
  private ExpressionLineageProgram() {
  }

  /** Generates a SQL program with passthrough columns and a final computed
   * column, either an arithmetic expression or a window expression. */
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

  /** Parses and validates the generated SQL using only Calcite. */
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
