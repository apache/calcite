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
package org.apache.calcite.test.enumerable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test suite using a very wide table.
 */
public class WideTableTest {

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5227">[CALCITE-5227]
   * Janino upgrade breaks SELECTs with many projects causing JVM crashes</a>.
   */
  @Test void testWideTable() {
    String expected = IntStream.range(0, 1000)
        .mapToObj(i -> String.format(Locale.ENGLISH, "$f%d=%d", i, i))
        .collect(Collectors.joining("; "));
    CalciteAssert.that()
        .withSchema("s", new MySchema())
        .withRel(
            builder -> builder
                .scan("s", "WIDE")
                .project(
                    manyProjects(builder))
                .build())
        .returnsUnordered(expected);
  }

  private RexNode[] manyProjects(RelBuilder builder) {
    return IntStream.range(0, 1000)
        .mapToObj(
            i -> builder.call(
            SqlStdOperatorTable.COALESCE,
            builder.field(i),
            builder.literal(0)))
        .collect(Collectors.toList()).toArray(new RexNode[1000]);
  }

  /**
   * A very wide table.
   */
  private static class MyTable extends AbstractTable
      implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      List<String> names = IntStream.range(0, 1000).mapToObj(i -> "field" + i)
          .collect(Collectors.toList());
      List<RelDataType> types = IntStream.range(0, 1000)
          .mapToObj(i -> typeFactory.createJavaType(Integer.class))
          .collect(Collectors.toList());
      return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
      Object[] row = IntStream.range(0, 1000).boxed().toArray();
      List<Object[]> data = new ArrayList<>();
      data.add(row);
      return Linq4j.asEnumerable(data);
    }
  }

  /**
   * Schema which provides a wide table.
   */
  private static class MySchema extends AbstractSchema {

    private final Map<String, Table> tables = Collections.singletonMap("WIDE", new MyTable());

    @Override protected Map<String, Table> getTableMap() {
      return tables;
    }
  }
}
