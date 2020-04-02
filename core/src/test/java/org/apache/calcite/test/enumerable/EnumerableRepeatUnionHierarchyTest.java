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

import org.apache.calcite.adapter.enumerable.EnumerableRepeatUnion;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.HierarchySchema;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Unit tests for
 * {@link EnumerableRepeatUnion}
 * <a href="https://issues.apache.org/jira/browse/CALCITE-2812">[CALCITE-2812]
 * Add algebraic operators to allow expressing recursive queries</a>.
 */
class EnumerableRepeatUnionHierarchyTest {

  // Tests for the following hierarchy:
  //      Emp1
  //      /  \
  //    Emp2  Emp4
  //    /  \
  // Emp3   Emp5

  private static final String EMP1 = "empid=1; name=Emp1";
  private static final String EMP2 = "empid=2; name=Emp2";
  private static final String EMP3 = "empid=3; name=Emp3";
  private static final String EMP4 = "empid=4; name=Emp4";
  private static final String EMP5 = "empid=5; name=Emp5";

  private static final int[] ID1 = new int[]{1};
  private static final String ID1_STR = Arrays.toString(ID1);
  private static final int[] ID2 = new int[]{2};
  private static final String ID2_STR = Arrays.toString(ID2);
  private static final int[] ID3 = new int[]{3};
  private static final String ID3_STR = Arrays.toString(ID3);
  private static final int[] ID4 = new int[]{4};
  private static final String ID4_STR = Arrays.toString(ID4);
  private static final int[] ID5 = new int[]{5};
  private static final String ID5_STR = Arrays.toString(ID5);
  private static final int[] ID3_5 = new int[]{3, 5};
  private static final String ID3_5_STR = Arrays.toString(ID3_5);
  private static final int[] ID1_3 = new int[]{1, 3};
  private static final String ID1_3_STR = Arrays.toString(ID1_3);

  public static Iterable<Object[]> data() {

    return Arrays.asList(new Object[][] {
        { true, ID1, ID1_STR, true, -1, new String[]{EMP1} },
        { true, ID2, ID2_STR, true, -2, new String[]{EMP2, EMP1} },
        { true, ID3, ID3_STR, true, -1, new String[]{EMP3, EMP2, EMP1} },
        { true, ID4, ID4_STR, true, -5, new String[]{EMP4, EMP1} },
        { true, ID5, ID5_STR, true, -1, new String[]{EMP5, EMP2, EMP1} },
        { true, ID3, ID3_STR, true,  0, new String[]{EMP3} },
        { true, ID3, ID3_STR, true,  1, new String[]{EMP3, EMP2} },
        { true, ID3, ID3_STR, true,  2, new String[]{EMP3, EMP2, EMP1} },
        { true, ID3, ID3_STR, true, 10, new String[]{EMP3, EMP2, EMP1} },

        { true, ID1, ID1_STR, false, -1, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5} },
        { true, ID2, ID2_STR, false, -10, new String[]{EMP2, EMP3, EMP5} },
        { true, ID3, ID3_STR, false, -100, new String[]{EMP3} },
        { true, ID4, ID4_STR, false, -1, new String[]{EMP4} },
        { true, ID1, ID1_STR, false,  0, new String[]{EMP1} },
        { true, ID1, ID1_STR, false,  1, new String[]{EMP1, EMP2, EMP4} },
        { true, ID1, ID1_STR, false,  2, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5} },
        { true, ID1, ID1_STR, false, 20, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5} },

        // tests to verify all=true vs all=false
        { true, ID3_5, ID3_5_STR, true, -1, new String[]{EMP3, EMP5, EMP2, EMP2, EMP1, EMP1} },
        { false, ID3_5, ID3_5_STR, true, -1, new String[]{EMP3, EMP5, EMP2, EMP1} },
        { true, ID3_5, ID3_5_STR, true, 0, new String[]{EMP3, EMP5} },
        { false, ID3_5, ID3_5_STR, true, 0, new String[]{EMP3, EMP5} },
        { true, ID3_5, ID3_5_STR, true, 1, new String[]{EMP3, EMP5, EMP2, EMP2} },
        { false, ID3_5, ID3_5_STR, true, 1, new String[]{EMP3, EMP5, EMP2} },
        { true, ID1_3, ID1_3_STR, false, -1, new String[]{EMP1, EMP3, EMP2, EMP4, EMP3, EMP5} },
        { false, ID1_3, ID1_3_STR, false, -1, new String[]{EMP1, EMP3, EMP2, EMP4, EMP5} },
    });
  }

  @ParameterizedTest(name = "{index} : hierarchy(startIds:{2}, ascendant:{3}, "
      + "maxDepth:{4}, all:{0})")
  @MethodSource("data")
  public void testHierarchy(
      boolean all,
      int[] startIds,
      String startIdsStr,
      boolean ascendant,
      int maxDepth,
      String[] expected) {
    final String fromField;
    final String toField;
    if (ascendant) {
      fromField = "subordinateid";
      toField = "managerid";
    } else {
      fromField = "managerid";
      toField = "subordinateid";
    }

    final Schema schema = new ReflectiveSchema(new HierarchySchema());
    CalciteAssert.that()
        .withSchema("s", schema)
        .query("?")
        .withRel(buildHierarchy(all, startIds, fromField, toField, maxDepth))
        .returnsOrdered(expected);
  }

  private Function<RelBuilder, RelNode> buildHierarchy(
      boolean all,
      int[] startIds,
      String fromField,
      String toField,
      int maxDepth) {

    //   WITH RECURSIVE delta(empid, name) as (
    //       SELECT empid, name FROM emps WHERE empid IN (<startIds>)
    //     UNION [ALL]
    //       SELECT e.empid, e.name FROM delta d
    //                              JOIN hierarchies h ON d.empid = h.<fromField>
    //                              JOIN emps e        ON h.<toField> = e.empid
    //   )
    //   SELECT empid, name FROM delta
    return builder -> {
      builder
          .scan("s", "emps");

      final List<RexNode> filters = new ArrayList<>();
      for (int startId : startIds) {
        filters.add(
            builder.equals(
                builder.field("empid"),
                builder.literal(startId)));
      }

      builder
        .filter(
            builder.or(filters))
        .project(
            builder.field("emps", "empid"),
            builder.field("emps", "name"))

        .transientScan("#DELTA#")
        .scan("s", "hierarchies")
        .join(
            JoinRelType.INNER,
            builder.equals(
                builder.field(2, "#DELTA#", "empid"),
                builder.field(2, "hierarchies", fromField)))
        .scan("s", "emps")
        .join(
            JoinRelType.INNER,
            builder.equals(
                builder.field(2, "hierarchies", toField),
                builder.field(2, "emps", "empid")))
        .project(
            builder.field("emps", "empid"),
            builder.field("emps", "name"))
        .repeatUnion("#DELTA#", all, maxDepth);

      return builder.build();
    };
  }

}
