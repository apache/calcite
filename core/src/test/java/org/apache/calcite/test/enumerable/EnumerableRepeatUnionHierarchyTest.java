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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.HierarchySchema;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Unit tests for
 * {@link EnumerableRepeatUnion}
 * <a href="https://issues.apache.org/jira/browse/CALCITE-2812">[CALCITE-2812]
 * Add algebraic operators to allow expressing recursive queries</a>.
 */
@RunWith(Parameterized.class)
public class EnumerableRepeatUnionHierarchyTest {

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

  @Parameterized.Parameters(name = "{index} : hierarchy(startId:{0}, ascendant:{1}, maxDepth:{2})")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { 1, true, -1, new String[]{EMP1}, "TMP0" },
        { 2, true, -1, new String[]{EMP2, EMP1}, "TMP1" },
        { 3, true, -1, new String[]{EMP3, EMP2, EMP1}, "TMP2" },
        { 4, true, -1, new String[]{EMP4, EMP1}, "TMP3" },
        { 5, true, -1, new String[]{EMP5, EMP2, EMP1}, "TMP4" },
        { 3, true,  0, new String[]{EMP3}, "TMP5" },
        { 3, true,  1, new String[]{EMP3, EMP2}, "TMP6" },
        { 3, true,  2, new String[]{EMP3, EMP2, EMP1}, "TMP7" },
        { 3, true, 10, new String[]{EMP3, EMP2, EMP1}, "TMP8" },

        { 1, false, -1, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5}, "TMP9" },
        { 2, false, -1, new String[]{EMP2, EMP3, EMP5}, "TMP10" },
        { 3, false, -1, new String[]{EMP3}, "TMP11" },
        { 4, false, -1, new String[]{EMP4}, "TMP12" },
        { 1, false,  0, new String[]{EMP1}, "TMP13" },
        { 1, false,  1, new String[]{EMP1, EMP2, EMP4}, "TMP14" },
        { 1, false,  2, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5}, "TMP15" },
        { 1, false, 20, new String[]{EMP1, EMP2, EMP4, EMP3, EMP5}, "TMP16" },
    });
  }

  private final int startId;
  private final int maxDepth;
  private final String fromField;
  private final String toField;
  private final String[] expected;
  private final String transientTable;

  public EnumerableRepeatUnionHierarchyTest(int startId, boolean ascendant,
                                            int maxDepth, String[] expected,
                                            String transientTable) {
    this.startId = startId;
    this.maxDepth = maxDepth;
    this.expected = expected;
    this.transientTable = transientTable;

    if (ascendant) {
      this.fromField = "subordinateid";
      this.toField = "managerid";
    } else {
      this.fromField = "managerid";
      this.toField = "subordinateid";
    }
  }

  @Test public void testHierarchy() {
    final Schema schema = new ReflectiveSchema(new HierarchySchema());
    CalciteAssert.that()
        .withSchema("s", schema)
        .query("?")
        .withRel(createHierarchy())
        .returnsOrdered(expected);
  }

  private Function<RelBuilder, RelNode> createHierarchy() {

    //   WITH RECURSIVE tmp(empid, name) as (
    //       SELECT empid, name FROM emps WHERE empid = <startId>
    //     UNION ALL
    //       SELECT e.empid, e.name FROM tmp t
    //                              JOIN hierarchies h ON t.empid = h.<fromField>
    //                              JOIN emps e        ON h.<toField> = e.empid
    //   )
    //   SELECT empid, name FROM tmp
    return builder -> builder
        .scan("s", "emps")
        .filter(
            builder.equals(
                builder.field("empid"),
                builder.literal(startId)))
        .project(
            builder.field("emps", "empid"),
            builder.field("emps", "name"))

        .transientScan(transientTable)
        .scan("s", "hierarchies")
        .join(
            JoinRelType.INNER,
            builder.equals(
                builder.field(2, transientTable, "empid"),
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
        .repeatUnion(transientTable, true, maxDepth)
        .build();
  }

}

// End EnumerableRepeatUnionHierarchyTest.java
