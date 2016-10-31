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

import org.apache.calcite.adapter.spark.SparkRel;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.sql.SQLException;

/**
 * Tests for using Calcite with Spark as an internal engine, as implemented by
 * the {@link org.apache.calcite.adapter.spark} package.
 */
public class SparkAdapterTest {
  /**
   * Tests a VALUES query evaluated using Spark.
   * There are no data sources.
   */
  @Test public void testValues() throws SQLException {
    // Insert a spurious reference to a class in Calcite's Spark adapter.
    // Otherwise this test doesn't depend on the Spark module at all, and
    // Javadoc gets confused.
    Util.discard(SparkRel.class);

    CalciteAssert.that()
        .with(CalciteAssert.Config.SPARK)
        .query("select *\n"
            + "from (values (1, 'a'), (2, 'b'))")
        .returns("EXPR$0=1; EXPR$1=a\n"
            + "EXPR$0=2; EXPR$1=b\n")
        .explainContains("SparkToEnumerableConverter\n"
            + "  SparkValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])");
  }

  /** Tests values followed by filter, evaluated by Spark. */
  @Test public void testValuesFilter() throws SQLException {
    CalciteAssert.that()
        .with(CalciteAssert.Config.SPARK)
        .query("select *\n"
            + "from (values (1, 'a'), (2, 'b')) as t(x, y)\n"
            + "where x < 2")
        .returns("X=1; Y=a\n")
        .explainContains("PLAN=SparkToEnumerableConverter\n"
            + "  SparkCalc(expr#0..1=[{inputs}], expr#2=[2], expr#3=[<($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "    SparkValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n");
  }
}

// End SparkAdapterTest.java
