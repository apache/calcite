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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;

import org.junit.Test;

import java.math.BigDecimal;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableJoin}.
 */
public class EnumerableJoinTest {

  /**
   *
   */
  public static class T1 {
    public Integer c1;
    public String c2;
    public BigDecimal c3;

    public T1(Integer c1, String c2, BigDecimal c3) {
      this.c1 = c1;
      this.c2 = c2;
      this.c3 = c3;
    }
  }

  /**
   *
   */
  public static class T2 {
    public Long c1;
    public String c2;
    public BigDecimal c3;


    public T2(Long c1, String c2, BigDecimal c3) {
      this.c1 = c1;
      this.c2 = c2;
      this.c3 = c3;
    }
  }

  /**
   *
   */
  public static class TestSchema {
    @Override public String toString() {
      return "TestSchema";
    }

    public final T1[] t1 = {
        new T1(1, "1", new BigDecimal("1")),
        new T1(2, "2", new BigDecimal("2"))
    };
    public final T2[] t2 = {
        new T2(1L, "1", new BigDecimal("1")),
        new T2(2L, "2", new BigDecimal("2"))
    };
  }

  @Test public void hashJoinKeysCompareIntAndLong() {
    tester(false, new TestSchema())
        .query(
            "select t1.*,t2.*  from t1 join t2 on "
                + "t1.c1=t2.c1")
        .returnsUnordered(
            "c1=1; c2=1; c3=1; c10=1; c20=1; c30=1",
            "c1=2; c2=2; c3=2; c10=2; c20=2; c30=2");
  }

  @Test public void hashJoinKeysCompareIntAndDecimal() {
    tester(false, new TestSchema())
        .query(
            "select t1.*,t2.*  from t1 join t2 on "
                + "t1.c1=t2.c3")
        .returnsUnordered(
            "c1=1; c2=1; c3=1; c10=1; c20=1; c30=1",
            "c1=2; c2=2; c3=2; c10=2; c20=2; c30=2");
  }


  @Test public void hashJoinKeysCompareIntAndString() {
    tester(false, new TestSchema())
        .query(
            "select t1.*,t2.*  from t1 join t2 on "
                + "t1.c1=t2.c2")
        .returnsUnordered(
            "c1=1; c2=1; c3=1; c10=1; c20=1; c30=1",
            "c1=2; c2=2; c3=2; c10=2; c20=2; c30=2");
  }

  @Test public void hashJoinKeysCompareMultiDifferentKeys() {
    tester(false, new TestSchema())
        .query(
            "select t1.*,t2.*  from t1 join t2 on "
                + "t1.c1=t2.c2 and t1.c2=t2.c3 and t1.c1=t2.c3 and t1.c1=t2.c1")
        .returnsUnordered(
            "c1=1; c2=1; c3=1; c10=1; c20=1; c30=1",
            "c1=2; c2=2; c3=2; c10=2; c20=2; c30=2");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}

// End EnumerableJoinTest.java
