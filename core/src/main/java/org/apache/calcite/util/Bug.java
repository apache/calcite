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
package org.apache.calcite.util;

/**
 * Holder for a list of constants describing which bugs which have not been
 * fixed.
 *
 * <p>You can use these constants to control the flow of your code. For example,
 * suppose that bug CALCITE-123 causes the "INSERT" statement to return an
 * incorrect row-count, and you want to disable unit tests. You might use the
 * constant in your code as follows:
 *
 * <blockquote>
 * <pre>Statement stmt = connection.createStatement();
 * int rowCount = stmt.execute(
 *     "INSERT INTO FemaleEmps SELECT * FROM Emps WHERE gender = 'F'");
 * if (Bug.CALCITE_123_FIXED) {
 *    assertEquals(rowCount, 5);
 * }</pre>
 * </blockquote>
 *
 * <p>The usage of the constant is a convenient way to identify the impact of
 * the bug. When someone fixes the bug, they will remove the constant and all
 * usages of it. Also, the constant helps track the propagation of the fix: as
 * the fix is integrated into other branches, the constant will be removed from
 * those branches.</p>
 *
 */
public abstract class Bug {
  //~ Static fields/initializers ---------------------------------------------

  // -----------------------------------------------------------------------
  // Developers should create new fields here, in their own section. This
  // will make merge conflicts much less likely than if everyone is
  // appending.

  public static final boolean DT239_FIXED = false;

  public static final boolean DT785_FIXED = false;

  // jhyde

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FNL-3">issue
   * Fnl-3</a> is fixed.
   */
  public static final boolean FNL3_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-327">issue
   * FRG-327: AssertionError while translating IN list that contains null</a>
   * is fixed.
   */
  public static final boolean FRG327_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-377">issue
   * FRG-377: Regular character set identifiers defined in SQL:2008 spec like
   * :ALPHA:, * :UPPER:, :LOWER:, ... etc. are not yet implemented in
   * SIMILAR TO expressions.</a> is fixed.
   */
  public static final boolean FRG377_FIXED = false;

  /**
   * Whether dtbug1684 "CURRENT_DATE not implemented in fennel calc" is fixed.
   */
  public static final boolean DT1684_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FNL-25">issue
   * FNL-25</a> is fixed. (also filed as dtbug 153)
   */
  public static final boolean FNL25_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-73">issue FRG-73:
   * miscellaneous bugs with nested comments</a> is fixed.
   */
  public static final boolean FRG73_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-78">issue FRG-78:
   * collation clause should be on expression instead of identifier</a> is
   * fixed.
   */
  public static final boolean FRG78_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-189">issue
   * FRG-189: FarragoAutoVmOperatorTest.testSelect fails</a> is fixed.
   */
  public static final boolean FRG189_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-254">issue
   * FRG-254: environment-dependent failure for
   * SqlOperatorTest.testPrefixPlusOperator</a> is fixed.
   */
  public static final boolean FRG254_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-282">issue
   * FRG-282: Support precision in TIME and TIMESTAMP data types</a> is fixed.
   */
  public static final boolean FRG282_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-296">issue
   * FRG-296: SUBSTRING(string FROM regexp FOR regexp)</a> is fixed.
   */
  public static final boolean FRG296_FIXED = false;

  /**
   * Whether <a href="http://issues.eigenbase.org/browse/FRG-375">issue
   * FRG-375: The expression VALUES ('cd' SIMILAR TO '[a-e^c]d') returns TRUE.
   * It should return FALSE.</a> is fixed.
   */
  public static final boolean FRG375_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-194">[CALCITE-194]
   * Array items in MongoDB adapter</a> is fixed. */
  public static final boolean CALCITE_194_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-673">[CALCITE-673]
   * Timeout executing joins against MySQL</a> is fixed. */
  public static final boolean CALCITE_673_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1048">[CALCITE-1048]
   * Make metadata more robust</a> is fixed. */
  public static final boolean CALCITE_1048_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[CALCITE-1045]
   * Decorrelate sub-queries in Project and Join</a> is fixed. */
  public static final boolean CALCITE_1045_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2400">[CALCITE-2400]
   * Allow standards-compliant column ordering for NATURAL JOIN and JOIN USING
   * when dynamic tables are used</a> is fixed. */
  public static final boolean CALCITE_2400_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2401">[CALCITE-2401]
   * Improve RelMdPredicates performance</a>
   */
  public static final boolean CALCITE_2401_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2539">[CALCITE-2539]
   * Several test case not passed in CalciteSqlOperatorTest.java</a> is fixed. */
  public static final boolean CALCITE_2539_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2869">[CALCITE-2869]
   * JSON data type support</a> is fixed. */
  public static final boolean CALCITE_2869_FIXED = false;

  /**
   * Use this to flag temporary code.
   */
  public static final boolean TODO_FIXED = false;

  /**
   * Use this method to flag temporary code.
   *
   * <p>Example #1:
   * <blockquote><pre>
   * if (Bug.remark("baz fixed") == null) {
   *   baz();
   * }</pre></blockquote>
   *
   * <p>Example #2:
   * <blockquote><pre>
   * /&#42;&#42; &#64;see Bug#remark Remove before checking in &#42;/
   * void uselessMethod() {}
   * </pre></blockquote>
   */
  public static <T> T remark(T remark) {
    return remark;
  }

  /**
   * Use this method to flag code that should be re-visited after upgrading
   * a component.
   *
   * <p>If the intended change is that a class or member be removed, flag
   * instead using a {@link Deprecated} annotation followed by a comment such as
   * "to be removed before 2.0".
   */
  public static boolean upgrade(String remark) {
    Util.discard(remark);
    return false;
  }
}

// End Bug.java
