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
 * <blockquote><pre>{@code
 *   Statement stmt = connection.createStatement();
 *   int rowCount =
 *       stmt.execute("INSERT INTO FemaleEmps\n"
 *           + "SELECT * FROM Emps WHERE gender = 'F'");
 *   if (Bug.CALCITE_123_FIXED) {
 *      assertEquals(rowCount, 5);
 *   }
 * }</pre></blockquote>
 *
 * <p>The usage of the constant is a convenient way to identify the impact of
 * the bug. When someone fixes the bug, they will remove the constant and all
 * usages of it. Also, the constant helps track the propagation of the fix: as
 * the fix is integrated into other branches, the constant will be removed from
 * those branches.
 *
 * <p>This class depends on no other classes.
 * (In the past, a dependency on {@code Util} caused class-loading cycles.)
 */
public abstract class Bug {
  //~ Static fields/initializers ---------------------------------------------

  // -----------------------------------------------------------------------
  // Developers should create new fields here, in their own section. This
  // will make merge conflicts much less likely than if everyone is
  // appending.

  public static final boolean DT239_FIXED = false;

  public static final boolean DT785_FIXED = false;

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
   * Improve RelMdPredicates performance</a> is fixed. */
  public static final boolean CALCITE_2401_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2869">[CALCITE-2869]
   * JSON data type support</a> is fixed. */
  public static final boolean CALCITE_2869_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3243">[CALCITE-3243]
   * Incomplete validation of operands in JSON functions</a> is fixed. */
  public static final boolean CALCITE_3243_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4204">[CALCITE-4204]
   * Intermittent precision in Druid results when using aggregation functions over columns of type
   * DOUBLE</a> is fixed. */
  public static final boolean CALCITE_4204_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4205">[CALCITE-4205]
   * DruidAdapterIT#testDruidTimeFloorAndTimeParseExpressions2 fails</a> is fixed. */
  public static final boolean CALCITE_4205_FIXED = false;
  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4213">[CALCITE-4213]
   * Druid plans with small intervals should be chosen over full interval scan plus filter</a> is
   * fixed. */
  public static final boolean CALCITE_4213_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4645">[CALCITE-4645]
   * In Elasticsearch adapter, a range predicate should be translated to a range query</a> is
   * fixed. */
  public static final boolean CALCITE_4645_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5422">[CALCITE-5422]
   * MILLISECOND and MICROSECOND units in INTERVAL literal</a> is fixed. */
  public static final boolean CALCITE_5422_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6092">[CALCITE-6092]
   * Invalid test cases in CAST String to Time</a> is fixed.
   * Fix to be available with Avatica 1.24.0 [CALCITE-6053] */
  public static final boolean CALCITE_6092_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6270">[CALCITE-6270]
   * Support FORMAT in CAST from Numeric and BYTES to String (Enabled in BigQuery)</a> is fixed. */
  public static final boolean CALCITE_6270_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6367">[CALCITE-6367]
   * Add timezone support for FORMAT clause in CAST (enabled in BigQuery)</a> is fixed. */
  public static final boolean CALCITE_6367_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE/issues/CALCITE-6391">
   * [CALCITE-6391] Apply mapping to RelCompositeTrait does not apply it to wrapped traits</a>
   * is fixed. */
  public static final boolean CALCITE_6391_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE/issues/CALCITE-6293">
   * [CALCITE-6293] Support OR condition in Arrow adapter</a> is fixed. */
  public static final boolean CALCITE_6293_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE/issues/CALCITE-6294">
   * [CALCITE-6294] Support IN filter in Arrow adapter</a> is fixed. */
  public static final boolean CALCITE_6294_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6328">[CALCITE-6328]
   * The BigQuery functions SAFE_* do not match the BigQuery specification</a>
   * is fixed. */
  public static final boolean CALCITE_6328_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6611">[CALCITE-6611]
   * Rules that modify the sort collation cannot be applied in VolcanoPlanner</a>
   * is fixed. */
  public static final boolean CALCITE_6611_FIXED = false;

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
  @SuppressWarnings("unused")
  public static boolean upgrade(String remark) {
    return false;
  }
}
