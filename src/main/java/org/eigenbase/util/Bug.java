/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.util;

/**
 * Holder for a list of constants describing which bugs which have not been
 * fixed.
 *
 * <p>You can use these constants to control the flow of your code. For example,
 * suppose that bug FNL-123 causes the "INSERT" statement to return an incorrect
 * row-count, and you want to disable unit tests. You might use the constant in
 * your code as follows:
 *
 * <blockquote>
 * <pre>Statement stmt = connection.createStatement();
 * int rowCount = stmt.execute(
 *     "INSERT INTO FemaleEmps SELECT * FROM Emps WHERE gender = 'F'");
 * if (Bug.Fnl123Fixed) {
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
 * @author jhyde
 * @version $Id$
 * @since 2006/3/2
 */
public abstract class Bug
{
    //~ Static fields/initializers ---------------------------------------------

    // -----------------------------------------------------------------------
    // Developers should create new fields here, in their own section. This
    // will make merge conflicts much less likely than if everyone is
    // appending.

    public static final boolean Dt239Fixed = false;

    public static final boolean Dt785Fixed = false;

    // angel

    /**
     * Whether dtbug1446 "Window Rank Functions not fully implemented" is fixed.
     */
    public static final boolean Dt1446Fixed = false;

    // jhyde

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FNL-3">issue
     * Fnl-3</a> is fixed.
     */
    public static final boolean Fnl3Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FNL-77">issue FNL-77:
     * Fennel calc returns CURRENT_TIMESTAMP in UTC, should be local time</a> is
     * fixed.
     */
    public static final boolean Fnl77Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-327">issue
     * FRG-327: AssertionError while translating IN list that contains null</a>
     * is fixed.
     */
    public static final boolean Frg327Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-377">issue
     * FRG-377: Regular character set identifiers defined in SQL:2008 spec like
     * :ALPHA:, * :UPPER:, :LOWER:, ... etc. are not yet implemented in
     * SIMILAR TO expressions.</a> is fixed.
     */
    public static final boolean Frg377Fixed = false;

    /**
     * Whether dtbug1684 "CURRENT_DATE not implemented in fennel calc" is fixed.
     */
    public static final boolean Dt1684Fixed = false;

    // kkrueger

    // mberkowitz

    // murali

    // rchen

    // schoi

    // stephan

    // tleung

    // xluo

    // zfong

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FNL-25">issue
     * FNL-25</a> is fixed. (also filed as dtbug 153)
     */
    public static final boolean Fnl25Fixed = false;

    // johnk

    // jouellette

    // jpham

    // jvs

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-73">issue FRG-73:
     * miscellaneous bugs with nested comments</a> is fixed.
     */
    public static final boolean Frg73Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-78">issue FRG-78:
     * collation clause should be on expression instead of identifier</a> is
     * fixed.
     */
    public static final boolean Frg78Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-140">issue
     * FRG-140: validator does not accept column qualified by schema name</a> is
     * fixed.
     */
    public static final boolean Frg140Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-187">issue
     * FRG-187: FarragoAutoVmOperatorTest.testOverlapsOperator fails</a> is
     * fixed.
     */
    public static final boolean Frg187Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-189">issue
     * FRG-189: FarragoAutoVmOperatorTest.testSelect fails</a> is fixed.
     */
    public static final boolean Frg189Fixed = false;

    // elin

    // fliang

    // fzhang

    // hersker

    // jack

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-254">issue
     * FRG-254: environment-dependent failure for
     * SqlOperatorTest.testPrefixPlusOperator</a> is fixed.
     */
    public static final boolean Frg254Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-282">issue
     * FRG-282: Support precision in TIME and TIMESTAMP data types</a> is fixed.
     */
    public static final boolean Frg282Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-283">issue
     * FRG-283: Calc cannot cast VARBINARY values</a> is fixed.
     */
    public static final boolean Frg283Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-296">issue
     * FRG-296: SUBSTRING(string FROM regexp FOR regexp)</a> is fixed.
     */
    public static final boolean Frg296Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-375">issue
     * FRG-375: The expression VALUES ('cd' SIMILAR TO '[a-e^c]d') returns TRUE.
     * It should return FALSE.</a> is fixed.
     */
    public static final boolean Frg375Fixed = false;

    /**
     * Whether <a href="http://issues.eigenbase.org/browse/FRG-378">issue
     * FRG-378: Regular expressions in SIMILAR TO predicates
     * potentially dont match SQL:2008 spec in a few cases.</a> is fixed.
     */
    public static final boolean Frg378Fixed = true;

    /** Temporary. */
    public static final boolean TodoFixed = false;
}

// End Bug.java
