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
package org.eigenbase.test;

import java.math.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for the {@link org.eigenbase.sarg} class library.
 *
 * <p>NOTE: This class lives in org.eigenbase.test rather than
 * org.eigenbase.sarg by design: we want to make sure we're only testing via the
 * public interface.
 */
public class SargTest {
    //~ Enums ------------------------------------------------------------------

    enum Zodiac
    {
        AQUARIUS, ARIES, CANCER, CAPRICORN, GEMINI, LEO, LIBRA, PISCES,
        SAGITTARIUS, SCORPIO, TAURUS, VIRGO
    }

    //~ Instance fields --------------------------------------------------------

    private SargFactory sargFactory;

    private RexBuilder rexBuilder;

    private RelDataType intType;

    private RelDataType stringType;

    private RexNode intLiteral7;

    private RexNode intLiteral8point5;

    private RexNode intLiteral490;

    private SargIntervalExpr [] exprs;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SargTest.
     */
    public SargTest() {
    }

    //~ Methods ----------------------------------------------------------------

    @Before public void setUp() {
      // create some reusable fixtures

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
        intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        intType = typeFactory.createTypeWithNullability(intType, true);
        stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
        stringType = typeFactory.createTypeWithNullability(stringType, true);

        rexBuilder = new RexBuilder(typeFactory);
        intLiteral7 = rexBuilder.makeExactLiteral(
            new BigDecimal(7),
            intType);
        intLiteral490 =
            rexBuilder.makeExactLiteral(
                new BigDecimal(490),
                intType);
        intLiteral8point5 =
            rexBuilder.makeExactLiteral(
                new BigDecimal("8.5"),
                intType);

        sargFactory = new SargFactory(rexBuilder);
    }

    @Test public void testDefaultEndpoint() {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);
        assertEquals(
            "-infinity",
            ep.toString());
    }

    @Test public void testInfiniteEndpoint() {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);
        ep.setInfinity(1);
        assertEquals(
            "+infinity",
            ep.toString());
        ep.setInfinity(-1);
        assertEquals(
            "-infinity",
            ep.toString());
    }

    @Test public void testFiniteEndpoint() {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);

        ep.setFinite(
            SargBoundType.LOWER,
            SargStrictness.OPEN,
            intLiteral7);
        assertEquals(
            "> 7",
            ep.toString());

        ep.setFinite(
            SargBoundType.LOWER,
            SargStrictness.CLOSED,
            intLiteral7);
        assertEquals(
            ">= 7",
            ep.toString());

        ep.setFinite(
            SargBoundType.UPPER,
            SargStrictness.OPEN,
            intLiteral7);
        assertEquals(
            "< 7",
            ep.toString());

        ep.setFinite(
            SargBoundType.UPPER,
            SargStrictness.CLOSED,
            intLiteral7);
        assertEquals(
            "<= 7",
            ep.toString());

        // after rounding, "> 8.5" is equivalent to ">= 9" over the domain
        // of integers
        ep.setFinite(
            SargBoundType.LOWER,
            SargStrictness.OPEN,
            intLiteral8point5);
        assertEquals(
            ">= 9",
            ep.toString());

        ep.setFinite(
            SargBoundType.LOWER,
            SargStrictness.CLOSED,
            intLiteral8point5);
        assertEquals(
            ">= 9",
            ep.toString());

        ep.setFinite(
            SargBoundType.UPPER,
            SargStrictness.OPEN,
            intLiteral8point5);
        assertEquals(
            "< 9",
            ep.toString());

        ep.setFinite(
            SargBoundType.UPPER,
            SargStrictness.CLOSED,
            intLiteral8point5);
        assertEquals(
            "< 9",
            ep.toString());
    }

    @Test public void testNullEndpoint() {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);

        ep.setFinite(
            SargBoundType.LOWER,
            SargStrictness.OPEN,
            sargFactory.newNullLiteral());
        assertEquals(
            "> null",
            ep.toString());
    }

    @Test public void testTouchingEndpoint() {
        SargMutableEndpoint ep1 = sargFactory.newEndpoint(intType);
        SargMutableEndpoint ep2 = sargFactory.newEndpoint(intType);

        // "-infinity" does not touch "-infinity" (seems like something you
        // could argue for hours late at night in a college dorm)
        assertFalse(ep1.isTouching(ep2));

        // "< 7" does not touch "> 7"
        ep1.setFinite(
            SargBoundType.UPPER,
            SargStrictness.OPEN,
            intLiteral7);
        ep2.setFinite(
            SargBoundType.LOWER,
            SargStrictness.OPEN,
            intLiteral7);
        assertFalse(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);

        // "< 7" does touch ">= 7"
        ep2.setFinite(
            SargBoundType.LOWER,
            SargStrictness.CLOSED,
            intLiteral7);
        assertTrue(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);

        // "<= 7" does touch ">= 7"
        ep1.setFinite(
            SargBoundType.LOWER,
            SargStrictness.CLOSED,
            intLiteral7);
        assertTrue(ep1.isTouching(ep2));
        assertEquals(
            0,
            ep1.compareTo(ep2));

        // "<= 7" does not touch ">= 490"
        ep2.setFinite(
            SargBoundType.LOWER,
            SargStrictness.CLOSED,
            intLiteral490);
        assertFalse(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);
    }

    @Test public void testDefaultIntervalExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        assertEquals(
            "(-infinity, +infinity)",
            interval.toString());
    }

    @Test public void testPointExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setPoint(intLiteral7);
        assertTrue(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals(
            "[7]",
            interval.toString());
        assertEquals(
            "[7]",
            interval.evaluate().toString());
    }

    @Test public void testRangeIntervalExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);

        interval.setLower(intLiteral7, SargStrictness.CLOSED);
        interval.setUpper(intLiteral490, SargStrictness.CLOSED);
        assertRange(interval);
        assertEquals(
            "[7, 490]",
            interval.toString());
        assertEquals(
            "[7, 490]",
            interval.evaluate().toString());

        interval.unsetLower();
        assertRange(interval);
        assertEquals(
            "(-infinity, 490]",
            interval.toString());
        assertEquals(
            "(null, 490]",
            interval.evaluate().toString());

        interval.setLower(intLiteral7, SargStrictness.CLOSED);
        interval.unsetUpper();
        assertRange(interval);
        assertEquals(
            "[7, +infinity)",
            interval.toString());
        assertEquals(
            "[7, +infinity)",
            interval.evaluate().toString());

        interval.setUpper(intLiteral490, SargStrictness.OPEN);
        assertRange(interval);
        assertEquals(
            "[7, 490)",
            interval.toString());
        assertEquals(
            "[7, 490)",
            interval.evaluate().toString());

        interval.setLower(intLiteral7, SargStrictness.OPEN);
        assertRange(interval);
        assertEquals(
            "(7, 490)",
            interval.toString());
        assertEquals(
            "(7, 490)",
            interval.evaluate().toString());
    }

    private void assertRange(SargIntervalExpr interval)
    {
        assertFalse(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
    }

    @Test public void testNullExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setNull();
        assertTrue(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals(
            "[null] NULL_MATCHES_NULL",
            interval.toString());
        assertEquals(
            "[null]",
            interval.evaluate().toString());
    }

    @Test public void testEmptyExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setEmpty();
        assertTrue(interval.isEmpty());
        assertFalse(interval.isUnconstrained());
        assertEquals(
            "()",
            interval.toString());
        assertEquals(
            "()",
            interval.evaluate().toString());
    }

    @Test public void testUnconstrainedExpr() {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setEmpty();
        assertFalse(interval.isUnconstrained());
        interval.setUnconstrained();
        assertTrue(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals(
            "(-infinity, +infinity)",
            interval.toString());
        assertEquals(
            "(-infinity, +infinity)",
            interval.evaluate().toString());
    }

    @Test public void testSetExpr() {
        SargIntervalExpr interval1 = sargFactory.newIntervalExpr(intType);
        SargIntervalExpr interval2 = sargFactory.newIntervalExpr(intType);

        interval1.setLower(intLiteral7, SargStrictness.OPEN);
        interval2.setUpper(intLiteral490, SargStrictness.OPEN);

        SargSetExpr intersectExpr =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.INTERSECTION);
        intersectExpr.addChild(interval1);
        intersectExpr.addChild(interval2);
        assertEquals(
            "INTERSECTION( (7, +infinity) (-infinity, 490) )",
            intersectExpr.toString());
        assertEquals(
            "(7, 490)",
            intersectExpr.evaluate().toString());

        SargSetExpr unionExpr =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.UNION);
        unionExpr.addChild(interval1);
        unionExpr.addChild(interval2);
        assertEquals(
            "UNION( (7, +infinity) (-infinity, 490) )",
            unionExpr.toString());
        assertEquals(
            "(null, +infinity)",
            unionExpr.evaluate().toString());

        // NOTE jvs 17-Apr-2006:  See
        // http://issues.eigenbase.org/browse/LDB-60) for why the
        // expected result is what it is.

        SargSetExpr complementExpr =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.COMPLEMENT);
        complementExpr.addChild(interval1);
        assertEquals(
            "COMPLEMENT( (7, +infinity) )",
            complementExpr.toString());
        assertEquals(
            "(null, 7]",
            complementExpr.evaluate().toString());
    }

    @Test public void testComplement() {
        // test
        //   complement ( union (interval1, interval2) )
        // it is evaluated as
        //   intersect (complement(interval1), complement(interval2))
        //
        // commonly seen in sql expressins like
        //   not (a in ( list))
        // or
        //   a not in (list)
        // (rchen 2006-08-18) note the latter does not have SQL support yet.
        //
        SargIntervalExpr interval1 = sargFactory.newIntervalExpr(intType);
        SargIntervalExpr interval2 = sargFactory.newIntervalExpr(intType);
        SargIntervalExpr interval3 = sargFactory.newIntervalExpr(intType);

        interval1.setLower(intLiteral7, SargStrictness.CLOSED);
        interval1.setUpper(intLiteral7, SargStrictness.CLOSED);

        interval2.setLower(intLiteral490, SargStrictness.CLOSED);
        interval2.setUpper(intLiteral490, SargStrictness.CLOSED);

        interval3.setUpper(intLiteral490, SargStrictness.OPEN);

        // REVIEW (ruchen 2006-08-18) lower and upper extremems are different
        // in the complement result:
        //     the lower is null while the upper is +infinity.
        // Not sure why the lower needs to be null.
        SargSetExpr complementExpr1 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.COMPLEMENT);
        complementExpr1.addChild(interval1);
        assertEquals(
            "UNION( (null, 7) (7, +infinity) )",
            complementExpr1.evaluate().toString());

        SargSetExpr complementExpr2 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.COMPLEMENT);
        complementExpr2.addChild(interval2);
        assertEquals(
            "UNION( (null, 490) (490, +infinity) )",
            complementExpr2.evaluate().toString());

        SargSetExpr intersectExpr1 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.INTERSECTION);
        intersectExpr1.addChild(complementExpr1);
        intersectExpr1.addChild(complementExpr2);

        assertEquals(
            "UNION( (null, 7) (7, 490) (490, +infinity) )",
            intersectExpr1.evaluate().toString());

        SargSetExpr intersectExpr2 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.INTERSECTION);
        intersectExpr2.addChild(complementExpr2);
        intersectExpr2.addChild(complementExpr1);

        assertEquals(
            "UNION( (null, 7) (7, 490) (490, +infinity) )",
            intersectExpr2.evaluate().toString());

        SargSetExpr intersectExpr3 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.INTERSECTION);
        intersectExpr3.addChild(complementExpr1);
        intersectExpr3.addChild(interval3);

        assertEquals(
            "UNION( (null, 7) (7, 490) )",
            intersectExpr3.evaluate().toString());

        SargSetExpr unionExpr1 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.UNION);
        unionExpr1.addChild(interval1);
        unionExpr1.addChild(interval2);

        SargSetExpr complementExpr3 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.COMPLEMENT);
        complementExpr3.addChild(unionExpr1);

        assertEquals(
            "UNION( (null, 7) (7, 490) (490, +infinity) )",
            complementExpr3.evaluate().toString());

        SargSetExpr unionExpr2 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.UNION);
        unionExpr2.addChild(interval2);
        unionExpr2.addChild(interval1);

        SargSetExpr complementExpr4 =
            sargFactory.newSetExpr(
                intType,
                SargSetOperator.COMPLEMENT);
        complementExpr4.addChild(unionExpr2);
        assertEquals(
            "UNION( (null, 7) (7, 490) (490, +infinity) )",
            complementExpr4.evaluate().toString());
    }

    @Test public void testUnion() {
        exprs = new SargIntervalExpr[11];
        for (int i = 0; i < 11; ++i) {
            exprs[i] = sargFactory.newIntervalExpr(stringType);
        }

        exprs[0].setPoint(createCoordinate(Zodiac.AQUARIUS));

        exprs[1].setPoint(createCoordinate(Zodiac.LEO));

        exprs[2].setUpper(
            createCoordinate(Zodiac.CAPRICORN),
            SargStrictness.CLOSED);

        exprs[3].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.OPEN);

        exprs[4].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);

        exprs[5].setNull();

        exprs[6].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);
        exprs[6].setUpper(
            createCoordinate(Zodiac.PISCES),
            SargStrictness.CLOSED);

        exprs[7].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);
        exprs[7].setUpper(
            createCoordinate(Zodiac.SCORPIO),
            SargStrictness.CLOSED);

        exprs[8].setLower(
            createCoordinate(Zodiac.ARIES),
            SargStrictness.CLOSED);
        exprs[8].setUpper(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);

        exprs[9].setLower(
            createCoordinate(Zodiac.ARIES),
            SargStrictness.CLOSED);
        exprs[9].setUpper(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.OPEN);

        exprs[10].setEmpty();

        checkUnion(
            0,
            2,
            5,
            "[null, 'CAPRICORN']");

        checkUnion(
            2,
            5,
            0,
            "[null, 'CAPRICORN']");

        checkUnion(
            5,
            6,
            7,
            "UNION( [null] ['GEMINI', 'SCORPIO'] )");

        checkUnion(
            8,
            4,
            5,
            "UNION( [null] ['ARIES', +infinity) )");

        checkUnion(
            9,
            4,
            5,
            "UNION( [null] ['ARIES', +infinity) )");

        checkUnion(
            7,
            8,
            9,
            "['ARIES', 'SCORPIO']");

        checkUnion(
            6,
            7,
            10,
            "['GEMINI', 'SCORPIO']");

        checkUnion(
            5,
            6,
            0,
            "UNION( [null] ['AQUARIUS'] ['GEMINI', 'PISCES'] )");

        checkUnion(
            10,
            9,
            5,
            "UNION( [null] ['ARIES', 'GEMINI') )");

        checkUnion(
            9,
            8,
            7,
            "['ARIES', 'SCORPIO']");

        checkUnion(
            3,
            9,
            1,
            "UNION( ['ARIES', 'GEMINI') ('GEMINI', +infinity) )");
    }

    @Test public void testIntersection() {
        exprs = new SargIntervalExpr[11];
        for (int i = 0; i < 11; ++i) {
            exprs[i] = sargFactory.newIntervalExpr(stringType);
        }

        exprs[0].setUnconstrained();

        exprs[1].setPoint(createCoordinate(Zodiac.LEO));

        exprs[2].setUpper(
            createCoordinate(Zodiac.CAPRICORN),
            SargStrictness.CLOSED);

        exprs[3].setLower(
            createCoordinate(Zodiac.CANCER),
            SargStrictness.OPEN);

        exprs[4].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);

        exprs[5].setNull();

        exprs[6].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);
        exprs[6].setUpper(
            createCoordinate(Zodiac.PISCES),
            SargStrictness.CLOSED);

        exprs[7].setLower(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);
        exprs[7].setUpper(
            createCoordinate(Zodiac.SCORPIO),
            SargStrictness.CLOSED);

        exprs[8].setLower(
            createCoordinate(Zodiac.ARIES),
            SargStrictness.CLOSED);
        exprs[8].setUpper(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.CLOSED);

        exprs[9].setLower(
            createCoordinate(Zodiac.ARIES),
            SargStrictness.CLOSED);
        exprs[9].setUpper(
            createCoordinate(Zodiac.GEMINI),
            SargStrictness.OPEN);

        exprs[10].setEmpty();

        checkIntersection(
            2,
            3,
            0,
            "('CANCER', 'CAPRICORN']");

        checkIntersection(
            0,
            3,
            2,
            "('CANCER', 'CAPRICORN']");

        checkIntersection(
            6,
            7,
            0,
            "['GEMINI', 'PISCES']");

        checkIntersection(
            6,
            7,
            8,
            "['GEMINI']");

        checkIntersection(
            8,
            7,
            6,
            "['GEMINI']");

        checkIntersection(
            9,
            7,
            6,
            "()");
    }

    private void checkUnion(
        int i1,
        int i2,
        int i3,
        String expected)
    {
        checkSetOp(SargSetOperator.UNION, i1, i2, i3, expected);
    }

    private void checkIntersection(
        int i1,
        int i2,
        int i3,
        String expected)
    {
        checkSetOp(SargSetOperator.INTERSECTION, i1, i2, i3, expected);
    }

    private void checkSetOp(
        SargSetOperator setOp,
        int i1,
        int i2,
        int i3,
        String expected)
    {
        SargSetExpr setExpr =
            sargFactory.newSetExpr(
                stringType,
                setOp);
        setExpr.addChild(exprs[i1]);
        setExpr.addChild(exprs[i2]);
        setExpr.addChild(exprs[i3]);
        assertEquals(
            expected,
            setExpr.evaluate().toString());
    }

    private RexNode createCoordinate(Zodiac z)
    {
        return sargFactory.getRexBuilder().makeLiteral(z.toString());
    }

    @Test public void testRexAnalyzer() {
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();
        RexNode pred1, pred2, pred3;
        SargBinding binding;

        RexNode inputRef8 = rexBuilder.makeInputRef(intType, 8);
        RexNode inputRef9 = rexBuilder.makeInputRef(intType, 8);

        // test variable before literal
        pred1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOperator,
                inputRef8,
                intLiteral7);
        binding = rexAnalyzer.analyze(pred1);
        assertNotNull(binding);
        assertEquals(
            "(-infinity, 7)",
            binding.getExpr().toString());

        // test literal before variable
        pred2 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOrEqualOperator,
                intLiteral490,
                inputRef9);
        binding = rexAnalyzer.analyze(pred2);
        assertNotNull(binding);
        assertEquals(
            "(-infinity, 490]",
            binding.getExpr().toString());

        // test AND
        pred3 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                pred1,
                pred2);
        binding = rexAnalyzer.analyze(pred3);
        assertNotNull(binding);
        assertEquals(
            "INTERSECTION( (-infinity, 7) (-infinity, 490] )",
            binding.getExpr().toString());

        // test OR
        pred3 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.orOperator,
                pred1,
                pred2);
        binding = rexAnalyzer.analyze(pred3);
        assertNotNull(binding);
        assertEquals(
            "UNION( (-infinity, 7) (-infinity, 490] )",
            binding.getExpr().toString());

        // test NOT
        pred3 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator,
                pred1);
        binding = rexAnalyzer.analyze(pred3);
        assertNotNull(binding);
        assertEquals(
            "COMPLEMENT( (-infinity, 7) )",
            binding.getExpr().toString());

        // This one should fail:  two variables
        pred1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOperator,
                inputRef8,
                inputRef9);
        binding = rexAnalyzer.analyze(pred1);
        assertNull(binding);

        // This one should fail:  two literals
        pred1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOperator,
                intLiteral7,
                intLiteral490);
        binding = rexAnalyzer.analyze(pred1);
        assertNull(binding);
    }
}

// End SargTest.java
