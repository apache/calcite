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

import java.math.BigDecimal;
import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;

import org.junit.Before;
import org.junit.Test;


/**
 * Unit tests for {@link RexProgram} and
 * {@link org.eigenbase.rex.RexProgramBuilder}.
 */
public class RexProgramTest {
    //~ Instance fields --------------------------------------------------------
    private JavaTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a RexProgramTest.
     */
    public RexProgramTest() {
      super();
    }

    @Before public void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    /**
     * Tests construction of a RexProgram.
     */
    @Test public void testBuildProgram() {
        final RexProgramBuilder builder = createProg(0);
        final RexProgram program = builder.getProgram(false);
        final String programString = program.toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, $0)], expr#6=[+($t4, $t2)], "
            + "a=[$t6], b=[$t5])",
            programString);

        // Normalize the program using the RexProgramBuilder.normalize API.
        // Note that unused expression '77' is eliminated, input refs (e.g. $0)
        // become local refs (e.g. $t0), and constants are assigned to locals.
        final RexProgram normalizedProgram =
            RexProgramBuilder.normalize(
                rexBuilder,
                program);
        final String normalizedProgramString = normalizedProgram.toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], a=[$t5], b=[$t6])",
            normalizedProgramString);
    }

    /**
     * Tests construction and normalization of a RexProgram.
     */
    @Test public void testNormalize() {
        final RexProgramBuilder builder = createProg(0);
        final String program = builder.getProgram(true).toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], a=[$t5], b=[$t6])",
            program);
    }

    /**
     * Tests construction and normalization of a RexProgram.
     */
    @Test public void testElimDups() {
        final RexProgramBuilder builder = createProg(1);
        final String unnormalizedProgram = builder.getProgram(false).toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], a=[$t7], b=[$t6])",
            unnormalizedProgram);

        // normalize eliminates dups (specifically "+($0, $1)")
        final RexProgramBuilder builder2 = createProg(1);
        final String program2 = builder2.getProgram(true).toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], a=[$t5], b=[$t6])",
            program2);
    }

    /**
     * Tests that AND(x, x) is translated to x.
     */
    @Test public void testDuplicateAnd() {
        final RexProgramBuilder builder = createProg(2);
        final String program = builder.getProgram(true).toString();
        TestUtil.assertEqualsVerbose(
            "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], expr#7=[>($t2, $t0)], "
            + "a=[$t5], b=[$t6], $condition=[$t7])",
            program);
    }

    /**
     * Creates a program, depending on variant:
     * <ol>
     * <li><code>select (x + y) + (x + 1) as a, (x + x) as b from t(x, y)</code>
     * <li><code>select (x + y) + (x + 1) as a, (x + (x + 1)) as b
     *     from t(x, y)</code>
     * <li><code>select (x + y) + (x + 1) as a, (x + x) as b from t(x, y)
     *     where ((x + y) > 1) and ((x + y) > 1)</code>
     * </ul>
     */
    private RexProgramBuilder createProg(int variant)
    {
        assert variant == 0 || variant == 1 || variant == 2;
        List<RelDataType> types =
            Arrays.asList(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.INTEGER));
        List<String> names = Arrays.asList("x", "y");
        RelDataType inputRowType = typeFactory.createStructType(types, names);
        final RexProgramBuilder builder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        // $t0 = x
        // $t1 = y
        // $t2 = $t0 + 1 (i.e. x + 1)
        final RexNode i0 = rexBuilder.makeInputRef(
            types.get(0), 0);
        final RexLiteral c1 = rexBuilder.makeExactLiteral(
            BigDecimal.ONE);
        RexLocalRef t2 =
            builder.addExpr(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.plusOperator,
                    i0,
                    c1));
        // $t3 = 77 (not used)
        final RexLiteral c77 =
            rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(77));
        RexLocalRef t3 =
            builder.addExpr(
                c77);
        Util.discard(t3);
        // $t4 = $t0 + $t1 (i.e. x + y)
        final RexNode i1 = rexBuilder.makeInputRef(
            types.get(1), 1);
        RexLocalRef t4 =
            builder.addExpr(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.plusOperator,
                    i0,
                    i1));
        RexLocalRef t5;
        switch (variant) {
        case 0:
        case 2:
            // $t5 = $t0 + $t0 (i.e. x + x)
            t5 = builder.addExpr(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.plusOperator,
                    i0,
                    i0));
            break;
        case 1:
            // $tx = $t0 + 1
            RexLocalRef tx =
                builder.addExpr(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.plusOperator,
                        i0,
                        c1));
            // $t5 = $t0 + $tx (i.e. x + (x + 1))
            t5 =
                builder.addExpr(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.plusOperator,
                        i0,
                        tx));
            break;
        default:
            throw Util.newInternal("unexpected variant " + variant);
        }
        // $t6 = $t4 + $t2 (i.e. (x + y) + (x + 1))
        RexLocalRef t6 =
            builder.addExpr(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.plusOperator,
                    t4,
                    t2));
        builder.addProject(t6.getIndex(), "a");
        builder.addProject(t5.getIndex(), "b");

        if (variant == 2) {
            // $t7 = $t4 > $i0 (i.e. (x + y) > 0)
            RexLocalRef t7 =
                builder.addExpr(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.greaterThanOperator,
                        t4,
                        i0));
            // $t8 = $t7 AND $t7
            RexLocalRef t8 =
                builder.addExpr(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        t7,
                        t7));
            builder.addCondition(t8);
            builder.addCondition(t7);
        }
        return builder;
    }
}

// End RexProgramTest.java
