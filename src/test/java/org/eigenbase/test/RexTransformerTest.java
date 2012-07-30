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

import junit.framework.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * Tests transformations on rex nodes.
 *
 * @author wael
 * @since Mar 9, 2004
 */
public class RexTransformerTest
    extends TestCase
{
    //~ Instance fields --------------------------------------------------------

    RexBuilder rexBuilder = null;
    RexNode x;
    RexNode y;
    RexNode z;
    RexNode trueRex;
    RexNode falseRex;
    RelDataType boolRelDataType;
    RelDataTypeFactory typeFactory;

    //~ Methods ----------------------------------------------------------------

    protected void setUp()
        throws Exception
    {
        typeFactory = new OJTypeFactoryImpl();
        rexBuilder = new JavaRexBuilder(typeFactory);
        boolRelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        x = new RexInputRef(
            0,
            typeFactory.createTypeWithNullability(boolRelDataType, true));
        y = new RexInputRef(
            1,
            typeFactory.createTypeWithNullability(boolRelDataType, true));
        z = new RexInputRef(
            2,
            typeFactory.createTypeWithNullability(boolRelDataType, true));
        trueRex = rexBuilder.makeLiteral(true);
        falseRex = rexBuilder.makeLiteral(false);
    }

    void check(
        Boolean encapsulateType,
        RexNode node,
        String expected)
    {
        RexNode root;
        if (null == encapsulateType) {
            root = node;
        } else if (encapsulateType.equals(Boolean.TRUE)) {
            root =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isTrueOperator,
                    node);
        } else { //if (encapsulateType.equals(Boolean.FALSE))
            root =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isFalseOperator,
                    node);
        }

        RexTransformer transformer = new RexTransformer(root, rexBuilder);
        RexNode result = transformer.transformNullSemantics();
        String actual = result.toString();
        if (!actual.equals(expected)) {
            String msg =
                "\nExpected=<" + expected + ">\n  Actual=<" + actual + ">";
            fail(msg);
        }
    }

    public void testPreTests()
    {
        //can make variable nullable?
        RexNode node =
            new RexInputRef(
                0,
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                    true));
        assertTrue(node.getType().isNullable());

        //can make variable not nullable?
        node =
            new RexInputRef(
                0,
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                    false));
        assertFalse(node.getType().isNullable());
    }

    public void testNonBooleans()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.plusOperator,
                x,
                y);
        String expected = node.toString();
        check(Boolean.TRUE, node, expected);
        check(Boolean.FALSE, node, expected);
        check(null, node, expected);
    }

    /**
     * the or operator should pass through unchanged since e.g. x OR y should
     * return true if x=null and y=true if it was transformed into something
     * like (x ISNOTNULL) AND (y ISNOTNULL) AND (x OR y) an incorrect result
     * could be produced
     */
    public void testOrUnchanged()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.orOperator,
                x,
                y);
        String expected = node.toString();
        check(Boolean.TRUE, node, expected);
        check(Boolean.FALSE, node, expected);
        check(null, node, expected);
    }

    public void testSimpleAnd()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                x,
                y);
        check(
            Boolean.FALSE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), AND($0, $1))");
    }

    public void testSimpleEquals()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                x,
                y);
        check(
            Boolean.TRUE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1))");
    }

    public void testSimpleNotEquals()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.notEqualsOperator,
                x,
                y);
        check(
            Boolean.FALSE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <>($0, $1))");
    }

    public void testSimpleGreaterThan()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOperator,
                x,
                y);
        check(
            Boolean.TRUE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >($0, $1))");
    }

    public void testSimpleGreaterEquals()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOrEqualOperator,
                x,
                y);
        check(
            Boolean.FALSE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >=($0, $1))");
    }

    public void testSimpleLessThan()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOperator,
                x,
                y);
        check(
            Boolean.TRUE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <($0, $1))");
    }

    public void testSimpleLessEqual()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOrEqualOperator,
                x,
                y);
        check(
            Boolean.FALSE,
            node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <=($0, $1))");
    }

    public void testOptimizeNonNullLiterals()
    {
        RexNode node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOrEqualOperator,
                x,
                trueRex);
        check(Boolean.TRUE, node, "AND(IS NOT NULL($0), <=($0, true))");
        node =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOrEqualOperator,
                trueRex,
                x);
        check(Boolean.FALSE, node, "AND(IS NOT NULL($0), <=(true, $0))");
    }

    public void testSimpleIdentifier()
    {
        RexNode node = rexBuilder.makeInputRef(boolRelDataType, 0);
        check(Boolean.TRUE, node, "=(IS TRUE($0), true)");
    }

    public void testMixed1()
    {
        //x=true AND y
        RexNode op1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                x,
                trueRex);
        RexNode and =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                op1,
                y);
        check(
            Boolean.FALSE,
            and,
            "AND(IS NOT NULL($1), AND(AND(IS NOT NULL($0), =($0, true)), $1))");
    }

    public void testMixed2()
    {
        //x!=true AND y>z
        RexNode op1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.notEqualsOperator,
                x,
                trueRex);
        RexNode op2 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOperator,
                y,
                z);
        RexNode and =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                op1,
                op2);
        check(
            Boolean.FALSE,
            and,
            "AND(AND(IS NOT NULL($0), <>($0, true)), AND(AND(IS NOT NULL($1), IS NOT NULL($2)), >($1, $2)))");
    }

    public void testMixed3()
    {
        //x=y AND false>z
        RexNode op1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                x,
                y);
        RexNode op2 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOperator,
                falseRex,
                z);
        RexNode and =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                op1,
                op2);
        check(
            Boolean.TRUE,
            and,
            "AND(AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1)), AND(IS NOT NULL($2), >(false, $2)))");
    }
}

// End RexTransformerTest.java
