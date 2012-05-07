/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.rex;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Call to an aggregate function over a window.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class RexOver
    extends RexCall
{
    //~ Instance fields --------------------------------------------------------

    private final RexWindow window;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexOver.
     *
     * <p>For example, "SUM(x) OVER (ROWS 3 PRECEDING)" is represented as:
     *
     * <ul>
     * <li>type = Integer,
     * <li>op = {@link org.eigenbase.sql.fun.SqlStdOperatorTable#sumOperator},
     * <li>operands = { {@link RexFieldAccess}("x") }
     * <li>window = {@link SqlWindow}(ROWS 3 PRECEDING)
     * </ul>
     *
     * @param type Result type
     * @param op Aggregate operator
     * @param operands Operands list
     * @param window Window specification
     *
     * @pre op.isAggregator()
     * @pre window != null
     * @pre window.getRefName() == null
     */
    RexOver(
        RelDataType type,
        SqlAggFunction op,
        RexNode [] operands,
        RexWindow window)
    {
        super(type, op, operands);
        assert op.isAggregator() : "precondition: op.isAggregator()";
        assert op instanceof SqlAggFunction;
        assert window != null : "precondition: window != null";
        this.window = window;
        this.digest = computeDigest(true);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the aggregate operator for this expression.
     */
    public SqlAggFunction getAggOperator()
    {
        return (SqlAggFunction) getOperator();
    }

    public RexWindow getWindow()
    {
        return window;
    }

    protected String computeDigest(boolean withType)
    {
        return super.computeDigest(withType) + " OVER (" + window + ")";
    }

    public RexOver clone()
    {
        return new RexOver(
            getType(),
            getAggOperator(),
            operands,
            window);
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        return visitor.visitOver(this);
    }

    /**
     * Returns whether an expression contains an OVER clause.
     */
    public static boolean containsOver(RexNode expr)
    {
        return Finder.instance.containsOver(expr);
    }

    /**
     * Returns whether a program contains an OVER clause.
     */
    public static boolean containsOver(RexProgram program)
    {
        for (RexNode expr : program.getExprList()) {
            if (Finder.instance.containsOver(expr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether an expression list contains an OVER clause.
     *
     * @deprecated
     */
    public static boolean containsOver(RexNode [] exprs, RexNode expr)
    {
        for (int i = 0; i < exprs.length; i++) {
            if (Finder.instance.containsOver(exprs[i])) {
                return true;
            }
        }
        if ((expr != null)
            && Finder.instance.containsOver(expr))
        {
            return true;
        }
        return false;
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class OverFound
        extends RuntimeException
    {
        public static final OverFound instance = new OverFound();
    }

    /**
     * Visitor which detects a {@link RexOver} inside a {@link RexNode}
     * expression.
     *
     * <p>It is re-entrant (two threads can use an instance at the same time)
     * and it can be re-used for multiple visits.
     */
    private static class Finder
        extends RexVisitorImpl<Void>
    {
        static final RexOver.Finder instance = new RexOver.Finder();

        public Finder()
        {
            super(true);
        }

        public Void visitOver(RexOver over)
        {
            throw OverFound.instance;
        }

        /**
         * Returns whether an expression contains an OVER clause.
         */
        boolean containsOver(RexNode expr)
        {
            try {
                expr.accept(this);
                return false;
            } catch (OverFound e) {
                Util.swallow(e, null);
                return true;
            }
        }
    }
}

// End RexOver.java
