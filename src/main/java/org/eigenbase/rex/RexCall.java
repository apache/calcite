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
package org.eigenbase.rex;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

/**
 * An expression formed by a call to an operator with zero or more expressions
 * as operands.
 *
 * <p>Operators may be binary, unary, functions, special syntactic constructs
 * like <code>CASE ... WHEN ... END</code>, or even internally generated
 * constructs like implicit type conversions. The syntax of the operator is
 * really irrelevant, because row-expressions (unlike {@link
 * org.eigenbase.sql.SqlNode SQL expressions}) do not directly represent a piece
 * of source code.</p>
 *
 * <p>It's not often necessary to sub-class this class. The smarts should be in
 * the operator, rather than the call. Any extra information about the call can
 * often be encoded as extra arguments. (These don't need to be hidden, because
 * no one is going to be generating source code from this tree.)</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public class RexCall
    extends RexNode
{
    //~ Instance fields --------------------------------------------------------

    private final SqlOperator op;
    public final ImmutableList<RexNode> operands;
    private final RelDataType type;
    private final RexKind kind;

    //~ Constructors -----------------------------------------------------------

    protected RexCall(
        RelDataType type,
        SqlOperator op,
        List<? extends RexNode> operands)
    {
        assert type != null : "precondition: type != null";
        assert op != null : "precondition: op != null";
        assert operands != null : "precondition: operands != null";
        this.type = type;
        this.op = op;
        this.operands = ImmutableList.copyOf(operands);
        this.kind = sqlKindToRexKind(op.getKind());
        assert this.kind != null : op;
        this.digest = computeDigest(true);

        // TODO zfong 11/19/07 - Extend the check below to all types of
        // operators, similar to SqlOperator.checkOperandCount.  However,
        // that method operates on SqlCalls, which may have not have the
        // same number of operands as their corresponding RexCalls.  One
        // example is the CAST operator, which is originally a 2-operand
        // SqlCall, but is later converted to a 1-operand RexCall.
        if (op instanceof SqlBinaryOperator) {
            assert operands.size() == 2;
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the {@link RexKind} corresponding to a {@link SqlKind}. Fails if
     * there is none. Never returns null.
     */
    static RexKind sqlKindToRexKind(SqlKind kind)
    {
        switch (kind) {
        case EQUALS:
            return RexKind.Equals;
        case IDENTIFIER:
            return RexKind.Identifier;
        case LITERAL:
            return RexKind.Literal;
        case DYNAMIC_PARAM:
            return RexKind.DynamicParam;
        case TIMES:
            return RexKind.Times;
        case DIVIDE:
            return RexKind.Divide;
        case PLUS:
            return RexKind.Plus;
        case MINUS:
            return RexKind.Minus;
        case LESS_THAN:
            return RexKind.LessThan;
        case GREATER_THAN:
            return RexKind.GreaterThan;
        case LESS_THAN_OR_EQUAL:
            return RexKind.LessThanOrEqual;
        case GREATER_THAN_OR_EQUAL:
            return RexKind.GreaterThanOrEqual;
        case NOT_EQUALS:
            return RexKind.NotEquals;
        case OR:
            return RexKind.Or;
        case AND:
            return RexKind.And;
        case NOT:
            return RexKind.Not;
        case IS_TRUE:
            return RexKind.IsTrue;
        case IS_FALSE:
            return RexKind.IsFalse;
        case IS_NULL:
            return RexKind.IsNull;
        case IS_UNKNOWN:
            return RexKind.IsNull;
        case PLUS_PREFIX:
            return RexKind.Plus;
        case MINUS_PREFIX:
            return RexKind.MinusPrefix;
        case VALUES:
            return RexKind.Values;
        case ROW:
            return RexKind.Row;
        case CAST:
            return RexKind.Cast;
        case TRIM:
            return RexKind.Trim;
        case OTHER_FUNCTION:
            return RexKind.Other;
        case CASE:
            return RexKind.Other;
        case OTHER:
            return RexKind.Other;
        case LIKE:
            return RexKind.Like;
        case SIMILAR:
            return RexKind.Similar;
        case MULTISET_QUERY_CONSTRUCTOR:
            return RexKind.MultisetQueryConstructor;
        case ARRAY_VALUE_CONSTRUCTOR:
            return RexKind.ArrayValueConstructor;
        case MAP_VALUE_CONSTRUCTOR:
            return RexKind.MapValueConstructor;
        case NEW_SPECIFICATION:
            return RexKind.NewSpecification;
        case REINTERPRET:
            return RexKind.Reinterpret;
        case COLUMN_LIST:
            return RexKind.Row;
        case DESCENDING:
            return RexKind.Descending;
        case NULLS_FIRST:
            return RexKind.NullsFirst;
        case NULLS_LAST:
             return RexKind.NullsLast;
        default:
            throw Util.unexpected(kind);
        }
    }

    protected String computeDigest(boolean withType)
    {
        StringBuilder sb = new StringBuilder(op.getName());
        if ((operands.size() == 0)
            && (op.getSyntax() == SqlSyntax.FunctionId))
        {
            // Don't print params for empty arg list. For example, we want
            // "SYSTEM_USER", not "SYSTEM_USER()".
        } else {
            sb.append("(");
            for (int i = 0; i < operands.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                RexNode operand = operands.get(i);
                sb.append(operand.toString());
            }
            sb.append(")");
        }
        if (withType) {
            sb.append(":");

            // NOTE jvs 16-Jan-2005:  for digests, it is very important
            // to use the full type string.
            sb.append(type.getFullTypeString());
        }
        return sb.toString();
    }

    public String toString()
    {
        // REVIEW jvs 16-Jan-2005: For CAST and NEW, the type is really an
        // operand and needs to be printed out.  But special-casing it here is
        // ugly.
        return computeDigest(
            isA(RexKind.Cast) || isA(RexKind.NewSpecification));
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        return visitor.visitCall(this);
    }

    public RelDataType getType()
    {
        return type;
    }

    public RexCall clone()
    {
        return new RexCall(type, op, operands);
    }

    public RexKind getKind()
    {
        return kind;
    }

    public List<RexNode> getOperands()
    {
        return ImmutableList.copyOf(operands);
    }

    public SqlOperator getOperator()
    {
        return op;
    }

    /**
     * Creates a new call to the same operator with different operands.
     *
     * @param type Return type
     * @param operands Operands to call
     *
     * @return New call
     */
    public RexCall clone(RelDataType type, List<RexNode> operands)
    {
        return new RexCall(type, op, operands);
    }
}

// End RexCall.java
