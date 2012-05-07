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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlJoin</code> is ...
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 29, 2003
 */
public class SqlJoin
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int LEFT_OPERAND = 0;

    /**
     * Operand says whether this is a natural join. Must be constant TRUE or
     * FALSE.
     */
    public static final int IS_NATURAL_OPERAND = 1;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * SqlJoinOperator.JoinType}.
     */
    public static final int TYPE_OPERAND = 2;
    public static final int RIGHT_OPERAND = 3;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * SqlJoinOperator.ConditionType}.
     */
    public static final int CONDITION_TYPE_OPERAND = 4;
    public static final int CONDITION_OPERAND = 5;

    //~ Constructors -----------------------------------------------------------

    public SqlJoin(
        SqlJoinOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(operator, operands, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public final SqlNode getCondition()
    {
        return operands[CONDITION_OPERAND];
    }

    /**
     * Returns a {@link SqlJoinOperator.ConditionType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.ConditionType getConditionType()
    {
        return (SqlJoinOperator.ConditionType) SqlLiteral.symbolValue(
            operands[CONDITION_TYPE_OPERAND]);
    }

    /**
     * Returns a {@link SqlJoinOperator.JoinType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.JoinType getJoinType()
    {
        return (SqlJoinOperator.JoinType) SqlLiteral.symbolValue(
            operands[TYPE_OPERAND]);
    }

    public final SqlNode getLeft()
    {
        return operands[LEFT_OPERAND];
    }

    public final boolean isNatural()
    {
        return SqlLiteral.booleanValue(operands[IS_NATURAL_OPERAND]);
    }

    public final SqlNode getRight()
    {
        return operands[RIGHT_OPERAND];
    }
}

// End SqlJoin.java
