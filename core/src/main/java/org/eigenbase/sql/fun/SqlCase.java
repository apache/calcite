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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlCase</code> is a node of a parse tree which represents a case
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 *
 * @author wael
 * @version $Id$
 * @since Mar 14, 2004
 */
public class SqlCase
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * VALUE_OPERAND = 0
     */
    public static final int VALUE_OPERAND = 0;

    /**
     * WHEN_OPERANDS = 1
     */
    public static final int WHEN_OPERANDS = 1;

    /**
     * THEN_OPERANDS = 2
     */
    public static final int THEN_OPERANDS = 2;

    /**
     * ELSE_OPERAND = 3
     */
    public static final int ELSE_OPERAND = 3;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlCase expression.
     *
     * <p>The operands are an array of SqlNodes where
     *
     * <ul>
     * <li>operands[0] is a SqlNodeList of all WHEN expressions
     * <li>operands[1] is a SqlNodeList of all THEN expressions
     * <li>operands[2] is a SqlNode representing the implicit or explicit ELSE
     * expression
     * </ul>
     *
     * <p>See {@link #VALUE_OPERAND}, {@link #WHEN_OPERANDS},
     * {@link #THEN_OPERANDS}, {@link #ELSE_OPERAND}.
     */
    SqlCase(
        SqlCaseOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(operator, operands, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getValueOperand()
    {
        return operands[VALUE_OPERAND];
    }

    public SqlNodeList getWhenOperands()
    {
        return (SqlNodeList) operands[WHEN_OPERANDS];
    }

    public SqlNodeList getThenOperands()
    {
        return (SqlNodeList) operands[THEN_OPERANDS];
    }

    public SqlNode getElseOperand()
    {
        return operands[ELSE_OPERAND];
    }
}

// End SqlCase.java
