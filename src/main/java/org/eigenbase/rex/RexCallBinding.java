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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * <code>RexCallBinding</code> implements {@link SqlOperatorBinding} by
 * referring to an underlying collection of {@link RexNode} operands.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class RexCallBinding
    extends SqlOperatorBinding
{
    //~ Instance fields --------------------------------------------------------

    private final RexNode [] operands;

    //~ Constructors -----------------------------------------------------------

    public RexCallBinding(
        RelDataTypeFactory typeFactory,
        SqlOperator sqlOperator,
        RexNode [] operands)
    {
        super(typeFactory, sqlOperator);
        this.operands = operands;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperatorBinding
    public String getStringLiteralOperand(int ordinal)
    {
        return RexLiteral.stringValue(operands[ordinal]);
    }

    // implement SqlOperatorBinding
    public int getIntLiteralOperand(int ordinal)
    {
        return RexLiteral.intValue(operands[ordinal]);
    }

    // implement SqlOperatorBinding
    public boolean isOperandNull(int ordinal, boolean allowCast)
    {
        return RexUtil.isNullLiteral(operands[ordinal], allowCast);
    }

    // implement SqlOperatorBinding
    public int getOperandCount()
    {
        return operands.length;
    }

    // implement SqlOperatorBinding
    public RelDataType getOperandType(int ordinal)
    {
        return operands[ordinal].getType();
    }

    public EigenbaseException newError(
        SqlValidatorException e)
    {
        return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
}

// End RexCallBinding.java
