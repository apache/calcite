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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Definition of the "TRIM" builtin SQL function.
 *
 * @author Wael Chatila, Julian Hyde
 * @version $Id$
 * @since May 28, 2004
 */
public class SqlTrimFunction
    extends SqlFunction
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
     */
    public enum Flag
        implements SqlLiteral.SqlSymbol
    {
        BOTH(1, 1), LEADING(1, 0), TRAILING(0, 1);

        private final int left;
        private final int right;

        Flag(int left, int right)
        {
            this.left = left;
            this.right = right;
        }

        public int getLeft()
        {
            return left;
        }

        public int getRight()
        {
            return right;
        }
    }

    //~ Constructors -----------------------------------------------------------

    public SqlTrimFunction()
    {
        super(
            "TRIM",
            SqlKind.TRIM,
            new SqlTypeTransformCascade(
                SqlTypeStrategies.rtiThirdArgType,
                SqlTypeTransforms.toNullable,
                SqlTypeTransforms.toVarying),
            null,
            SqlTypeStrategies.otcStringSameX2,
            SqlFunctionCategory.String);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlOperandCountRange getOperandCountRange()
    {
        // REVIEW jvs 2-June-2005:  shouldn't this be TwoOrThree?
        // Also, inconsistent with with otc above!
        return SqlOperandCountRange.Three;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        assert operands[0] instanceof SqlLiteral;
        operands[0].unparse(writer, 0, 0);
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[2].unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} FROM {2})";
        case 3:
            return "{0}({1} {2} FROM {3})";
        }
        assert (false);
        return null;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode ... operands)
    {
        assert functionQualifier == null;

        // Be defensive, in case the parser instantiates a call using say
        // "TRIM"('a').
        if (operands.length != 3) {
            operands =
                new SqlNode[] {
                    (operands.length > 0) ? operands[0] : null,
                    (operands.length > 1) ? operands[1] : null,
                    (operands.length > 2) ? operands[2]
                    : SqlLiteral.createNull(SqlParserPos.ZERO)
                };
        }
        if (null == operands[0]) {
            operands[0] = SqlLiteral.createSymbol(Flag.BOTH, pos);
        }

        if (null == operands[1]) {
            operands[1] = SqlLiteral.createCharString(" ", pos);
        }
        return super.createCall(functionQualifier, pos, operands);
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        for (int i = 1; i < 3; i++) {
            if (!SqlTypeStrategies.otcString.checkSingleOperandType(
                    callBinding,
                    call.operands[i],
                    0,
                    throwOnFailure))
            {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }

        SqlNode [] ops = { call.operands[1], call.operands[2] };

        return SqlTypeUtil.isCharTypeComparable(
            callBinding,
            ops,
            throwOnFailure);
    }
}

// End SqlTrimFunction.java
