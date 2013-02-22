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
import org.eigenbase.sql.type.*;


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
    private final Flag flag;

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

    public SqlTrimFunction(Flag flag)
    {
        super(
            "TRIM",
            SqlKind.TRIM,
            new SqlTypeTransformCascade(
                SqlTypeStrategies.rtiSecondArgType,
                SqlTypeTransforms.toNullable,
                SqlTypeTransforms.toVarying),
            null,
            SqlTypeStrategies.otcStringSameX2,
            SqlFunctionCategory.String);
        this.flag = flag;
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        assert operands[0] instanceof SqlLiteral;
        writer.sep(flag.name());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}([BOTH|LEADING|TRAILING} {1} FROM {2})";
        default:
            throw new AssertionError();
        }
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode ... operands)
    {
        assert functionQualifier == null;
        assert operands.length == 2;
        if (operands[0] == null) {
            operands[0] = SqlLiteral.createCharString(" ", pos);
        }
        return super.createCall(functionQualifier, pos, operands);
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        for (int i = 0; i < 2; i++) {
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

        return SqlTypeUtil.isCharTypeComparable(
            callBinding,
            call.operands,
            throwOnFailure);
    }
}

// End SqlTrimFunction.java
