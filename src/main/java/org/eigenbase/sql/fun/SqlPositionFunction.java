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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * The <code>POSITION</code> function.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlPositionFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    // FIXME jvs 25-Jan-2009:  POSITION should verify that
    // params are all same character set, like OVERLAY does implicitly
    // as part of rtiDyadicStringSumPrecision

    public SqlPositionFunction()
    {
        super(
            "POSITION",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiNullableInteger,
            null,
            SqlTypeStrategies.otcStringSameX2,
            SqlFunctionCategory.Numeric);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("IN");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} IN {2})";
        }
        assert (false);
        return null;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlValidator validator = callBinding.getValidator();
        SqlCall call = callBinding.getCall();

        //check that the two operands are of same type.
        RelDataType type0 = validator.getValidatedNodeType(call.operands[0]);
        RelDataType type1 = validator.getValidatedNodeType(call.operands[1]);
        if (!SqlTypeUtil.inSameFamily(type0, type1)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        return getOperandTypeChecker().checkOperandTypes(
            callBinding,
            throwOnFailure);
    }
}

// End SqlPositionFunction.java
