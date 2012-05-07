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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * SqlOverlapsOperator represents the SQL:1999 standard OVERLAPS function
 * Determins if two anchored time intervals overlaps.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 11, 2004
 */
public class SqlOverlapsOperator
    extends SqlSpecialOperator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlWriter.FrameType OverlapsFrameType =
        SqlWriter.FrameTypeEnum.create("OVERLAPS");

    //~ Constructors -----------------------------------------------------------

    public SqlOverlapsOperator()
    {
        super(
            "OVERLAPS",
            SqlKind.OVERLAPS,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            null);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(OverlapsFrameType, "(", ")");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep(",", true);
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.sep(")", true);
        writer.sep(getName());
        writer.sep("(", true);
        operands[2].unparse(writer, leftPrec, rightPrec);
        writer.sep(",", true);
        operands[3].unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Four;
    }

    public String getSignatureTemplate(int operandsCount)
    {
        if (4 == operandsCount) {
            return "({1}, {2}) {0} ({3}, {4})";
        }
        assert (false);
        return null;
    }

    public String getAllowedSignatures(String opName)
    {
        final String d = "DATETIME";
        final String i = "INTERVAL";
        String [] typeNames = {
            d, d,
            d, i,
            i, d,
            i, i
        };

        StringBuilder ret = new StringBuilder();
        for (int y = 0; y < typeNames.length; y += 2) {
            if (y > 0) {
                ret.append(NL);
            }
            ArrayList<String> list = new ArrayList<String>();
            list.add(d);
            list.add(typeNames[y]);
            list.add(d);
            list.add(typeNames[y + 1]);
            ret.append(SqlUtil.getAliasedSignature(this, opName, list));
        }
        return ret.toString();
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        SqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();
        if (!SqlTypeStrategies.otcDatetime.checkSingleOperandType(
                callBinding,
                call.operands[0],
                0,
                throwOnFailure))
        {
            return false;
        }
        if (!SqlTypeStrategies.otcDatetime.checkSingleOperandType(
                callBinding,
                call.operands[2],
                0,
                throwOnFailure))
        {
            return false;
        }

        RelDataType t0 = validator.deriveType(scope, call.operands[0]);
        RelDataType t1 = validator.deriveType(scope, call.operands[1]);
        RelDataType t2 = validator.deriveType(scope, call.operands[2]);
        RelDataType t3 = validator.deriveType(scope, call.operands[3]);

        // t0 must be comparable with t2
        if (!SqlTypeUtil.sameNamedType(t0, t2)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        if (SqlTypeUtil.isDatetime(t1)) {
            // if t1 is of DATETIME,
            // then t1 must be comparable with t0
            if (!SqlTypeUtil.sameNamedType(t0, t1)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        } else if (!SqlTypeUtil.isInterval(t1)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        if (SqlTypeUtil.isDatetime(t3)) {
            // if t3 is of DATETIME,
            // then t3 must be comparable with t2
            if (!SqlTypeUtil.sameNamedType(t2, t3)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        } else if (!SqlTypeUtil.isInterval(t3)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }
}

// End SqlOverlapsOperator.java
