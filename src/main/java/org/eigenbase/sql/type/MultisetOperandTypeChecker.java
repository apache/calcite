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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * Parameter type-checking strategy types must be [nullable] Multiset,
 * [nullable] Multiset and the two types must have the same element type
 *
 * @author Wael Chatila
 * @version $Id$
 * @see MultisetSqlType#getComponentType
 */
public class MultisetOperandTypeChecker
    implements SqlOperandTypeChecker
{
    //~ Methods ----------------------------------------------------------------

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        SqlNode op0 = call.operands[0];
        if (!SqlTypeStrategies.otcMultiset.checkSingleOperandType(
                callBinding,
                op0,
                0,
                throwOnFailure))
        {
            return false;
        }

        SqlNode op1 = call.operands[1];
        if (!SqlTypeStrategies.otcMultiset.checkSingleOperandType(
                callBinding,
                op1,
                0,
                throwOnFailure))
        {
            return false;
        }

        RelDataType [] argTypes = new RelDataType[2];
        argTypes[0] =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                op0).getComponentType();
        argTypes[1] =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                op1).getComponentType();

        //TODO this wont work if element types are of ROW types and there is a
        //mismatch.
        RelDataType biggest =
            callBinding.getTypeFactory().leastRestrictive(
                argTypes);
        if (null == biggest) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    EigenbaseResource.instance().TypeNotComparable.ex(
                        call.operands[0].getParserPosition().toString(),
                        call.operands[1].getParserPosition().toString()));
            }

            return false;
        }
        return true;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }

    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        return "<MULTISET> " + opName + " <MULTISET>";
    }
}

// End MultisetOperandTypeChecker.java
