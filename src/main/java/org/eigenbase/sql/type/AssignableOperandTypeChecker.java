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
import org.eigenbase.sql.*;


/**
 * AssignableOperandTypeChecker implements {@link SqlOperandTypeChecker} by
 * verifying that the type of each argument is assignable to a predefined set of
 * parameter types (under the SQL definition of "assignable").
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class AssignableOperandTypeChecker
    implements SqlOperandTypeChecker
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType [] paramTypes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Instantiates this strategy with a specific set of parameter types.
     *
     * @param paramTypes parameter types for operands; index in this array
     * corresponds to operand number
     */
    public AssignableOperandTypeChecker(RelDataType [] paramTypes)
    {
        this.paramTypes = paramTypes;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange()
    {
        return new SqlOperandCountRange(paramTypes.length);
    }

    // implement SqlOperandTypeChecker
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        for (int i = 0; i < callBinding.getOperandCount(); ++i) {
            RelDataType argType =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    callBinding.getCall().operands[i]);
            if (!SqlTypeUtil.canAssignFrom(paramTypes[i], argType)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(opName);
        sb.append("(");
        for (int i = 0; i < paramTypes.length; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("<");
            sb.append(paramTypes[i].getFamily().toString());
            sb.append(">");
        }
        sb.append(")");
        return sb.toString();
    }
}

// End AssignableOperandTypeChecker.java
