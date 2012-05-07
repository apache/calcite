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

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * SqlCastFunction. Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable. So you can't store
 * any local info in the class and hence the return type data is maintained in
 * operand[1] through the validation phase.
 *
 * @author lee
 * @version $Id$
 * @since Jun 5, 2004
 */
public class SqlCastFunction
    extends SqlFunction
{
    //~ Instance fields --------------------------------------------------------

    private final Set<TypeFamilyCast> nonMonotonicPreservingCasts =
        createNonMonotonicPreservingCasts();

    //~ Constructors -----------------------------------------------------------

    public SqlCastFunction()
    {
        super(
            "CAST",
            SqlKind.CAST,
            null,
            SqlTypeStrategies.otiFirstKnown,
            null,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    /*
     * List all casts that do not preserve monotonicity
     */
    private Set<TypeFamilyCast> createNonMonotonicPreservingCasts()
    {
        Set<TypeFamilyCast> result = new HashSet<TypeFamilyCast>();
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.EXACT_NUMERIC,
                SqlTypeFamily.CHARACTER));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.NUMERIC,
                SqlTypeFamily.CHARACTER));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.APPROXIMATE_NUMERIC,
                SqlTypeFamily.CHARACTER));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.DATETIME_INTERVAL,
                SqlTypeFamily.CHARACTER));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.EXACT_NUMERIC));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.NUMERIC));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.APPROXIMATE_NUMERIC));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.DATETIME_INTERVAL));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.DATETIME,
                SqlTypeFamily.TIME));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.TIMESTAMP,
                SqlTypeFamily.TIME));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.TIME,
                SqlTypeFamily.DATETIME));
        result.add(
            new TypeFamilyCast(
                SqlTypeFamily.TIME,
                SqlTypeFamily.TIMESTAMP));
        return result;
    }

    private boolean isMonotonicPreservingCast(
        RelDataTypeFamily castFrom,
        RelDataTypeFamily castTo)
    {
        return !nonMonotonicPreservingCasts.contains(
            new TypeFamilyCast(castFrom, castTo));
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        assert (opBinding.getOperandCount() == 2);
        RelDataType ret = opBinding.getOperandType(1);
        RelDataType firstType = opBinding.getOperandType(0);
        ret =
            opBinding.getTypeFactory().createTypeWithNullability(
                ret,
                firstType.isNullable());
        if (opBinding instanceof SqlCallBinding) {
            SqlCallBinding callBinding = (SqlCallBinding) opBinding;
            SqlNode operand0 = callBinding.getCall().operands[0];

            // dynamic parameters and null constants need their types assigned
            // to them using the type they are casted to.
            if (((operand0 instanceof SqlLiteral)
                    && (((SqlLiteral) operand0).getValue() == null))
                || (operand0 instanceof SqlDynamicParam))
            {
                callBinding.getValidator().setValidatedNodeType(
                    operand0,
                    ret);
            }
        }
        return ret;
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} AS {2})";
        }
        assert (false);
        return null;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments can
     * override this method.
     */
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (SqlUtil.isNullLiteral(callBinding.getCall().operands[0], false)
                || callBinding.getCall().operands[0] instanceof SqlDynamicParam)
        {
            return true;
        }
        RelDataType validatedNodeType =
            callBinding.getValidator().getValidatedNodeType(
                callBinding.getCall().operands[0]);
        RelDataType returnType =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                callBinding.getCall().operands[1]);
        if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    EigenbaseResource.instance().CannotCastValue.ex(
                        validatedNodeType.toString(),
                        returnType.toString()));
            }
            return false;
        }
        if (SqlTypeUtil.areCharacterSetsMismatched(
                validatedNodeType,
                returnType))
        {
            if (throwOnFailure) {
                // Include full type string to indicate character
                // set mismatch.
                throw callBinding.newError(
                    EigenbaseResource.instance().CannotCastValue.ex(
                        validatedNodeType.getFullTypeString(),
                        returnType.getFullTypeString()));
            }
            return false;
        }
        return true;
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert operands.length == 2;
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, 0, 0);
        writer.sep("AS");
        if (operands[1] instanceof SqlIntervalQualifier) {
            writer.sep("INTERVAL");
        }
        operands[1].unparse(writer, 0, 0);
        writer.endFunCall(frame);
    }

    @Override public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        RelDataTypeFamily castFrom =
            scope.getValidator().deriveType(scope, call.operands[0])
            .getFamily();
        RelDataTypeFamily castTo =
            scope.getValidator().deriveType(scope, call.operands[1])
            .getFamily();
        if (isMonotonicPreservingCast(castFrom, castTo)) {
            return call.operands[0].getMonotonicity(scope);
        } else {
            return SqlMonotonicity.NotMonotonic;
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private class TypeFamilyCast
    {
        private final RelDataTypeFamily castFrom;
        private final RelDataTypeFamily castTo;

        public TypeFamilyCast(
            RelDataTypeFamily castFrom,
            RelDataTypeFamily castTo)
        {
            this.castFrom = castFrom;
            this.castTo = castTo;
        }

        @Override public boolean equals(Object obj)
        {
            // TODO Auto-generated method stub
            if (obj.getClass() != TypeFamilyCast.class) {
                return false;
            }
            TypeFamilyCast other = (TypeFamilyCast) obj;
            return this.castFrom.equals(other.castFrom)
                && this.castTo.equals(other.castTo);
        }

        @Override public int hashCode()
        {
            // TODO Auto-generated method stub
            return castFrom.hashCode() + castTo.hashCode();
        }
    }
}

// End SqlCastFunction.java
