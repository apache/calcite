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

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlFunction
    extends SqlOperator
{
    //~ Instance fields --------------------------------------------------------

    private final SqlFunctionCategory functionType;

    private final SqlIdentifier sqlIdentifier;

    private final RelDataType [] paramTypes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name of builtin function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param funcType categorization for function
     */
    public SqlFunction(
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory funcType)
    {
        super(
            name,
            kind,
            100,
            100,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);

        assert !((funcType == SqlFunctionCategory.UserDefinedConstructor)
            && (returnTypeInference == null));

        this.functionType = funcType;

        // NOTE jvs 18-Jan-2005:  we leave sqlIdentifier as null to indicate
        // that this is a builtin.  Same for paramTypes.
        this.sqlIdentifier = null;
        this.paramTypes = null;
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a
     * possibly qualified name. This name must be resolved into either a builtin
     * function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param paramTypes array of parameter types
     * @param funcType function category
     */
    public SqlFunction(
        SqlIdentifier sqlIdentifier,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        RelDataType [] paramTypes,
        SqlFunctionCategory funcType)
    {
        super(
            sqlIdentifier.names[sqlIdentifier.names.length - 1],
            SqlKind.OTHER_FUNCTION,
            100,
            100,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);

        // assert !(funcType == SqlFunctionCategory.UserDefinedConstructor
        // &&           returnTypeInference == null);

        this.sqlIdentifier = sqlIdentifier;
        this.functionType = funcType;
        this.paramTypes = paramTypes;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Function;
    }

    /**
     * @return fully qualified name of function, or null for a builtin function
     */
    public SqlIdentifier getSqlIdentifier()
    {
        return sqlIdentifier;
    }

    /**
     * @return fully qualified name of function
     */
    public SqlIdentifier getNameAsId()
    {
        if (sqlIdentifier != null) {
            return sqlIdentifier;
        }
        return new SqlIdentifier(
            getName(),
            SqlParserPos.ZERO);
    }

    /**
     * @return array of parameter types, or null for builtin function
     */
    public RelDataType [] getParamTypes()
    {
        return paramTypes;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        getSyntax().unparse(writer, this, operands, leftPrec, rightPrec);
    }

    /**
     * @return function category
     */
    public SqlFunctionCategory getFunctionType()
    {
        return this.functionType;
    }

    /**
     * Returns whether this function allows a <code>DISTINCT</code> or <code>
     * ALL</code> quantifier. The default is <code>false</code>; some aggregate
     * functions return <code>true</code>.
     */
    public boolean isQuantifierAllowed()
    {
        return false;
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        // This implementation looks for the quantifier keywords DISTINCT or
        // ALL as the first operand in the list.  If found then the literal is
        // not called to validate itself.  Further the function is checked to
        // make sure that a quantifier is valid for that particular function.
        //
        // If the first operand does not appear to be a quantifier then the
        // parent ValidateCall is invoked to do normal function validation.

        super.validateCall(call, validator, scope, operandScope);
        validateQuantifier(validator, call);
    }

    /**
     * Throws a validation error if a DISTINCT or ALL quantifier is present but
     * not allowed.
     */
    protected void validateQuantifier(SqlValidator validator, SqlCall call)
    {
        if ((null != call.getFunctionQuantifier()) && !isQuantifierAllowed()) {
            throw validator.newValidationError(
                call.getFunctionQuantifier(),
                EigenbaseResource.instance().FunctionQuantifierNotAllowed.ex(
                    call.getOperator().getName()));
        }
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        return deriveType(validator, scope, call, true);
    }

    private RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        boolean convertRowArgToColumnList)
    {
        final SqlNode [] operands = call.operands;
        RelDataType [] argTypes = new RelDataType[operands.length];

        // Scope for operands. Usually the same as 'scope'.
        final SqlValidatorScope operandScope = scope.getOperandScope(call);

        // Indicate to the validator that we're validating a new function call
        validator.pushFunctionCall();

        try {
            boolean containsRowArg = false;
            for (int i = 0; i < operands.length; ++i) {
                RelDataType nodeType;

                // for row arguments that should be converted to ColumnList
                // types, set the nodeType to a ColumnList type but defer
                // validating the arguments of the row constructor until we know
                // for sure that the row argument maps to a ColumnList type
                if (operands[i].getKind() == SqlKind.ROW
                    && convertRowArgToColumnList)
                {
                    containsRowArg = true;
                    RelDataTypeFactory typeFactory = validator.getTypeFactory();
                    nodeType =
                        typeFactory.createSqlType(SqlTypeName.COLUMN_LIST);
                } else {
                    nodeType = validator.deriveType(operandScope, operands[i]);
                }
                validator.setValidatedNodeType(operands[i], nodeType);
                argTypes[i] = nodeType;
            }

            SqlFunction function =
                SqlUtil.lookupRoutine(
                    validator.getOperatorTable(),
                    getNameAsId(),
                    argTypes,
                    getFunctionType());

            // if we have a match on function name and parameter count, but
            // couldn't find a function with  a COLUMN_LIST type, retry, but
            // this time, don't convert the row argument to a COLUMN_LIST type;
            // if we did find a match, go back and revalidate the row operands
            // (corresponding to column references), now that we can set the
            // scope to that of the source cursor referenced by that ColumnList
            // type
            if (containsRowArg) {
                if ((function == null)
                    && SqlUtil.matchRoutinesByParameterCount(
                        validator.getOperatorTable(),
                        getNameAsId(),
                        argTypes,
                        getFunctionType()))
                {
                    // remove the already validated node types corresponding to
                    // row arguments before revalidating
                    for (SqlNode operand : operands) {
                        if (operand.getKind() == SqlKind.ROW) {
                            validator.removeValidatedNodeType(operand);
                        }
                    }
                    return deriveType(validator, scope, call, false);
                } else if (function != null) {
                    validator.validateColumnListParams(
                        function,
                        argTypes,
                        operands);
                }
            }

            if (getFunctionType()
                == SqlFunctionCategory.UserDefinedConstructor)
            {
                return validator.deriveConstructorType(
                    scope,
                    call,
                    this,
                    function,
                    argTypes);
            }
            if (function == null) {
                validator.handleUnresolvedFunction(
                    call,
                    this,
                    argTypes);
            }

            // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
            // identifiers, but we ignore shouldExpandIdentifiers()
            // because otherwise later validation code will
            // choke on the unresolved function.
            call.setOperator(function);
            return function.validateOperands(
                validator,
                operandScope,
                call);
        } finally {
            validator.popFunctionCall();
        }
    }
}

// End SqlFunction.java
