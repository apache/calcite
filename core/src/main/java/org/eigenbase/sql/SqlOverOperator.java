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
package org.eigenbase.sql;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;


/**
 * An operator describing a window function specification.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: name of window function ({@link org.eigenbase.sql.SqlCall})</li>
 * <li>1: window name ({@link org.eigenbase.sql.SqlLiteral}) or window in-line
 * specification ({@link SqlWindowOperator})</li>
 * </ul>
 * </p>
 *
 * @author klo
 * @version $Id$
 * @since Nov 4, 2004
 */
public class SqlOverOperator
    extends SqlBinaryOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlOverOperator()
    {
        super(
            "OVER",
            SqlKind.OVER,
            20,
            true,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcAnyX2);
    }

    //~ Methods ----------------------------------------------------------------

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        assert call.getOperator() == this;
        final SqlNode [] operands = call.getOperands();
        assert operands.length == 2;
        SqlCall aggCall = (SqlCall) operands[0];
        if (!aggCall.getOperator().isAggregator()) {
            throw validator.newValidationError(
                aggCall,
                EigenbaseResource.instance().OverNonAggregate.ex());
        }
        validator.validateWindow(operands[1], scope, aggCall);
        validator.validateAggregateParams(aggCall, scope);
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        // Do not try to derive the types of the operands. We will do that
        // later, top down.
        return validateOperands(validator, scope, call);
    }

    /**
     * Accepts a {@link SqlVisitor}, and tells it to visit each child.
     *
     * @param visitor Visitor
     */
    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];

                // if the second parm is an Identifier then it's supposed to
                // be a name from a window clause and isn't part of the
                // group by check
                if (operand == null) {
                    continue;
                }
                if ((i == 1) && (operand instanceof SqlIdentifier)) {
                    continue;
                }
                argHandler.visitChild(visitor, call, i, operand);
            }
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }
}

// End SqlOverOperator.java
