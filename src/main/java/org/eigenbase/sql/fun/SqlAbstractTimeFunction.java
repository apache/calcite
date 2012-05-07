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
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlAbstractTimeFunction
    extends SqlFunction
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlOperandTypeChecker otcCustom =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.OR,
            SqlTypeStrategies.otcPositiveIntLit,
            SqlTypeStrategies.otcNiladic);

    //~ Instance fields --------------------------------------------------------

    private final SqlTypeName typeName;

    //~ Constructors -----------------------------------------------------------

    protected SqlAbstractTimeFunction(String name, SqlTypeName typeName)
    {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            null,
            null,
            otcCustom,
            SqlFunctionCategory.TimeDate);
        this.typeName = typeName;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.FunctionId;
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        // REVIEW jvs 20-Feb-2005: Need to take care of time zones.
        int precision = 0;
        if (opBinding.getOperandCount() == 1) {
            RelDataType type = opBinding.getOperandType(0);
            if (SqlTypeUtil.isNumeric(type)) {
                precision = opBinding.getIntLiteralOperand(0);
            }
        }
        assert (precision >= 0);
        if (precision > SqlTypeName.MAX_DATETIME_PRECISION) {
            throw opBinding.newError(
                EigenbaseResource.instance().ArgumentMustBeValidPrecision.ex(
                    opBinding.getOperator().getName(),
                    "0",
                    String.valueOf(SqlTypeName.MAX_DATETIME_PRECISION)));
        }
        return opBinding.getTypeFactory().createSqlType(typeName, precision);
    }

    // All of the time functions are increasing. Not strictly increasing.
    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        return SqlMonotonicity.Increasing;
    }

    // Plans referencing context variables should never be cached
    public boolean isDynamicFunction()
    {
        return true;
    }
}

// End SqlAbstractTimeFunction.java
