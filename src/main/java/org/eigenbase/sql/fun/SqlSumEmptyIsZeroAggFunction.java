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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * <code>Sum0</code> is an aggregator which returns the sum of the values which
 * go into it like <code>Sum</code>. It differs in that when no non null values
 * are applied zero is returned instead of null. Can be used along with <code>
 * Count</code> to implement <code>Sum</code>.
 *
 * @author jhahn
 * @version $Id$
 */
public class SqlSumEmptyIsZeroAggFunction
    extends SqlAggFunction
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    public SqlSumEmptyIsZeroAggFunction(RelDataType type)
    {
        super(
            "$SUM0",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType[] { type };
    }

    public RelDataType getType()
    {
        return type;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return type;
    }

}

// End SqlSumEmptyIsZeroAggFunction.java
