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
package org.eigenbase.test;

import java.util.Arrays;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator
 * table, plus a list of operators.
 */
public class MockSqlOperatorTable
    extends ChainedSqlOperatorTable
{
    //~ Instance fields --------------------------------------------------------

    private final ListSqlOperatorTable listOpTab;

    //~ Constructors -----------------------------------------------------------

    public MockSqlOperatorTable(SqlOperatorTable parentTable)
    {
        super(
            Arrays.<SqlOperatorTable>asList(
                parentTable, new ListSqlOperatorTable()));
        listOpTab = (ListSqlOperatorTable) tableList.get(1);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds an operator to this table.
     */
    public void addOperator(SqlOperator op)
    {
        listOpTab.add(op);
    }

    public static void addRamp(MockSqlOperatorTable opTab)
    {
        opTab.addOperator(
            new SqlFunction(
                "RAMP",
                SqlKind.OTHER_FUNCTION,
                null,
                null,
                SqlTypeStrategies.otcNumeric,
                SqlFunctionCategory.UserDefinedFunction)
            {
                public RelDataType inferReturnType(
                    SqlOperatorBinding opBinding)
                {
                    final RelDataTypeFactory typeFactory =
                        opBinding.getTypeFactory();
                    final RelDataType [] types =
                    { typeFactory.createSqlType(SqlTypeName.INTEGER) };
                    final String [] fieldNames = new String[] { "I" };
                    return typeFactory.createStructType(types, fieldNames);
                }
            });

        opTab.addOperator(
            new SqlFunction(
                "DEDUP",
                SqlKind.OTHER_FUNCTION,
                null,
                null,
                SqlTypeStrategies.otcVariadic,
                SqlFunctionCategory.UserDefinedFunction)
            {
                public RelDataType inferReturnType(
                    SqlOperatorBinding opBinding)
                {
                    final RelDataTypeFactory typeFactory =
                        opBinding.getTypeFactory();
                    final RelDataType [] types =
                    { typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024) };
                    final String [] fieldNames = new String[] { "NAME" };
                    return typeFactory.createStructType(types, fieldNames);
                }
            });
    }
}

// End MockSqlOperatorTable.java
