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
package org.eigenbase.sql.util;

import java.util.*;

import org.eigenbase.sql.*;


/**
 * ChainedSqlOperatorTable implements the {@link SqlOperatorTable} interface by
 * chaining together any number of underlying operator table instances.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ChainedSqlOperatorTable
    implements SqlOperatorTable
{
    //~ Instance fields --------------------------------------------------------

    protected final List<SqlOperatorTable> tableList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a table based on a given list.
     */
    public ChainedSqlOperatorTable(List<SqlOperatorTable> tableList) {
        this.tableList = tableList;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds an underlying table. The order in which tables are added is
     * significant; tables added earlier have higher lookup precedence. A table
     * is not added if it is already on the list.
     *
     * @param table table to add
     */
    public void add(SqlOperatorTable table)
    {
        if (!tableList.contains(table)) {
            tableList.add(table);
        }
    }

    // implement SqlOperatorTable
    public List<SqlOperator> lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlFunctionCategory category,
        SqlSyntax syntax)
    {
        List<SqlOperator> list = new ArrayList<SqlOperator>();
        for (int i = 0; i < tableList.size(); ++i) {
            SqlOperatorTable table = tableList.get(i);
            list.addAll(
                table.lookupOperatorOverloads(opName, category, syntax));
        }
        return list;
    }

    // implement SqlOperatorTable
    public List<SqlOperator> getOperatorList()
    {
        List<SqlOperator> list = new ArrayList<SqlOperator>();
        for (SqlOperatorTable table : tableList) {
            list.addAll(table.getOperatorList());
        }
        return list;
    }
}

// End ChainedSqlOperatorTable.java
