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
package org.eigenbase.runtime;

import java.sql.*;


/**
 * When a relational expression obeys the {@link
 * org.eigenbase.relopt.CallingConvention#RESULT_SET result set calling
 * convention}, and does not explicitly specify a row type, the results are
 * object of type <code>Row</code>.
 */
public class Row
{
    //~ Instance fields --------------------------------------------------------

    ResultSet resultSet;
    Object [] values;

    //~ Constructors -----------------------------------------------------------

    public Row(ResultSet resultSet)
        throws SQLException
    {
        this.resultSet = resultSet;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        final int count = resultSetMetaData.getColumnCount();
        this.values = new Object[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = resultSet.getObject(i + 1);
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the value of a given column, similar to {@link
     * ResultSet#getObject(int)}.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     *
     * @return a <code>java.lang.Object</code> holding the column value
     */
    public Object getObject(int columnIndex)
    {
        return values[columnIndex - 1];
    }

    /**
     * Returns the result set that this row belongs to.
     */
    public ResultSet getResultSet()
    {
        return resultSet;
    }
}

// End Row.java
