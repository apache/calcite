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
package org.eigenbase.runtime;

import java.sql.*;


/**
 * A <code>ResultSetTupleIter</code> is an adapter which converts a {@link
 * ResultSet} to a {@link TupleIter}.
 */
public class ResultSetTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    protected ResultSetProvider resultSetProvider;
    protected ResultSet resultSet;
    protected boolean endOfStream;
    protected boolean underflow;
    protected Object row;

    //~ Constructors -----------------------------------------------------------

    public ResultSetTupleIter(ResultSetProvider resultSetProvider)
    {
        // NOTE jvs 4-Mar-2004:  I changed this to not call makeRow() from
        // this constructor, since subclasses aren't initialized yet.  Now
        // it follows the same pattern as CalcTupleIter.
        this.resultSetProvider = resultSetProvider;
        underflow = endOfStream = false;
    }

    //~ Methods ----------------------------------------------------------------

    public Object fetchNext()
    {
        underflow = false;              // trying again
        // here row may not be null, after restart()
        if (row == null && !endOfStream) {
            row = getNextRow();
        }
        if (endOfStream) {
            return NoDataReason.END_OF_DATA;
        } else if (underflow) {
            return NoDataReason.UNDERFLOW;
        }
        Object result = row;
        row = null;
        return result;
    }

    /**
     * Instantiates the result set from the result set provider, if it has not
     * been instantiated already.  Typically this method is called on first
     * fetch.
     */
    protected void instantiateResultSet() throws SQLException
    {
        if (resultSet == null) {
            resultSet = resultSetProvider.getResultSet();
        }
    }

    protected Object getNextRow() throws TimeoutException
    {
        try {
            instantiateResultSet();
            if (resultSet.next()) {
                return makeRow();
            } else {
                // remember EOS, some ResultSet impls dislike an extra next()
                endOfStream = true;
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void restart()
    {
        try {
            instantiateResultSet();
            if (resultSet.first()) {
                endOfStream = false;
                row = makeRow();
            } else {
                row = null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeAllocation()
    {
        // REVIEW: SWZ: 2/23/2006: Call close on resultSet?
        // resultSet.close();
    }

    /**
     * Creates an object representing the current row of the result set. The
     * default implementation of this method returns a {@link Row}, but derived
     * classes may override this.
     */
    protected Object makeRow()
        throws SQLException
    {
        assert resultSet != null;
        return new Row(resultSet);
    }
}

// End ResultSetTupleIter.java
