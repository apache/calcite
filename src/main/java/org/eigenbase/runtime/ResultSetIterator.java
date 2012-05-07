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

import java.util.*;


/**
 * A <code>ResultSetIterator</code> is an adapter which converts a {@link
 * ResultSet} to a {@link Iterator}.
 *
 * <p>NOTE jvs 21-Mar-2006: This class is no longer used except by Saffron, but
 * is generally useful. Should probably be moved to a utility package.
 */
public class ResultSetIterator
    implements RestartableIterator
{
    //~ Instance fields --------------------------------------------------------

    protected ResultSet resultSet;
    private Object row;
    private boolean endOfStream;

    //~ Constructors -----------------------------------------------------------

    public ResultSetIterator(ResultSet resultSet)
    {
        // NOTE jvs 4-Mar-2004:  I changed this to not call makeRow() from
        // this constructor, since subclasses aren't initialized yet.  Now
        // it follows the same pattern as CalcTupleIter.
        this.resultSet = resultSet;
        endOfStream = false;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        if (row != null) {
            return true;
        }
        moveToNext();
        return row != null;
    }

    public Object next()
    {
        if (row == null) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
        }
        Object result = row;
        row = null;
        return result;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void restart()
    {
        try {
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

    /**
     * Creates an object representing the current row of the result set. The
     * default implementation of this method returns a {@link Row}, but derived
     * classes may override this.
     */
    protected Object makeRow()
        throws SQLException
    {
        return new Row(resultSet);
    }

    private void moveToNext()
    {
        try {
            if (endOfStream) {
                return;
            }
            if (resultSet.next()) {
                row = makeRow();
            } else {
                // record endOfStream since some ResultSet implementations don't
                // like extra calls to next() after it returns false
                endOfStream = true;
                row = null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

// End ResultSetIterator.java
