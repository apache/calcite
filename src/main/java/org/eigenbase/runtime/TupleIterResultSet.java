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

import org.eigenbase.util.*;


public abstract class TupleIterResultSet
    extends AbstractIterResultSet
{
    //~ Instance fields --------------------------------------------------------

    private final TupleIter tupleIter;
    private TimeoutQueueTupleIter timeoutTupleIter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     *
     * @pre tupleIter != null
     */
    public TupleIterResultSet(
        TupleIter tupleIter,
        ColumnGetter columnGetter)
    {
        super(columnGetter);

        Util.pre(tupleIter != null, "tupleIter != null");
        this.tupleIter = tupleIter;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the timeout that this TupleIterResultSet will wait for a row from
     * the underlying iterator.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     */
    public void setTimeout(long timeoutMillis)
    {
        super.setTimeout(timeoutMillis);

        assert timeoutTupleIter == null;

        // we create a new semaphore for each executeQuery call
        // and then pass ownership to the result set returned
        // the query timeout used is the last set via JDBC.
        timeoutTupleIter = new TimeoutQueueTupleIter(tupleIter);
        timeoutTupleIter.start();
    }

    public void close()
        throws SQLException
    {
        if (timeoutTupleIter != null) {
            final long noTimeout = 0;
            timeoutTupleIter.closeAllocation(noTimeout);
            timeoutTupleIter = null;
        }
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next()
        throws SQLException
    {
        if (maxRows > 0) {
            if (row >= maxRows) {
                return false;
            }
        }

        try {
            Object next =
                (timeoutTupleIter != null)
                ? timeoutTupleIter.fetchNext(timeoutMillis)
                : tupleIter.fetchNext();

            if (next == TupleIter.NoDataReason.END_OF_DATA) {
                return false;
            } else if (next instanceof TupleIter.NoDataReason) {
                // TODO: SWZ: 2/23/2006: better exception
                throw new RuntimeException();
            }

            current = next;
            row++;
            return true;
        } catch (QueueIterator.TimeoutException e) {
            throw new SqlTimeoutException();
        } catch (Throwable e) {
            throw newFetchError(e);
        }
    }
}

// End TupleIterResultSet.java
