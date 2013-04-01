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

import org.eigenbase.util.*;


/**
 * A <code>IteratorResultSet</code> is an adapter which converts a {@link
 * java.util.Iterator} into a {@link java.sql.ResultSet}.
 */
public abstract class IteratorResultSet
    extends AbstractIterResultSet
{
    //~ Instance fields --------------------------------------------------------

    private final Iterator iterator;
    private TimeoutQueueIterator timeoutIter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     *
     * @pre iterator != null
     */
    public IteratorResultSet(
        Iterator iterator,
        ColumnGetter columnGetter)
    {
        super(columnGetter);

        Util.pre(iterator != null, "iterator != null");
        this.iterator = iterator;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Factory method. Returns a suitable implementation for the current JDBC
     * version.
     *
     * @param iterator Underlying iterator
     * @param columnGetter Accesses columns
     */
    public static IteratorResultSet create(
        Iterator iterator,
        ColumnGetter columnGetter)
    {
        return new IteratorResultSetJdbc41(iterator, columnGetter);
    }

    /**
     * Sets the timeout that this IteratorResultSet will wait for a row from the
     * underlying iterator.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     */
    public void setTimeout(long timeoutMillis)
    {
        super.setTimeout(timeoutMillis);

        assert timeoutIter == null;

        // we create a new semaphore for each executeQuery call
        // and then pass ownership to the result set returned
        // the query timeout used is the last set via JDBC.
        timeoutIter = new TimeoutQueueIterator(iterator);
        timeoutIter.start();
    }

    public void close()
        throws SQLException
    {
        if (timeoutIter != null) {
            final long noTimeout = 0;
            timeoutIter.close(noTimeout);
            timeoutIter = null;
        }
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next()
        throws SQLException
    {
        if (timeoutIter != null) {
            try {
                long endTime = System.currentTimeMillis() + timeoutMillis;
                if (timeoutIter.hasNext(timeoutMillis)) {
                    long remainingTimeout =
                        endTime - System.currentTimeMillis();
                    if (remainingTimeout <= 0) {
                        // The call to hasNext() took longer than we
                        // expected -- we're out of time.
                        throw new SqlTimeoutException();
                    }
                    this.current = timeoutIter.next(remainingTimeout);
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (QueueIterator.TimeoutException e) {
                throw new SqlTimeoutException();
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        } else {
            try {
                if (iterator.hasNext()) {
                    this.current = iterator.next();
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        }
    }

    private static class IteratorResultSetJdbc41 extends IteratorResultSet {
        public IteratorResultSetJdbc41(
            Iterator iterator, ColumnGetter columnGetter)
        {
            super(iterator, columnGetter);
        }

        public <T> T getObject(
            int columnIndex,
            Class<T> type) throws SQLException
        {
            return type.cast(getObject(columnIndex));
        }

        public <T> T getObject(
            String columnLabel, Class<T> type) throws SQLException
        {
            return type.cast(getObject(columnLabel));
        }
    }
}

// End IteratorResultSet.java
