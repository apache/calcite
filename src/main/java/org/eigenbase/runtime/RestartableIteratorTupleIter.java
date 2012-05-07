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

import java.util.*;


// REVIEW mberkowitz 1-Nov-2008. This adapter is used only to present a
// FarragoJavaUdxIterator as a TupleIter. Redundant since a
// FarragoJavaUdxIterator can be a TupleIter itself, and provide a correct,
// non-blocking fetchNext(). However some farrago queries depend on fetchNext()
// to block: eg in unitsql/expressions/udfInvocation.sql, SELECT * FROM
// TABLE(RAMP(5)) ORDER BY 1;
//
// Consequently, I've made FarragoJavaUdxIterator implement TupleIter as well as
// RestartableIterator, but as a kludge I've retained this adapter for farrago
// queries.

/**
 * <code>RestartableIteratorTupleIter</code> adapts an underlying {@link
 * RestartableIterator} as a {@link TupleIter}. It is an imperfect adaptor;
 * {@link #fetchNext} blocks when a real TupleIter would return {@link
 * TupleIter.NoDataReason#UNDERFLOW}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RestartableIteratorTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final RestartableIterator iterator;

    //~ Constructors -----------------------------------------------------------

    public RestartableIteratorTupleIter(RestartableIterator iterator)
    {
        this.iterator = iterator;
    }

    //~ Methods ----------------------------------------------------------------

    // implement TupleIter
    public Object fetchNext()
    {
        if (iterator.hasNext()) {
            // blocks
            return iterator.next();
        }

        return NoDataReason.END_OF_DATA;
    }

    // implement TupleIter
    public void restart()
    {
        iterator.restart();
    }

    // implement TupleIter
    public void closeAllocation()
    {
    }
}

// End RestartableIteratorTupleIter.java
