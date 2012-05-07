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


/**
 * <code>RestartableCollectionTupleIter</code> implements the {@link TupleIter}
 * interface in terms of an underlying {@link Collection}. It is used to
 * implement {@link org.eigenbase.oj.rel.IterOneRowRel}.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class RestartableCollectionTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final Collection collection;
    private Iterator iterator;
    private volatile MoreDataListener listener;

    //~ Constructors -----------------------------------------------------------

    // this handles the case where we thought a join was one-to-many
    // but it's actually one-to-one
    public RestartableCollectionTupleIter(Object obj)
    {
        if (obj instanceof Collection) {
            collection = (Collection) obj;
        } else {
            collection = Collections.singleton(obj);
        }
        iterator = collection.iterator();
        listener = null;
    }

    public RestartableCollectionTupleIter(Collection collection)
    {
        this.collection = collection;
        iterator = collection.iterator();
        listener = null;
    }

    //~ Methods ----------------------------------------------------------------
    // implement TupleIter
    public boolean addListener(MoreDataListener c)
    {
        listener = c;
        return true;
    }

    // implement TupleIter
    public Object fetchNext()
    {
        if (iterator.hasNext()) {
            return iterator.next();
        }

        return NoDataReason.END_OF_DATA;
    }

    // implement TupleIter
    public void restart()
    {
        iterator = collection.iterator();
        MoreDataListener listener = this.listener;
        if (listener != null) {
            listener.onMoreData();
        }
    }

    // implement TupleIter
    public void closeAllocation()
    {
        iterator = null;
    }
}

// End RestartableCollectionTupleIter.java
