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

import java.util.logging.*;

import org.eigenbase.trace.*;


/**
 * <code>CompoundTupleIter</code> creates an iterator out of several iterators.
 *
 * <p>CompoundTupleIter is serial: it yields all the elements of its first input
 * Iterator, then all those of its second input, etc. When all inputs are
 * exhausted, it is done. (Cf {@link CompoundParallelTupleIter}.)
 *
 * @author Stephan Zuercher
 */
public class CompoundTupleIter
    implements TupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getCompoundIteratorTracer();

    //~ Instance fields --------------------------------------------------------

    private TupleIter iterator;
    private TupleIter [] iterators;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundTupleIter(TupleIter [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;

        initIterator();
    }

    //~ Methods ----------------------------------------------------------------

    private void initIterator()
    {
        if (this.iterators.length > 0) {
            this.iterator = this.iterators[0];
        } else {
            this.iterator = TupleIter.EMPTY_ITERATOR;
        }
    }

    public boolean addListener(MoreDataListener c)
    {
        return iterator.addListener(c);
    }

    public boolean setTimeout(long timeout, boolean asUnderflow)
    {
        // try to set a timeout on all underlings, but return false if any
        // refused.
        boolean result = true;
        for (int i = 0; i < iterators.length; i++) {
            result &= iterators[i].setTimeout(timeout, asUnderflow);
        }
        return result;
    }

    public Object fetchNext()
    {
        tracer.finer(toString());
        Object next = iterator.fetchNext();

        if (next == NoDataReason.END_OF_DATA) {
            i++;
            if (i < iterators.length) {
                iterator = iterators[i];
                tracer.fine("try " + iterator);
                return fetchNext();
            }

            return NoDataReason.END_OF_DATA;
        }

        return next;
    }

    public void restart()
    {
        // fetchNext() can be called repeatedly after it returns END_OF_DATA.
        // Even if it isn't, it uses recursion which implies an extra call
        // when END_OF_DATA on the last iterator is reached.
        // Each extra call increments i.  We want to restart all iterators
        // that we've touched (e.g. 0 to i, inclusive) but need to compensate
        // i that's grown too large.
        final int maxIndex = Math.min(i, iterators.length - 1);
        for (int index = 0; index <= maxIndex; index++) {
            iterators[index].restart();
        }

        i = 0;
        initIterator();
    }

    public StringBuilder printStatus(StringBuilder b)
    {
        return iterator.printStatus(b);
    }

    public void closeAllocation()
    {
        for (TupleIter iter : iterators) {
            iter.closeAllocation();
        }
    }
}

// End CompoundTupleIter.java
