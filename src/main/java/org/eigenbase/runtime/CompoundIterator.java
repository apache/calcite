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

import java.util.*;
import java.util.logging.*;

import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * <code>CompoundIterator</code> creates an iterator out of several.
 * CompoundIterator is serial: it yields all the elements of its first input
 * Iterator, then all those of its second input, etc. When all inputs are
 * exhausted, it is done.
 *
 * <p>NOTE jvs 21-Mar-2006: This class is no longer used except by Saffron, but
 * is generally useful. Should probably be moved to a utility package.
 */
public class CompoundIterator
    implements RestartableIterator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getCompoundIteratorTracer();

    //~ Instance fields --------------------------------------------------------

    private Iterator iterator;
    private Iterator [] iterators;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundIterator(Iterator [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        tracer.finer(toString());
        if (iterator == null) {
            return nextIterator();
        }
        if (iterator.hasNext()) {
            return true;
        }
        return nextIterator();
    }

    public Object next()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        return iterator.next();
    }

    public void remove()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        iterator.remove();
    }

    // moves to the next child iterator, skipping any empty ones, and returns
    // true. when all the child iteratators are used up, return false;
    private boolean nextIterator()
    {
        while (i < iterators.length) {
            iterator = iterators[i++];
            tracer.fine("try " + iterator);
            if (iterator.hasNext()) {
                return true;
            }
        }
        tracer.fine("exhausted iterators");
        iterator = Collections.EMPTY_LIST.iterator();
        return false;
    }

    // implement RestartableIterator
    public void restart()
    {
        for (int j = 0; j < i; ++j) {
            Util.restartIterator(iterators[j]);
        }
        i = 0;
        iterator = null;
    }
}
// End CompoundIterator.java
