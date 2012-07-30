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


/**
 * <code>BufferedIterator</code> converts a regular iterator into one which
 * implements {@link Iterable} (and {@link Enumeration} for good measure).
 *
 * <p><i>Implementation note</i>: The first time you read from it, it duplicates
 * objects into a list. The next time, it creates an iterator from that list.
 * The implementation handles infinite iterators gracefully: it copies objects
 * onto the replay list only when they are requested for the first time.</p>
 *
 * @author jhyde
 * @since 26 April, 2002
 */
public class BufferedIterator
    implements Iterator,
        Iterable,
        Enumeration
{
    //~ Instance fields --------------------------------------------------------

    private Clonerator clonerator;
    private Iterator iterator;
    private List list;

    //~ Constructors -----------------------------------------------------------

    public BufferedIterator(Iterator iterator)
    {
        this.list = new ArrayList();
        this.clonerator = new Clonerator(iterator, list);
        this.iterator = clonerator;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Enumeration
    public boolean hasMoreElements()
    {
        return iterator.hasNext();
    }

    // implement Iterator
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    // implement Iterable
    public Iterator iterator()
    {
        restart();
        return this;
    }

    // implement Iterator
    public Object next()
    {
        return iterator.next();
    }

    // implement Enumeration
    public Object nextElement()
    {
        return iterator.next();
    }

    // implement Iterator
    public void remove()
    {
        iterator.remove();
    }

    // implement Restartable
    public void restart()
    {
        if (clonerator == null) {
            // We have already read everything from the clonerator and
            // discarded it.
            iterator = list.iterator();
        } else if (!clonerator.hasNext()) {
            // They read everything from the clonerator. We can discard it
            // now.
            clonerator = null;
            iterator = list.iterator();
        } else {
            // Still stuff left in the clonerator. Create a compound
            // iterator, so that if they go further next time, it will
            // read later stuff from the clonerator.
            iterator =
                new CompoundIterator(
                    new Iterator[] { list.iterator(), clonerator });
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Reads from an iterator, duplicating elements into a list as it does so.
     */
    static class Clonerator
        implements Iterator
    {
        Iterator iterator;
        List list;

        Clonerator(
            Iterator iterator,
            List list)
        {
            this.iterator = iterator;
            this.list = list;
        }

        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        public Object next()
        {
            Object o = iterator.next();
            list.add(o);
            return o;
        }

        public void remove()
        {
            iterator.remove();
        }
    }
}

// End BufferedIterator.java
