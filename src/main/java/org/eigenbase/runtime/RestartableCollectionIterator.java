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
 * <code>RestartableCollectionIterator</code> implements the {@link
 * RestartableIterator} interface in terms of an underlying {@link Collection}.
 *
 * <p>TODO jvs 21-Mar-2006: This class is no longer used except by Saffron, so
 * we should move it to Saffron.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RestartableCollectionIterator
    implements RestartableIterator
{
    //~ Instance fields --------------------------------------------------------

    private final Collection collection;
    private Iterator iterator;

    //~ Constructors -----------------------------------------------------------

    public RestartableCollectionIterator(Collection collection)
    {
        this.collection = collection;
        iterator = collection.iterator();
    }

    //~ Methods ----------------------------------------------------------------

    // implement Iterator
    public Object next()
    {
        return iterator.next();
    }

    // implement Iterator
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    // implement Iterator
    public void remove()
    {
        iterator.remove();
    }

    // implement RestartableIterator
    public void restart()
    {
        iterator = collection.iterator();
    }
}

// End RestartableCollectionIterator.java
