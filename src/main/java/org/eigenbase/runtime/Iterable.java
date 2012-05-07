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
 * An object is <code>Iterable</code> if it has an {@link #iterator} method to
 * create an {@link Iterator} over its elements.
 *
 * <p>Some implementations of this interface may allow only one iterator at a
 * time. For example, {@link BufferedIterator} simply restarts and returns
 * itself. Iterators received from previous calls to {@link #iterator} will also
 * restart.</p>
 *
 * <p>If an object implements this interface, it can be used as a relation in a
 * saffron relational expression. For example,
 *
 * <blockquote>
 * <pre>Iterable iterable = new Iterable() {
 *     public Iterator iterator() {
 *         ArrayList list = new ArrayList();
 *         list.add(new Integer(1));
 *         list.add(new Integer(2));
 *         return list.iterator();
 *     }
 * };
 * for (i in (Integer[]) iterable) {
 *     print(i.intValue());
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since 1 May, 2002
 */
public interface Iterable
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns an iterator over the elements in this collection. There are no
     * guarantees over the order in which the elements are returned.
     *
     * <p>If this method is called twice on the same object, and the object is
     * not modified in between times, the iterators produced may or may not be
     * the same iterator, and may or may not return the elements in the same
     * order, but must return the same objects.</p>
     */
    Iterator iterator();
}

// End Iterable.java
