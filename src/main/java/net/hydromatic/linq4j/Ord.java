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
package net.hydromatic.linq4j;

import java.util.Iterator;

/**
 * Pair of an element and an ordinal.
 */
public class Ord<E> {
    public final int n;
    public final E e;

    private Ord(int n, E e) {
        this.n = n;
        this.e = e;
    }

    public static <E> Ord<E> of(int n, E e) {
        return new Ord<E>(n, e);
    }

    public static <E> Iterable<Ord<E>> of(final Iterable<E> iterable) {
        return new Iterable<Ord<E>>() {
            public Iterator<Ord<E>> iterator() {
                return of(iterable.iterator());
            }
        };
    }

    public static <E> Iterator<Ord<E>> of(final Iterator<E> iterator) {
        return new Iterator<Ord<E>>() {
            int n = 0;

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Ord<E> next() {
                return Ord.of(n++, iterator.next());
            }

            public void remove() {
                iterator.remove();
            }
        };
    }
}

// End Ord.java
