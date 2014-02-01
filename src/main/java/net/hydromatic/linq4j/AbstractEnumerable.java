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
 * Abstract implementation of the {@link Enumerable} interface that
 * implements the extension methods.
 *
 * <p>It is helpful to derive from this class if you are implementing
 * {@code Enumerable}, because {@code Enumerable} has so many extension methods,
 * but it is not required.</p>
 *
 * @param <T> Element type
 */
public abstract class AbstractEnumerable<T> extends DefaultEnumerable<T> {
  public Iterator<T> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }
}

// End AbstractEnumerable.java
