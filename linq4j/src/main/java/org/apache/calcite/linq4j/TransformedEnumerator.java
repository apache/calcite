/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.linq4j;

/** Enumerator that applies a transform to each value from a backing
 * enumerator.
 *
 * @param <F> Element type of backing enumerator
 * @param <E> Element type
 */
public abstract class TransformedEnumerator<F, E> implements Enumerator<E> {
  protected final Enumerator<F> enumerator;

  protected TransformedEnumerator(Enumerator<F> enumerator) {
    this.enumerator = enumerator;
  }

  protected abstract E transform(F from);

  @Override public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override public E current() {
    return transform(enumerator.current());
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }
}
