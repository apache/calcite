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

/**
 * Simple enumerator that just delegates all calls to the passed enumerator.
 *
 * @param <T> type of value to return, as passed from the delegate enumerator
 */
public class DelegatingEnumerator<T> implements Enumerator<T> {
  protected final Enumerator<T> delegate;

  public DelegatingEnumerator(Enumerator<T> delegate) {
    this.delegate = delegate;
  }

  @Override public T current() {
    return delegate.current();
  }

  @Override public boolean moveNext() {
    return delegate.moveNext();
  }

  @Override public void reset() {
    delegate.reset();
  }

  @Override public void close() {
    delegate.close();
  }
}

// End DelegatingEnumerator.java
