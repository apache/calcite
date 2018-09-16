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
 * Abstract implementation of the {@link org.apache.calcite.linq4j.Enumerable}
 * interface that implements the extension methods.
 *
 * <p>It is helpful to derive from this class if you are implementing
 * {@code Enumerable}, because {@code Enumerable} has so many extension methods,
 * but it is not required.</p>
 *
 * @param <T> Element type
 */
public abstract class AbstractEnumerable2<T> extends DefaultEnumerable<T> {
  public Enumerator<T> enumerator() {
    return new Linq4j.IterableEnumerator<>(this);
  }
}

// End AbstractEnumerable2.java
