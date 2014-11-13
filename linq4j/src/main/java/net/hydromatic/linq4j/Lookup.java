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

import net.hydromatic.linq4j.function.Function2;

import java.util.Map;

/**
 * Represents a collection of keys each mapped to one or more values.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface Lookup<K, V>
    extends Map<K, Enumerable<V>>, Enumerable<Grouping<K, V>> {
  /**
   * Applies a transform function to each key and its associated values and
   * returns the results.
   *
   * @param resultSelector Result selector
   * @param <TResult> Result type
   *
   * @return Enumerable over results
   */
  <TResult> Enumerable<TResult> applyResultSelector(
      Function2<K, Enumerable<V>, TResult> resultSelector);
}

// End Lookup.java
