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
package org.apache.calcite.linq4j.function;

/**
 * Function with two parameters returning a native {@code boolean} value.
 *
 * @param <T0> Type of argument #0
 * @param <T1> Type of argument #1
 */
public interface Predicate2<T0, T1> extends Function<Boolean> {
  /**
   * Predicate that always evaluates to {@code true}.
   *
   * @see org.apache.calcite.linq4j.function.Functions#truePredicate1()
   */
  Predicate2<Object, Object> TRUE = (v0, v1) -> true;

  /**
   * Predicate that always evaluates to {@code false}.
   *
   * @see org.apache.calcite.linq4j.function.Functions#falsePredicate1()
   */
  Predicate2<Object, Object> FALSE = (v0, v1) -> false;

  boolean apply(T0 v0, T1 v1);
}

// End Predicate2.java
