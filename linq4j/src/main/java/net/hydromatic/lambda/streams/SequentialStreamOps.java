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
package net.hydromatic.lambda.streams;

import net.hydromatic.lambda.functions.*;

/**
 * Operations on streams.
 *
 * <p>Based on {@code java.util.streams.SequentialStreamOps}.</p>
 */
public interface SequentialStreamOps<T> {
  Stream<T> filter(Predicate<? super T> predicate);

  <R> Stream<R> map(Mapper<? super T, ? extends R> mapper);

  <R> Stream<R> flatMap(FlatMapper<? super T, R> mapper);

  T reduce(T base, BinaryOperator<T> op);

  <U> U fold(Factory<U> seedFactory, Combiner<U, T, U> reducer,
      BinaryOperator<U> combiner);

  boolean anyMatch(Predicate<? super T> predicate);

  boolean allMatch(Predicate<? super T> predicate);

  boolean noneMatch(Predicate<? super T> predicate);
}

// End SequentialStreamOps.java
