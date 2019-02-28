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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Function2;

/**
 * Performs accumulation against a pre-collected list of input sources,
 * used with {@link LazyAggregateLambdaFactory}.
 *
 * @param <TAccumulate> Type of the accumulator
 * @param <TSource>     Type of the enumerable input source
 */
public class BasicLazyAccumulator<TAccumulate, TSource>
    implements LazyAggregateLambdaFactory.LazyAccumulator<TAccumulate, TSource> {

  private final Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder;

  public BasicLazyAccumulator(Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder) {
    this.accumulatorAdder = accumulatorAdder;
  }

  @Override public void accumulate(Iterable<TSource> sourceIterable, TAccumulate accumulator) {
    TAccumulate accumulator1 = accumulator;
    for (TSource tSource : sourceIterable) {
      accumulator1 = accumulatorAdder.apply(accumulator1, tSource);
    }
  }
}

// End BasicLazyAccumulator.java
