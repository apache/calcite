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

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

import java.util.List;

/**
 * Implementation of {@link AggregateLambdaFactory} that applies a sequence of
 * accumulator adders to input source.
 *
 * @param <TSource> Type of the enumerable input source
 * @param <TAccumulate> Type of the accumulator
 * @param <TResult> Type of the enumerable output result
 * @param <TKey> Type of the group-by key
 */
public class BasicAggregateLambdaFactory<TSource, TAccumulate, TResult, TKey>
    implements AggregateLambdaFactory<TSource, TAccumulate, TAccumulate, TResult, TKey> {

  private final Function0<TAccumulate> accumulatorInitializer;
  private final Function2<TAccumulate, TSource, TAccumulate> accumulatorAdderDecorator;

  public BasicAggregateLambdaFactory(
      Function0<TAccumulate> accumulatorInitializer,
      List<Function2<TAccumulate, TSource, TAccumulate>> accumulatorAdders) {
    this.accumulatorInitializer = accumulatorInitializer;
    this.accumulatorAdderDecorator = new AccumulatorAdderSeq(accumulatorAdders);
  }

  @Override public Function0<TAccumulate> accumulatorInitializer() {
    return accumulatorInitializer;
  }

  @Override public Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder() {
    return accumulatorAdderDecorator;
  }

  @Override public Function1<TAccumulate, TResult> singleGroupResultSelector(
      Function1<TAccumulate, TResult> resultSelector) {
    return resultSelector;
  }

  @Override public Function2<TKey, TAccumulate, TResult> resultSelector(
      Function2<TKey, TAccumulate, TResult> resultSelector) {
    return resultSelector;
  }

  /**
   * Decorator class of a sequence of accumulator adder functions.
   */
  private class AccumulatorAdderSeq
      implements Function2<TAccumulate, TSource, TAccumulate> {
    private final List<Function2<TAccumulate, TSource, TAccumulate>> accumulatorAdders;

    AccumulatorAdderSeq(
        List<Function2<TAccumulate, TSource, TAccumulate>> accumulatorAdders) {
      this.accumulatorAdders = accumulatorAdders;
    }

    @Override public TAccumulate apply(TAccumulate accumulator, TSource source) {
      TAccumulate result = accumulator;
      for (Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder
          : accumulatorAdders) {
        result = accumulatorAdder.apply(accumulator, source);
      }
      return result;
    }
  }
}

// End BasicAggregateLambdaFactory.java
