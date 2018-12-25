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
package org.apache.calcite.runtime;

import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Utilities for processing {@link org.apache.calcite.linq4j.Enumerable}
 * collections.
 *
 * <p>This class is a place to put things not yet added to linq4j.
 * Methods are subject to removal without notice.
 */
public class Enumerables {

  private Enumerables() {}

  /** Converts an enumerable over singleton arrays into the enumerable of their
   * first elements. */
  public static <E> Enumerable<E> slice0(Enumerable<E[]> enumerable) {
    //noinspection unchecked
    return enumerable.select(elements -> elements[0]);
  }

  /** Converts an {@link Enumerable} over object arrays into an
   * {@link Enumerable} over {@link Row} objects. */
  public static Enumerable<Row> toRow(final Enumerable<Object[]> enumerable) {
    return enumerable.select((Function1<Object[], Row>) Row::asCopy);
  }

  /** Converts a supplier of an {@link Enumerable} over object arrays into a
   * supplier of an {@link Enumerable} over {@link Row} objects. */
  public static Supplier<Enumerable<Row>> toRow(
      final Supplier<Enumerable<Object[]>> supplier) {
    return () -> toRow(supplier.get());
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static com.google.common.base.Supplier<Enumerable<Row>> toRow(
      final com.google.common.base.Supplier<Enumerable<Object[]>> supplier) {
    return () -> toRow(supplier.get());
  }

  public static <E, TKey, TResult> Enumerable<TResult> match(
      Enumerable<E> enumerable,
      final Function1<E, TKey> keySelector,
      Matcher<E> matcher,
      Emitter<E, TResult> emitter, int history, int future) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          final Enumerator<E> inputEnumerator = enumerable.enumerator();

          // State of each partition.
          final Map<TKey, Matcher.PartitionState<E>> partitionStates =
              new HashMap<>();

          int inputRow = -1;

          final Deque<TResult> emitRows = new ArrayDeque<>();

          /** Current result row. Null if no row is ready. */
          TResult resultRow;

          /** Match counter is 1 based in Oracle */
          final AtomicInteger matchCounter = new AtomicInteger(1);

          public TResult current() {
            Objects.requireNonNull(resultRow);
            return resultRow;
          }

          public boolean moveNext() {
            for (;;) {
              resultRow = emitRows.pollFirst();
              if (resultRow != null) {
                return true;
              }
              // No rows are currently read to emit. Read the next input row,
              // see whether it completes a match (or matches), and if so, add
              // the resulting rows to the buffer.
              if (!inputEnumerator.moveNext()) {
                return false;
              }
              ++inputRow;
              final E row = inputEnumerator.current();
              final TKey key = keySelector.apply(row);
              final Matcher.PartitionState<E> partitionState =
                  partitionStates.computeIfAbsent(key,
                      k -> matcher.createPartitionState(history, future));

              partitionState.getMemoryFactory().add(row);


              matcher.matchOne(partitionState.getRows(), partitionState,
                  // TODO 26.12.18 jf: add row states (whatever this is?)
                  list -> emitter.emit(list, null, null,
                      matchCounter.getAndIncrement(), emitRows::add));
/*
              recentRows.add(e);
              int earliestRetainedRow = Integer.MAX_VALUE;
              for (int i = 0; i < partitionState.incompleteMatches.size(); i++) {
                MatchState match = partitionState.incompleteMatches.get(i);
                earliestRetainedRow = Math.min(earliestRetainedRow, match.firstRow);
                final int state = automaton.nextState(match.state, e);
                switch (state) {
                case Automaton.ACCEPT:
                  final List<E> matchedRows =
                      recentRows.subList(0, 0); // TODO:
                  final List<Integer> rowStates = ImmutableList.of(); // TODO:
                  emitRows.addAll(
                      emitter.emit(matchedRows, rowStates,
                          partitionState.matchCount++));
                  // fall through
                case Automaton.FAIL:
                  partitionState.incompleteMatches.remove(i--);
                  break;
                default:
                  match.state = state;
                }
              }
              // Try to start a match based on the current row
              final int state = automaton.nextState(Automaton.START_STATE, e);
              switch (state) {
              case Automaton.ACCEPT:
                final List<E> matchedRows = ImmutableList.of(e);
                final List<Integer> rowStates = ImmutableList.of(state);
                emitRows.addAll(
                    emitter.emit(matchedRows, rowStates,
                        partitionState.matchCount++));
                // fall through
              case Automaton.FAIL:
                // since it immediately succeeded or failed, don't add
                // it to the queue
                break;
              default:
                partitionState.incompleteMatches.add(
                    new MatchState(inputRow, state));
              }
*/
            }
          }

          public void reset() {
            throw new UnsupportedOperationException();
          }

          public void close() {
            inputEnumerator.close();
          }
        };
      }
    };
  }

  /** Given a match (a list of rows, and their states) produces a list
   * of rows to be output.
   *
   * @param <E> element type
   * @param <TResult> result type */
  public interface Emitter<E, TResult> {
    void emit(List<E> rows, List<Integer> rowStates, List<String> rowSymbols, int match,
        Consumer<TResult> consumer);
  }
}

// End Enumerables.java
