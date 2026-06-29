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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.SetOp;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.SetOp},
 * including {@link org.apache.calcite.rel.core.Minus},
 * {@link org.apache.calcite.rel.core.Union} and
 * {@link org.apache.calcite.rel.core.Intersect}.
 */
public class SetOpNode implements Node {
  private final List<Source> sources;
  private final Sink sink;
  private final SetOp setOp;

  public SetOpNode(Compiler compiler, SetOp setOp) {
    final int arity = setOp.getInputs().size();
    checkArgument(arity >= 2, "invalid set op arity %s", arity);
    final ImmutableList.Builder<Source> sources = ImmutableList.builder();
    for (int i = 0; i < arity; i++) {
      sources.add(compiler.source(setOp, i));
    }
    this.sources = sources.build();
    assert this.sources.size() == arity;
    sink = compiler.sink(setOp);
    this.setOp = setOp;
  }

  @Override public void close() {
    for (Source source : sources) {
      source.close();
    }
  }

  @Override public void run() throws InterruptedException {
    switch (setOp.kind) {
    case UNION:
      union();
      break;
    case INTERSECT:
      intersect();
      break;
    case EXCEPT:
      minus();
      break;
    default:
      break;
    }
  }

  /** Evaluates UNION [ALL]. Does not need to buffer: sends each row to the
   * output as it arrives, eliminating duplicates on the fly unless ALL. */
  private void union() throws InterruptedException {
    if (setOp.all) {
      for (Source source : sources) {
        Row row;
        while ((row = source.receive()) != null) {
          sink.send(row);
        }
      }
    } else {
      final Set<Row> seen = new HashSet<>();
      for (Source source : sources) {
        Row row;
        while ((row = source.receive()) != null) {
          if (seen.add(row)) {
            sink.send(row);
          }
        }
      }
    }
  }

  /** Evaluates INTERSECT [ALL] by retaining, for each successive input, the
   * rows it has in common with the result so far. The minimum multiplicity
   * across all inputs survives. */
  private void intersect() throws InterruptedException {
    if (setOp.all) {
      // INTERSECT ALL: count each value's occurrences in every input and emit
      // it the minimum number of times. 'min' holds the smallest count seen
      // across the inputs processed so far; 'current' counts the occurrences
      // in the input being processed.
      final Map<Row, CountPair> counts = new HashMap<>();
      Row row;
      final Source first = sources.get(0);
      while ((row = first.receive()) != null) {
        counts.computeIfAbsent(row, k -> new CountPair()).min++;
      }
      final int last = sources.size() - 1;
      for (int i = 1; i < last; i++) {
        final Source source = sources.get(i);
        while ((row = source.receive()) != null) {
          final CountPair pair = counts.get(row);
          if (pair != null) {
            pair.current++;
          }
        }
        // Reduce the running minimum, dropping values absent from this input.
        counts.values().removeIf(pair -> {
          if (pair.current == 0) {
            return true;
          }
          pair.min = Math.min(pair.min, pair.current);
          pair.current = 0;
          return false;
        });
      }
      // Last input: count occurrences and emit each value that occurs in it
      // min(min, current) times.
      final Source source = sources.get(last);
      while ((row = source.receive()) != null) {
        final CountPair pair = counts.get(row);
        if (pair != null) {
          pair.current++;
        }
      }
      for (Map.Entry<Row, CountPair> entry : counts.entrySet()) {
        final CountPair pair = entry.getValue();
        if (pair.current > 0) {
          for (int n = Math.min(pair.min, pair.current); n > 0; n--) {
            sink.send(entry.getKey());
          }
        }
      }
    } else {
      // INTERSECT DISTINCT: from the running result, keep the rows present in
      // each successive input. Inputs after the first are streamed, so only
      // the result set (which only shrinks) is held in memory.
      Set<Row> result = read(sources.get(0));
      for (int i = 1; i < sources.size(); i++) {
        final Source source = sources.get(i);
        final Set<Row> next = new HashSet<>();
        Row row;
        while ((row = source.receive()) != null) {
          if (result.contains(row)) {
            next.add(row);
          }
        }
        result = next;
      }
      for (Row r : result) {
        sink.send(r);
      }
    }
  }

  /** Evaluates EXCEPT [ALL]. */
  private void minus() throws InterruptedException {
    if (setOp.all) {
      // EXCEPT ALL: count occurrences of each value in a map. A row from the
      // first input increments its value's count; a row from a later input
      // decrements it, and a value whose count reaches zero is removed. After
      // all inputs have been read, emit each surviving value as many times as
      // its remaining count.
      final Map<Row, Count> counts = new HashMap<>();
      Row row;
      final Source first = sources.get(0);
      while ((row = first.receive()) != null) {
        counts.computeIfAbsent(row, k -> new Count()).i++;
      }
      for (int i = 1; i < sources.size(); i++) {
        final Source source = sources.get(i);
        while ((row = source.receive()) != null) {
          final Count count = counts.get(row);
          if (count != null && --count.i == 0) {
            counts.remove(row);
          }
        }
      }
      for (Map.Entry<Row, Count> entry : counts.entrySet()) {
        for (int n = entry.getValue().i; n > 0; n--) {
          sink.send(entry.getKey());
        }
      }
    } else {
      // EXCEPT DISTINCT: remove from the running result every row that occurs
      // in a later input. Later inputs are streamed, so only the result set is
      // held in memory.
      final Set<Row> result = read(sources.get(0));
      for (int i = 1; i < sources.size(); i++) {
        final Source source = sources.get(i);
        Row row;
        while ((row = source.receive()) != null) {
          result.remove(row);
        }
      }
      for (Row r : result) {
        sink.send(r);
      }
    }
  }

  /** Reads a single input into a set, eliminating duplicates. */
  private static Set<Row> read(Source source) {
    final Set<Row> rows = new HashSet<>();
    Row row;
    while ((row = source.receive()) != null) {
      rows.add(row);
    }
    return rows;
  }

  /** Mutable count of the occurrences of a row, used by {@link #minus()}. */
  private static class Count {
    int i;
  }

  /** Mutable pair of counts used by {@link #intersect()}: the minimum
   * multiplicity of a row across the inputs seen so far, and its count in the
   * input currently being processed. */
  private static class CountPair {
    int min;
    int current;
  }
}
