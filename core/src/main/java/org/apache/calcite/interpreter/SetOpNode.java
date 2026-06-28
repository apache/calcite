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
import org.apache.calcite.sql.SqlKind;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    final ImmutableList.Builder<Source> sources = ImmutableList.builder();
    for (int i = 0; i < setOp.getInputs().size(); i++) {
      sources.add(compiler.source(setOp, i));
    }
    this.sources = sources.build();
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
    final List<Collection<Row>> inputs = readInputs();
    Collection<Row> result = inputs.get(0);
    for (int i = 1; i < inputs.size(); i++) {
      final Collection<Row> rows = inputs.get(i);
      final Collection<Row> intersection =
          setOp.all ? HashMultiset.create() : new HashSet<>();
      for (Row leftRow : result) {
        if (rows.remove(leftRow)) {
          intersection.add(leftRow);
        }
      }
      result = intersection;
    }
    for (Row r : result) {
      sink.send(r);
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
      final Map<Row, Integer> counts = new HashMap<>();
      Row row;
      final Source first = sources.get(0);
      while ((row = first.receive()) != null) {
        counts.merge(row, 1, Integer::sum);
      }
      for (int i = 1; i < sources.size(); i++) {
        final Source source = sources.get(i);
        while ((row = source.receive()) != null) {
          final Integer count = counts.get(row);
          if (count != null) {
            if (count == 1) {
              counts.remove(row);
            } else {
              counts.put(row, count - 1);
            }
          }
        }
      }
      for (Map.Entry<Row, Integer> entry : counts.entrySet()) {
        for (int n = entry.getValue(); n > 0; n--) {
          sink.send(entry.getKey());
        }
      }
    } else {
      // EXCEPT: remove each value that occurs in a later input.
      final List<Collection<Row>> inputs = readInputs();
      final Collection<Row> result = inputs.get(0);
      for (int i = 1; i < inputs.size(); i++) {
        for (Row rightRow : inputs.get(i)) {
          result.remove(rightRow);
        }
      }
      for (Row r : result) {
        sink.send(r);
      }
    }
  }

  /** Reads every input into a collection. A HashMultiset preserves duplicate
   * counts for the ALL variants; a HashSet eliminates them otherwise. */
  private List<Collection<Row>> readInputs() throws InterruptedException {
    final List<Collection<Row>> inputs = new ArrayList<>();
    for (Source source : sources) {
      final Collection<Row> rows =
          setOp.all ? HashMultiset.create() : new HashSet<>();
      Row row;
      while ((row = source.receive()) != null) {
        rows.add(row);
      }
      inputs.add(rows);
    }
    return inputs;
  }
}
