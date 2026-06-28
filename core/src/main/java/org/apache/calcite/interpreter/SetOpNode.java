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
    if (setOp.kind == SqlKind.UNION) {
      // Union does not need to buffer: send each row to the output as it
      // arrives.
      if (setOp.all) {
        for (Source source : sources) {
          Row row;
          while ((row = source.receive()) != null) {
            sink.send(row);
          }
        }
      } else {
        // Eliminate duplicates on the fly.
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
      return;
    }

    if (setOp.kind == SqlKind.EXCEPT && setOp.all) {
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
      return;
    }

    // Intersect and (distinct) minus must read all inputs before producing
    // output. A HashMultiset preserves duplicate counts for INTERSECT ALL, a
    // HashSet eliminates them otherwise.
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

    Collection<Row> result = inputs.get(0);
    for (int i = 1; i < inputs.size(); i++) {
      final Collection<Row> rows = inputs.get(i);
      switch (setOp.kind) {
      case INTERSECT:
        // Keep a row for each occurrence present in both collections; this
        // yields the minimum multiplicity across all inputs.
        final Collection<Row> intersection =
            setOp.all ? HashMultiset.create() : new HashSet<>();
        for (Row leftRow : result) {
          if (rows.remove(leftRow)) {
            intersection.add(leftRow);
          }
        }
        result = intersection;
        break;
      case EXCEPT:
        // Remove one occurrence from the result for each occurrence in a
        // later input.
        for (Row rightRow : rows) {
          result.remove(rightRow);
        }
        break;
      default:
        break;
      }
    }
    for (Row r : result) {
      sink.send(r);
    }
  }
}
