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

import org.apache.calcite.interpreter.Row.RowBuilder;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Interpreter node that implements an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateNode extends AbstractSingleNode<Aggregate> {
  private final List<Grouping> groups = Lists.newArrayList();
  private final ImmutableBitSet unionGroups;
  private final int outputRowLength;

  public AggregateNode(Interpreter interpreter, Aggregate rel) {
    super(interpreter, rel);

    ImmutableBitSet union = ImmutableBitSet.of();

    if (rel.getGroupSets() != null) {
      for (ImmutableBitSet group : rel.getGroupSets()) {
        union = union.union(group);
        groups.add(new Grouping(group));
      }
    }

    this.unionGroups = union;
    this.outputRowLength = unionGroups.cardinality()
        + (rel.indicator ? unionGroups.cardinality() : 0)
        + rel.getAggCallList().size();
  }

  public void run() throws InterruptedException {

    Row r;
    while ((r = source.receive()) != null) {
      for (Grouping group : groups) {
        group.send(r);
      }
    }

    for (Grouping group : groups) {
      group.end(sink);
    }
  }

  private AccumulatorList getNewAccumList() {
    AccumulatorList list = new AccumulatorList();
    for (AggregateCall call : rel.getAggCallList()) {
      list.add(getAccumulator(call));
    }
    return list;
  }

  private static Accumulator getAccumulator(final AggregateCall call) {
    String agg = call.getAggregation().getName();

    if (agg.equals("COUNT")) {
      return new Accumulator() {
        long cnt = 0;

        public void send(Row row) {
          boolean notNull = true;
          for (Integer i : call.getArgList()) {
            if (row.getObject(i) == null) {
              notNull = false;
              break;
            }
          }
          if (notNull) {
            cnt++;
          }
        }

        public Object end() {
          return cnt;
        }

      };
    } else {
      throw new UnsupportedOperationException(
          String.format("Aggregate doesn't currently support "
              + "the %s aggregate function.", agg));
    }

  }

  /**
   * Internal class to track groupings
   */
  private class Grouping {
    private ImmutableBitSet grouping;
    private Map<Row, AccumulatorList> accum = Maps.newHashMap();

    private Grouping(ImmutableBitSet grouping) {
      this.grouping = grouping;
    }

    public void send(Row row) {
      // TODO: fix the size of this row.
      RowBuilder builder = Row.newBuilder(grouping.cardinality());
      for (Integer i : grouping) {
        builder.set(i, row.getObject(i));
      }
      Row key = builder.build();

      if (!accum.containsKey(key)) {
        accum.put(key, getNewAccumList());
      }

      accum.get(key).send(row);
    }

    public void end(Sink sink) throws InterruptedException {
      for (Map.Entry<Row, AccumulatorList> e : accum.entrySet()) {
        final Row key = e.getKey();
        final AccumulatorList list = e.getValue();

        RowBuilder rb = Row.newBuilder(outputRowLength);
        int index = 0;
        for (Integer groupPos : unionGroups) {
          if (grouping.get(groupPos)) {
            rb.set(index, key.getObject(groupPos));
            if (rel.indicator) {
              rb.set(unionGroups.cardinality() + index, true);
            }
          }
          // need to set false when not part of grouping set.

          index++;
        }

        list.end(rb);

        sink.send(rb.build());
      }
    }
  }

  /**
   * A list of accumulators used during grouping.
   */
  private class AccumulatorList extends ArrayList<Accumulator> {
    public void send(Row row) {
      for (Accumulator a : this) {
        a.send(row);
      }
    }

    public void end(RowBuilder r) {
      for (int accIndex = 0, rowIndex = r.size() - size();
          rowIndex < r.size(); rowIndex++, accIndex++) {
        r.set(rowIndex, get(accIndex).end());
      }
    }
  }

  /**
   * Defines function implementation for
   * things like {@code count()} and {@code sum()}.
   */
  private interface Accumulator {
    void send(Row row);
    Object end();
  }
}

// End AggregateNode.java
