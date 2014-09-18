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
package net.hydromatic.optiq.impl.interpreter;

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rex.RexLiteral;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.eigenbase.rel.SortRel}.
 */
public class SortNode implements Node {
  private final Source source;
  private final Sink sink;
  private final SortRel rel;

  public SortNode(Interpreter interpreter, SortRel rel) {
    this.rel = rel;
    this.source = interpreter.source(rel, 0);
    this.sink = interpreter.sink(rel);
  }

  public void run() throws InterruptedException {
    final int offset =
        rel.offset == null
            ? 0
            : (Integer) ((RexLiteral) rel.offset).getValue();
    final int fetch =
        rel.fetch == null
            ? -1
            : (Integer) ((RexLiteral) rel.fetch).getValue();
    // In pure limit mode. No sort required.
    Row row;
  loop:
    if (rel.getCollation().getFieldCollations().isEmpty()) {
      for (int i = 0; i < offset; i++) {
        row = source.receive();
        if (row == null) {
          break loop;
        }
      }
      if (fetch >= 0) {
        for (int i = 0; i < offset && (row = source.receive()) != null; i++) {
          sink.send(row);
        }
      } else {
        while ((row = source.receive()) != null) {
          sink.send(row);
        }
      }
    } else {
      // Build a sorted collection.
      final List<Row> list = Lists.newArrayList();
      while ((row = source.receive()) != null) {
        list.add(row);
      }
      Collections.sort(list, comparator());
      final int end = fetch < 0 || offset + fetch > list.size()
          ? list.size()
          : offset + fetch;
      for (int i = offset; i < end; i++) {
        sink.send(list.get(i));
      }
    }
    sink.end();
  }

  private Comparator<Row> comparator() {
    if (rel.getCollation().getFieldCollations().size() == 1) {
      return comparator(rel.getCollation().getFieldCollations().get(0));
    }
    return Ordering.compound(
        Iterables.transform(rel.getCollation().getFieldCollations(),
            new Function<RelFieldCollation, Comparator<? super Row>>() {
              public Comparator<? super Row> apply(RelFieldCollation input) {
                return comparator(input);
              }
            }));
  }

  private Comparator<Row> comparator(final RelFieldCollation fieldCollation) {
    switch (fieldCollation.direction) {
    case ASCENDING:
      return new Comparator<Row>() {
        final int x = fieldCollation.getFieldIndex();
        public int compare(Row o1, Row o2) {
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          //noinspection unchecked
          return c1.compareTo(c2);
        }
      };
    default:
      return new Comparator<Row>() {
        final int x = fieldCollation.getFieldIndex();
        public int compare(Row o1, Row o2) {
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          //noinspection unchecked
          return c2.compareTo(c1);
        }
      };
    }
  }
}

// End ScanNode.java
