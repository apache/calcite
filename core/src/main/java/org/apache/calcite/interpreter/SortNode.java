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

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Sort}.
 */
public class SortNode extends AbstractSingleNode<Sort> {
  public SortNode(Interpreter interpreter, Sort rel) {
    super(interpreter, rel);
  }

  public void run() throws InterruptedException {
    final int offset =
        rel.offset == null
            ? 0
            : ((RexLiteral) rel.offset).getValueAs(Integer.class);
    final int fetch =
        rel.fetch == null
            ? -1
            : ((RexLiteral) rel.fetch).getValueAs(Integer.class);
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
        for (int i = 0; i < fetch && (row = source.receive()) != null; i++) {
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

  private Comparator<Row> comparator(RelFieldCollation fieldCollation) {
    final int nullComparison = fieldCollation.nullDirection.nullComparison;
    final int x = fieldCollation.getFieldIndex();
    switch (fieldCollation.direction) {
    case ASCENDING:
      return new Comparator<Row>() {
        public int compare(Row o1, Row o2) {
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          return RelFieldCollation.compare(c1, c2, nullComparison);
        }
      };
    default:
      return new Comparator<Row>() {
        public int compare(Row o1, Row o2) {
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          return RelFieldCollation.compare(c2, c1, -nullComparison);
        }
      };
    }
  }
}

// End SortNode.java
