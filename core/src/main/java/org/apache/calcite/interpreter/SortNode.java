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

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Sort}.
 */
public class SortNode extends AbstractSingleNode<Sort> {
  private final @Nullable Scalar fetchScalar;
  private final @Nullable Context fetchContext;

  public SortNode(Compiler compiler, Sort rel) {
    super(compiler, rel);
    if (rel.fetch != null && !(rel.fetch instanceof RexLiteral)) {
      this.fetchScalar = compiler.compile(ImmutableList.of(rel.fetch), null);
      this.fetchContext = compiler.createContext();
    } else {
      this.fetchScalar = null;
      this.fetchContext = null;
    }
  }

  private static int getValueAsInt(RexNode node) {
    return requireNonNull(((RexLiteral) node).getValueAs(Integer.class),
        () -> "getValueAs(Integer.class) for " + node);
  }

  private @Nullable BigDecimal getFetch() {
    if (rel.fetch == null) {
      return null;
    }
    final @Nullable Number value;
    if (rel.fetch instanceof RexLiteral) {
      value = ((RexLiteral) rel.fetch).getValueAs(Number.class);
    } else {
      final Object result =
          requireNonNull(fetchScalar, "fetchScalar")
              .execute(requireNonNull(fetchContext, "fetchContext"));
      if (result != null && !(result instanceof Number)) {
        throw new IllegalArgumentException("FETCH value is not numeric: " + result);
      }
      value = (Number) result;
    }
    return EnumerableLimit.toFetchValue(value);
  }

  @Override public void run() throws InterruptedException {
    final int offset =
        rel.offset == null
            ? 0
            : getValueAsInt(rel.offset);
    final @Nullable BigDecimal fetch = getFetch();
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
      if (fetch != null) {
        BigDecimal fetched = BigDecimal.ZERO;
        while (fetched.compareTo(fetch) < 0
            && (row = source.receive()) != null) {
          sink.send(row);
          fetched = fetched.add(BigDecimal.ONE);
        }
      } else {
        while ((row = source.receive()) != null) {
          sink.send(row);
        }
      }
    } else {
      // Build a sorted collection.
      final List<Row> list = new ArrayList<>();
      while ((row = source.receive()) != null) {
        list.add(row);
      }
      list.sort(comparator());
      final int available = Math.max(list.size() - offset, 0);
      final int end = fetch == null
          || fetch.compareTo(BigDecimal.valueOf(available)) >= 0
          ? list.size()
          : offset + fetch.intValueExact();
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
        Util.transform(rel.getCollation().getFieldCollations(),
            SortNode::comparator));
  }

  private static Comparator<Row> comparator(RelFieldCollation fieldCollation) {
    final int nullComparison = fieldCollation.nullDirection.nullComparison;
    final int x = fieldCollation.getFieldIndex();
    switch (fieldCollation.direction) {
    case ASCENDING:
      return (o1, o2) -> {
        final Comparable c1 = (Comparable) o1.getValues()[x];
        final Comparable c2 = (Comparable) o2.getValues()[x];
        return RelFieldCollation.compare(c1, c2, nullComparison);
      };
    default:
      return (o1, o2) -> {
        final Comparable c1 = (Comparable) o1.getValues()[x];
        final Comparable c2 = (Comparable) o2.getValues()[x];
        return RelFieldCollation.compare(c2, c1, -nullComparison);
      };
    }
  }
}
