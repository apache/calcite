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

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.FetchOffsetRoundingPolicy;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Sort}.
 */
public class SortNode extends AbstractSingleNode<Sort> {
  private final @Nullable Scalar offsetScalar;
  private final @Nullable Context offsetContext;
  private final @Nullable Scalar fetchScalar;
  private final @Nullable Context fetchContext;
  private final FetchOffsetRoundingPolicy fetchOffsetRoundingPolicy;

  public SortNode(Compiler compiler, Sort rel) {
    super(compiler, rel);
    if (rel.offset != null && !(rel.offset instanceof RexLiteral)) {
      this.offsetScalar = compiler.compile(ImmutableList.of(rel.offset), null);
      this.offsetContext = compiler.createContext();
    } else {
      this.offsetScalar = null;
      this.offsetContext = null;
    }
    if (rel.fetch != null && !(rel.fetch instanceof RexLiteral)) {
      this.fetchScalar = compiler.compile(ImmutableList.of(rel.fetch), null);
      this.fetchContext = compiler.createContext();
    } else {
      this.fetchScalar = null;
      this.fetchContext = null;
    }
    final Object roundingPolicy = compiler.getDataContext()
        .get(EnumerableRelImplementor.FETCH_OFFSET_ROUNDING_POLICY);
    this.fetchOffsetRoundingPolicy =
        roundingPolicy instanceof FetchOffsetRoundingPolicy
            ? (FetchOffsetRoundingPolicy) roundingPolicy
            : FetchOffsetRoundingPolicy.NONE;
  }

  @Override public void run() throws InterruptedException {
    final BigDecimal offset = getOffset();
    final @Nullable BigDecimal fetch = getFetch();
    // In pure limit mode. No sort required.
    Row row;
  loop:
    if (rel.getCollation().getFieldCollations().isEmpty()) {
      BigDecimal skipped = BigDecimal.ZERO;
      while (skipped.compareTo(offset) < 0) {
        row = source.receive();
        if (row == null) {
          break loop;
        }
        skipped = skipped.add(BigDecimal.ONE);
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
      final int start = offset.compareTo(BigDecimal.valueOf(list.size())) >= 0
          ? list.size()
          : rowCount(offset);
      final int available = list.size() - start;
      final int end = fetch == null
          || fetch.compareTo(BigDecimal.valueOf(available)) >= 0
          ? list.size()
          : start + rowCount(fetch);
      for (int i = start; i < end; i++) {
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

  private @Nullable BigDecimal getFetch() {
    if (rel.fetch == null) {
      return null;
    }
    return getValue(rel.fetch, fetchScalar, fetchContext, "FETCH");
  }

  private BigDecimal getOffset() {
    if (rel.offset == null) {
      return BigDecimal.ZERO;
    }
    return getValue(rel.offset, offsetScalar, offsetContext, "OFFSET");
  }

  private BigDecimal getValue(RexNode node, @Nullable Scalar scalar,
      @Nullable Context context, String kind) {
    final @Nullable Object value;
    if (node instanceof RexLiteral) {
      value = RexLiteral.bigDecimalValue(node);
    } else {
      value =
          requireNonNull(scalar, () -> kind + " scalar")
              .execute(requireNonNull(context, () -> kind + " context"));
    }
    return EnumUtils.numberToBigDecimal(value, kind, fetchOffsetRoundingPolicy);
  }

  private static int rowCount(BigDecimal value) {
    return value.setScale(0, RoundingMode.CEILING).intValueExact();
  }
}
