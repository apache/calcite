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
package org.apache.calcite.rex;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Analyzes an expression, figures out what are the unbound variables,
 * assigns a variety of values to each unbound variable, and evaluates
 * the expression. */
public class RexAnalyzer {
  public final RexNode e;
  public final List<RexNode> variables;
  public final int unsupportedCount;

  /** Creates a RexAnalyzer. */
  public RexAnalyzer(RexNode e, RelOptPredicateList predicates) {
    this.e = e;
    final VariableCollector variableCollector = new VariableCollector();
    e.accept(variableCollector);
    predicates.pulledUpPredicates.forEach(p -> p.accept(variableCollector));
    variables = ImmutableList.copyOf(variableCollector.builder);
    unsupportedCount = variableCollector.unsupportedCount;
  }

  /** Generates a map of variables and lists of values that could be assigned
   * to them. */
  public Iterable<Map<RexNode, Comparable>> assignments() {
    final List<List<Comparable>> generators =
        variables.stream().map(RexAnalyzer::getComparables)
            .collect(Util.toImmutableList());
    final Iterable<List<Comparable>> product = Linq4j.product(generators);
    //noinspection StaticPseudoFunctionalStyleMethod
    return Iterables.transform(product,
        values -> ImmutableMap.copyOf(Pair.zip(variables, values)));
  }

  private static List<Comparable> getComparables(RexNode variable) {
    final ImmutableList.Builder<Comparable> values = ImmutableList.builder();
    switch (variable.getType().getSqlTypeName()) {
    case BOOLEAN:
      values.add(true);
      values.add(false);
      break;
    case INTEGER:
      values.add(BigDecimal.valueOf(-1L));
      values.add(BigDecimal.valueOf(0L));
      values.add(BigDecimal.valueOf(1L));
      values.add(BigDecimal.valueOf(1_000_000L));
      break;
    case VARCHAR:
      values.add(new NlsString("", null, null));
      values.add(new NlsString("hello", null, null));
      break;
    case TIMESTAMP:
      values.add(0L); // 1970-01-01 00:00:00
      break;
    case DATE:
      values.add(0); // 1970-01-01
      values.add(365); // 1971-01-01
      values.add(-365); // 1969-01-01
      break;
    case TIME:
      values.add(0); // 00:00:00.000
      values.add(86_399_000); // 23:59:59.000
      break;
    default:
      throw new AssertionError("don't know values for " + variable
          + " of type " + variable.getType());
    }
    if (variable.getType().isNullable()) {
      values.add(NullSentinel.INSTANCE);
    }
    return values.build();
  }

  /** Collects the variables (or other bindable sites) in an expression, and
   * counts features (such as CAST) that {@link RexInterpreter} cannot
   * handle. */
  private static class VariableCollector extends RexVisitorImpl<Void> {
    private final Set<RexNode> builder = new LinkedHashSet<>();
    private int unsupportedCount = 0;

    VariableCollector() {
      super(true);
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      builder.add(inputRef);
      return super.visitInputRef(inputRef);
    }

    @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexDynamicParam) {
        builder.add(fieldAccess);
        return null;
      } else {
        return super.visitFieldAccess(fieldAccess);
      }
    }

    @Override public Void visitCall(RexCall call) {
      switch (call.getKind()) {
      case CAST:
        ++unsupportedCount;
        return null;
      default:
        return super.visitCall(call);
      }
    }
  }
}

// End RexAnalyzer.java
