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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for bounded expression-lineage expansion. */
class RelMdExpressionLineageTest {
  private final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());

  @ParameterizedTest
  @CsvSource({"1, 1", "2, 2", "2, 4", "4, 4", "8, 4", "16, 4", "90, 4", "2, 16"})
  void testGeneratedPrograms(int branches, int columns) throws Exception {
    for (boolean window : new boolean[] {false, true}) {
      for (boolean calc : new boolean[] {false, true}) {
        RelNode rel = ExpressionLineageProgram.rel(branches, columns, window);
        if (calc) {
          HepPlanner planner = new HepPlanner(new HepProgramBuilder()
              .addRuleInstance(CoreRules.PROJECT_TO_CALC).build());
          planner.setRoot(rel);
          rel = planner.findBestExp();
          assertTrue(rel instanceof Calc, "Expected the Calc metadata path");
        }
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        RexNode result = RexInputRef.of(columns, rel.getRowType().getFieldList());
        Set<RexNode> lineage = mq.getExpressionLineage(rel, result);
        if (Math.pow(branches, columns) > 10_000) {
          // No truncated subset may escape as complete metadata.
          assertNull(lineage);
        } else {
          assertNotNull(lineage);
          assertEquals((int) Math.pow(branches, columns), lineage.size());
        }
        // An expensive result must not poison other columns or later queries.
        Set<RexNode> passthrough = mq.getExpressionLineage(rel,
            RexInputRef.of(0, rel.getRowType().getFieldList()));
        assertNotNull(passthrough);
        assertEquals(branches, passthrough.size());
        assertEquals(lineage, mq.getExpressionLineage(rel, result));
        rel.getCluster().invalidateMetadataQuery();
        assertEquals(lineage, rel.getCluster().getMetadataQuery()
            .getExpressionLineage(rel, result));
      }
    }
  }

  @Test void testBudgetBoundaryAndMappingUnchanged() {
    RexInputRef a = ref(0);
    RexInputRef b = ref(1);
    RexNode expression = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a, b);
    Map<RexInputRef, Set<RexNode>> mapping = mapping(a, b);
    Map<RexInputRef, Set<RexNode>> original = new LinkedHashMap<>(mapping);
    Set<RexNode> unlimited = expand(expression, mapping, -1);
    assertNotNull(unlimited);
    assertEquals(4, unlimited.size());
    assertEquals(unlimited, expand(expression, mapping, 4));
    assertNull(expand(expression, mapping, 3));
    assertEquals(original, mapping);
    assertEquals(unlimited, expand(expression, mapping, 5));
  }

  @Test void testGeneratedProgramExceedsBudget() throws Exception {
    RelNode rel = ExpressionLineageProgram.rel(16, 4, true);
    Set<RexNode> lineage = rel.getCluster().getMetadataQuery().getExpressionLineage(rel,
        RexInputRef.of(4, rel.getRowType().getFieldList()));
    assertTrue(lineage == null, () -> "Expected unknown lineage, not "
        + (lineage == null ? "null" : lineage.size()) + " candidates");
  }

  @Test void testBudgetCountsAttemptsBeforeDeduplication() {
    RexInputRef a = ref(0);
    RexInputRef b = ref(1);
    RexNode expression = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, a, b);
    Map<RexInputRef, Set<RexNode>> mapping = mapping(a, b);
    // Commutative normalization merges 1 + 10 and 10 + 1, but constructing
    // both still takes work. Budgeting the result Set size would miss this.
    Set<RexNode> unlimited = expand(expression, mapping, -1);
    assertNotNull(unlimited);
    assertEquals(3, unlimited.size());
    assertEquals(unlimited, expand(expression, mapping, 4));
    assertNull(expand(expression, mapping, 3));
  }

  @Test void testRepeatedAndUnusedReferences() {
    RexInputRef a = ref(0);
    RexInputRef b = ref(1);
    RexNode expression = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a, a);
    Map<RexInputRef, Set<RexNode>> mapping = mapping(a, b);
    // Only a is referenced: repeated occurrences share a substitution, and
    // unused b must not contribute a dimension to the product.
    assertEquals(expand(expression, mapping, -1), expand(expression, mapping, 2));
    assertNull(expand(expression, mapping, 1));
  }

  @Test void testZeroBudgetAndConstants() {
    Map<RexInputRef, Set<RexNode>> mapping = mapping(ref(0), ref(1));
    assertNull(expand(ref(0), mapping, 0));
    RexNode one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    assertEquals(ImmutableSet.of(one), expand(one, mapping, 0));
    assertThrows(IllegalArgumentException.class, () -> expand(one, mapping, -2));
  }

  @Test void testProductDoesNotOverflowOrEnumerate() {
    RexInputRef a = ref(0);
    RexInputRef b = ref(1);
    RexNode expression = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a, b);
    Set<RexNode> many = new AbstractSet<RexNode>() {
      @Override public int size() {
        return 65_536;
      }

      @Override public Iterator<RexNode> iterator() {
        throw new AssertionError("Over-budget candidates must not be enumerated");
      }
    };
    Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    mapping.put(a, many);
    mapping.put(b, many);
    // 65,536 squared wraps to zero in int arithmetic.
    assertNull(expand(expression, mapping, Integer.MAX_VALUE));
    assertEquals(2, mapping.size());
  }

  @Test void testGeneratorRejectsInvalidDimensions() {
    assertThrows(IllegalArgumentException.class, () -> ExpressionLineageProgram.sql(0, 4, true));
    assertThrows(IllegalArgumentException.class, () -> ExpressionLineageProgram.sql(2, 0, false));
  }

  @Test void testAllPredicatesUsesTheSameBudget() throws Exception {
    for (int branches : new int[] {2, 16}) {
      RelNode rel = ExpressionLineageProgram.rel(branches, 4, false);
      RexBuilder builder = rel.getCluster().getRexBuilder();
      RexNode expression = RexInputRef.of(0, rel.getRowType().getFieldList());
      for (int i = 1; i < 4; i++) {
        expression = builder.makeCall(SqlStdOperatorTable.MINUS, expression,
            RexInputRef.of(i, rel.getRowType().getFieldList()));
      }
      RelNode filter = LogicalFilter.create(rel, builder.makeCall(SqlStdOperatorTable.GREATER_THAN,
          expression, builder.makeExactLiteral(BigDecimal.ZERO)));
      RelOptPredicateList predicates = filter.getCluster().getMetadataQuery()
          .getAllPredicates(filter);
      if (branches == 2) {
        assertNotNull(predicates);
        assertEquals(16, predicates.pulledUpPredicates.size());
      } else {
        assertNull(predicates);
      }
    }
  }

  @Test void testGeneratedMappingsRespectBudget() {
    Random random = new Random(0);
    for (int trial = 0; trial < 100; trial++) {
      int columns = 1 + random.nextInt(4);
      Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
      RexNode expression = ref(0);
      int product = 1;
      for (int column = 0; column < columns; column++) {
        int count = 1 + random.nextInt(4);
        ImmutableSet.Builder<RexNode> values = ImmutableSet.builder();
        for (int value = 0; value < count; value++) {
          values.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(value)));
        }
        mapping.put(ref(column), values.build());
        product *= count;
        if (column > 0) {
          expression = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, expression, ref(column));
        }
      }
      Map<RexInputRef, Set<RexNode>> original = new LinkedHashMap<>(mapping);
      assertEquals(expand(expression, mapping, -1), expand(expression, mapping, product));
      assertNull(expand(expression, mapping, product - 1));
      assertEquals(original, mapping);
    }
  }

  private RexInputRef ref(int index) {
    return rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
        index);
  }

  private Map<RexInputRef, Set<RexNode>> mapping(RexInputRef a, RexInputRef b) {
    Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    Set<RexNode> replacements = ImmutableSet.of(
        rexBuilder.makeExactLiteral(BigDecimal.ONE), rexBuilder.makeExactLiteral(BigDecimal.TEN));
    mapping.put(a, replacements);
    mapping.put(b, replacements);
    return mapping;
  }

  private @Nullable Set<RexNode> expand(RexNode expression, Map<RexInputRef, Set<RexNode>> mapping,
      int budget) {
    return RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, expression, mapping,
        budget);
  }
}
