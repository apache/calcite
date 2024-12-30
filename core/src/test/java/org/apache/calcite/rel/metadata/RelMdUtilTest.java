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

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.test.RelMetadataFixture;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.rel.metadata.RelMdUtil.numDistinctVals;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test cases for {@link RelMdUtil}.
 */
public class RelMdUtilTest {

  /** Creates a fixture. */
  protected RelMetadataFixture fixture() {
    return RelMetadataFixture.DEFAULT;
  }

  final RelMetadataFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  private static final double EPSILON = 1e-5;

  @Test void testNumDistinctVals() {
    // the first element must be distinct, the second one has half chance of being distinct
    assertThat(numDistinctVals(2.0, 2.0), closeTo(1.5, EPSILON));

    // when no selection is made, we get no distinct value
    double domainSize = 100;
    assertThat(numDistinctVals(domainSize, 0.0), closeTo(0, EPSILON));

    // when we perform one selection, we always have 1 distinct value,
    // regardless of the domain size
    for (double dSize = 1; dSize < 100; dSize += 1) {
      assertThat(numDistinctVals(dSize, 1.0), closeTo(1.0, EPSILON));
    }

    // when we select n objects from a set with n values
    // we get no more than n distinct values
    for (double dSize = 1; dSize < 100; dSize += 1) {
      assertThat(numDistinctVals(dSize, dSize), lessThanOrEqualTo(dSize));
    }

    // when the number of selections is large enough
    // we get all distinct values, w.h.p.
    assertThat(numDistinctVals(domainSize, domainSize * 100),
        closeTo(domainSize, EPSILON));

    assertThat(numDistinctVals(100.0, 2.0), closeTo(1.99, EPSILON));
    assertThat(numDistinctVals(1000.0, 2.0), closeTo(1.999, EPSILON));
    assertThat(numDistinctVals(10000.0, 2.0), closeTo(1.9999, EPSILON));
  }

  @Test void testNumDistinctValsWithLargeDomain() {
    double[] domainSizes = {1e18, 1e20};
    double[] numSels = {1e2, 1e4, 1e6, 1e8, 1e10, 1e12};
    double res;
    for (double domainSize : domainSizes) {
      for (double numSel : numSels) {
        res = numDistinctVals(domainSize, numSel);
        assertThat(res, not(0));
        // due to the possible duplicate selections, the distinct values
        // must be smaller than or equal to the number of selections
        assertThat(res, lessThanOrEqualTo(numSel));
      }
      res = numDistinctVals(domainSize, 1.0);
      assertThat(res, closeTo(1.0, EPSILON));

      res = numDistinctVals(domainSize, 2.0);
      assertThat(res, closeTo(2.0, EPSILON));
    }
  }

  @Test void testDynamicParameterInLimitOffset() {
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      RelMetadataQuery mq = cluster.getMetadataQuery();
      RelNode rel = sql("select * from emp limit ? offset ?").toRel();
      Sort sort = (Sort) rel;
      assertFalse(
          RelMdUtil.checkInputForCollationAndLimit(mq, sort.getInput(),
              RelCollations.EMPTY, sort.offset, sort.fetch));
      return null;
    });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6749">[CALCITE-6749]
   * RelMdUtil#setAggChildKeys may return an incorrect result</a>. */
  @Test void testSetAggChildKeys() {
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      RelNode rel = sql("select d.deptno, count(distinct e.job)\n"
          + "from sales.emp e\n"
          + "right outer join sales.dept d on e.deptno = d.deptno\n"
          + "group by d.deptno")
          .withRelTransform(relNode -> {
            final HepProgramBuilder builder = HepProgram.builder();
            builder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
            final HepPlanner prePlanner = new HepPlanner(builder.build());
            prePlanner.setRoot(relNode);
            return prePlanner.findBestExp();
          }).toRel();
      final Aggregate agg = (Aggregate) rel;
      // We should get an Aggregate(group=[{9}], EXPR$1=[COUNT(DISTINCT $2)])
      assertEquals(1, agg.getGroupCount());
      assertEquals(9, agg.getGroupSet().asList().get(0));
      assertEquals(1, agg.getAggCallList().size());
      assertEquals(1, agg.getAggCallList().get(0).getArgList().size());
      assertEquals(2, agg.getAggCallList().get(0).getArgList().get(0));
      // The childKey corresponding to 0 (group key) must be 9
      final ImmutableBitSet.Builder builder1 = ImmutableBitSet.builder();
      RelMdUtil.setAggChildKeys(ImmutableBitSet.of(0), agg, builder1);
      assertEquals(ImmutableBitSet.of(9), builder1.build());
      // The childKey corresponding to 1 (count aggCall) must be 2
      final ImmutableBitSet.Builder builder2 = ImmutableBitSet.builder();
      RelMdUtil.setAggChildKeys(ImmutableBitSet.of(1), agg, builder2);
      assertEquals(ImmutableBitSet.of(2), builder2.build());
      return null;
    });
  }

}
