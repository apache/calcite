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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.Arrays;

import static org.apache.calcite.test.Matchers.sortsAs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link org.apache.calcite.rel.metadata.RelMdPredicates} class. */
public class RelMdPredicatesTest {

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestPullUpFromJoin.csv", delimiter = ';')
  void testPullUpPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .filter(b.equals(b.field("ENAME"), b.literal("Victor")))
        .scan("DEPT")
        .filter(b.equals(b.field("DNAME"), b.literal("CSD")))
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.pulledUpPredicates, sortsAs(expectedPredicates));
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestLeftInferredFromJoin.csv", delimiter = ';')
  void testLeftInferredPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .scan("DEPT")
        .filter(b.greaterThan(b.field("DEPTNO"), b.literal(10)))
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.leftInferredPredicates, sortsAs(expectedPredicates));
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestRightInferredFromJoin.csv", delimiter = ';')
  void testRightInferredPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .filter(b.greaterThan(b.field("DEPTNO"), b.literal(10)))
        .scan("DEPT")
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.rightInferredPredicates, sortsAs(expectedPredicates));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6507">[CALCITE-6507]
   * Random functions are incorrectly considered deterministic</a>. */
  @Test void testRandomFunctionsAreNotConsideredConstant() {
    FrameworkConfig config = RelBuilderTest.config().build();
    for (SqlOperator randomOp : Arrays.asList(SqlStdOperatorTable.RAND,
        SqlLibraryOperators.RANDOM, SqlStdOperatorTable.RAND_INTEGER)) {
      RelBuilder b = RelBuilder.create(config);
      RelNode rel = b
          .scan("EMP")
          .project(b.field(0), b.call(randomOp))
          .sort(1)
          .build();
      RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      RelOptPredicateList list = mq.getPulledUpPredicates(rel);
      assertTrue(list.constantMap.isEmpty(),
          "Operator " + randomOp + " considered constant: " + list.constantMap);
    }
  }

}
