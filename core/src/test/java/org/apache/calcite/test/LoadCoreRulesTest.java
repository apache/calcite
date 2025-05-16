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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link QuidemTest} loading {@link CoreRules}.
 */
class LoadCoreRulesTest {
  //~ Methods ----------------------------------------------------------------

  @Test void testLoadAllRules() {
    Field[] fields = CoreRules.class.getDeclaredFields();
    for (Field field : fields) {
      String fieldName = field.getName();
      // Skip JaCoCo-injected fields (e.g. $jacocoData)
      if (fieldName.contains("jacocoData")) {
        continue;
      }
      QuidemTest.getCoreRule(fieldName);
    }
  }

  @Test void testLoadNonExistRule() {
    assertThrows(RuntimeException.class,
        () -> QuidemTest.getCoreRule("xxx"));
  }

  @Test void testLoadSpecifyRule() {
    RelOptRule rule1 =
        QuidemTest.getCoreRule("EXPAND_FILTER_DISJUNCTION_LOCAL");
    RelOptRule expected1 = CoreRules.EXPAND_FILTER_DISJUNCTION_LOCAL;
    assertEquals(rule1, expected1);

    RelOptRule rule2 =
        QuidemTest.getCoreRule("EXPAND_JOIN_DISJUNCTION_LOCAL");
    RelOptRule expected2 = CoreRules.EXPAND_JOIN_DISJUNCTION_LOCAL;
    assertEquals(rule2, expected2);

    // Same rule type with different Configs should not be equal.
    assertNotEquals(rule1, rule2);
  }

  @Test void testLoadIncludeSubclassesRule() {
    RelOptRule rule1 =
        QuidemTest.getCoreRule("FILTER_REDUCE_EXPRESSIONS");
    RelOptRule expected1 = CoreRules.FILTER_REDUCE_EXPRESSIONS;
    assertEquals(rule1, expected1);

    RelOptRule rule2 =
        QuidemTest.getCoreRule("PROJECT_REDUCE_EXPRESSIONS");
    RelOptRule expected2 = CoreRules.PROJECT_REDUCE_EXPRESSIONS;
    assertEquals(rule2, expected2);
  }
}
