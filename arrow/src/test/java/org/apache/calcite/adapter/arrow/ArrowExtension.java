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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.config.CalciteSystemProperty;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit5 extension to handle Arrow tests.
 *
 * <p>Tests will be skipped if the Gandiva library cannot be loaded on the given platform.
 */
class ArrowExtension implements ExecutionCondition {

  /**
   * Whether to run this test.
   *
   * <p>Enabled by default, unless explicitly disabled from command line
   * ({@code -Dcalcite.test.arrow=false}) or if Gandiva library, used to implement arrow
   * filtering/projection, cannot be loaded.
   *
   * @return {@code true} if the test is enabled and can run in the current environment,
   *         {@code false} otherwise
   */
  @Override public ConditionEvaluationResult evaluateExecutionCondition(
      final ExtensionContext context) {

    boolean enabled = CalciteSystemProperty.TEST_ARROW.value();
    try {
      Schema emptySchema = new Schema(new ArrayList<>(), null);
      List<ExpressionTree> expressions = new ArrayList<>();
      Projector.make(emptySchema, expressions);
    } catch (GandivaException e) {
      // this exception comes from using an empty expression,
      // but the JNI library was loaded properly
    } catch (UnsatisfiedLinkError e) {
      enabled = false;
    }

    if (enabled) {
      return ConditionEvaluationResult.enabled("Arrow tests enabled");
    } else {
      return ConditionEvaluationResult.disabled("Cassandra tests disabled");
    }
  }
}
