/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.linq4j.expressions;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds context for evaluating expressions.
 */
class Evaluator {
  final List<ParameterExpression> parameters =
      new ArrayList<ParameterExpression>();
  final List<Object> values = new ArrayList<Object>();

  Evaluator() {
  }

  void push(ParameterExpression parameter, Object value) {
    parameters.add(parameter);
    values.add(value);
  }

  void pop(int n) {
    while (n > 0) {
      parameters.remove(parameters.size() - 1);
      values.remove(values.size() - 1);
      --n;
    }
  }

  Object peek(ParameterExpression param) {
    for (int i = parameters.size() - 1; i >= 0; i--) {
      if (parameters.get(i) == param) {
        return values.get(i);
      }
    }
    throw new RuntimeException("parameter " + param + " not on stack");
  }

  Object evaluate(Node expression) {
    return ((AbstractNode) expression).evaluate(this);
  }

  void clear() {
    parameters.clear();
    values.clear();
  }
}

// End Evaluator.java
