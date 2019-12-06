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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rex.RexCall;

/** Implementor of Functions used in MATCH_RECOGNIZE Context. */
public interface MatchImplementor {

  /**
   * Implements a call.
   *
   * @param translator Translator for the call
   * @param call Call that should be implemented
   * @param row Current Row
   * @param rows All Rows that are traversed so far
   * @param symbols All Symbols of the rows that were traversed so far
   * @return Translated call
   */
  Expression implement(
      RexToLixTranslator translator,
      RexCall call,
      ParameterExpression row,
      ParameterExpression rows,
      ParameterExpression symbols,
      ParameterExpression currentIndex);

}

// End MatchImplementor.java
