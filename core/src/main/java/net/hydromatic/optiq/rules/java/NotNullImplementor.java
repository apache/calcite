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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;

import org.eigenbase.rex.RexCall;

import java.util.List;

/**
 * Simplified version of {@link net.hydromatic.optiq.rules.java.CallImplementor} that does not know about
 * null semantics.
 * <p>
 * @see net.hydromatic.optiq.rules.java.RexImpTable
 * @see net.hydromatic.optiq.rules.java.CallImplementor
 *
 **/
public interface NotNullImplementor {
  /**
   * Implements a call with assumption that all the null-checking is
   * implemented by caller.
   * <p>
   * @param translator translator to implement the code
   * @param call call to implement
   * @param translatedOperands arguments of a call
   * @return expression that implements given call
   */
  Expression implement(
      RexToLixTranslator translator,
      RexCall call,
      List<Expression> translatedOperands);
}

// End NotNullImplementor.java
