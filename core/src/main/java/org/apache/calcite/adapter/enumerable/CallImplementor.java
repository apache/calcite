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
import org.apache.calcite.rex.RexCall;

/**
 * Implements a call via given translator.
 *
 * @see org.apache.calcite.schema.ScalarFunction
 * @see org.apache.calcite.schema.TableFunction
 * @see org.apache.calcite.adapter.enumerable.RexImpTable
 */
public interface CallImplementor {
  /**
   * Implements a call.
   *
   * @param translator Translator for the call
   * @param call Call that should be implemented
   * @param nullAs The desired mode of {@code null} translation
   * @return Translated call
   */
  Expression implement(
      RexToLixTranslator translator,
      RexCall call,
      RexImpTable.NullAs nullAs);
}

// End CallImplementor.java
