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
package org.apache.optiq.impl.enumerable;

import org.apache.linq4j.expressions.Expression;

import org.apache.optiq.rex.RexCall;

/**
 * Implements a call via given translator.
 * <p>
 * @see org.apache.optiq.ScalarFunction
 * @see org.apache.optiq.TableFunction
 * @see org.apache.optiq.impl.enumerable.RexImpTable
 */
public interface CallImplementor {
  /** Implements a call. */
  Expression implement(
      RexToLixTranslator translator,
      RexCall call,
      RexImpTable.NullAs nullAs);
}
