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

import org.apache.linq4j.expressions.ParameterExpression;

import org.apache.optiq.DataContext;

import org.apache.optiq.rel.RelImplementorImpl;
import org.apache.optiq.rex.RexBuilder;

/**
 * Abstract base class for implementations of {@link RelImplementorImpl}
 * that generate java code.
 */
public abstract class JavaRelImplementor extends RelImplementorImpl {
  public JavaRelImplementor(RexBuilder rexBuilder) {
    super(rexBuilder);
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return (JavaTypeFactory) super.getTypeFactory();
  }

  /** Returns the expression with which to access the
   * {@link org.apache.optiq.DataContext}. */
  public ParameterExpression getRootExpression() {
    return DataContext.ROOT;
  }
}

// End JavaRelImplementor.java
