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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelImplementor;
import org.apache.calcite.rex.RexBuilder;

/**
 * Abstract base class for implementations of {@link RelImplementor}
 * that generate java code.
 */
public abstract class JavaRelImplementor implements RelImplementor {
  private final RexBuilder rexBuilder;

  public JavaRelImplementor(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    assert rexBuilder.getTypeFactory() instanceof JavaTypeFactory
        : "Type factory of rexBuilder should be a JavaTypeFactory";
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }

  public JavaTypeFactory getTypeFactory() {
    return (JavaTypeFactory) rexBuilder.getTypeFactory();
  }

  /**
   * Returns the expression used to access
   * {@link org.apache.calcite.DataContext}.
   *
   * @return expression used to access {@link org.apache.calcite.DataContext}.
   */
  public ParameterExpression getRootExpression() {
    return DataContext.ROOT;
  }
}

// End JavaRelImplementor.java
