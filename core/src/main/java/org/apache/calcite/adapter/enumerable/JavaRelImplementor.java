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

import net.hydromatic.linq4j.expressions.ParameterExpression;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.RelImplementorImpl;
import org.eigenbase.rex.RexBuilder;

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
   * {@link net.hydromatic.optiq.DataContext}. */
  public ParameterExpression getRootExpression() {
    return DataContext.ROOT;
  }
}

// End JavaRelImplementor.java
