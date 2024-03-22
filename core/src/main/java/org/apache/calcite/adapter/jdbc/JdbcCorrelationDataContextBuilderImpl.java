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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.core.CorrelationId;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;

/**
 * An implementation class of JdbcCorrelationDataContext.
 */
public class JdbcCorrelationDataContextBuilderImpl implements JdbcCorrelationDataContextBuilder {
  private static final Constructor NEW =
      Types.lookupConstructor(JdbcCorrelationDataContext.class, DataContext.class, Object[].class);
  private final ImmutableList.Builder<Expression> parameters = new ImmutableList.Builder<>();
  private int offset = JdbcCorrelationDataContext.OFFSET;
  private final EnumerableRelImplementor implementor;
  private final BlockBuilder builder;
  private final Expression dataContext;

  public JdbcCorrelationDataContextBuilderImpl(EnumerableRelImplementor implementor,
      BlockBuilder builder, Expression dataContext) {
    this.implementor = implementor;
    this.builder = builder;
    this.dataContext = dataContext;
  }

  @Override public int add(CorrelationId id, int ordinal, Type type) {
    parameters.add(implementor.getCorrelVariableGetter(id.getName()).field(builder, ordinal, type));
    return offset++;
  }

  public Expression build() {
    return  Expressions.new_(NEW, dataContext,
      Expressions.newArrayInit(Object.class, 1, parameters.build()));
  }
}
