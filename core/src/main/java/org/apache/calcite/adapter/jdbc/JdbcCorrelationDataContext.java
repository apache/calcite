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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.lang.Integer.parseInt;

/**
 * A special DataContext which handles correlation variable for batch nested loop joins.
 */
public class JdbcCorrelationDataContext implements DataContext {
  public static final int OFFSET = Integer.MAX_VALUE - 10000;

  private final DataContext delegate;
  private final Object[] parameters;

  public JdbcCorrelationDataContext(DataContext delegate, Object[] parameters) {
    this.delegate = delegate;
    this.parameters = parameters;
  }

  @Override public @Nullable SchemaPlus getRootSchema() {
    return delegate.getRootSchema();
  }

  @Override public JavaTypeFactory getTypeFactory() {
    return delegate.getTypeFactory();
  }

  @Override public QueryProvider getQueryProvider() {
    return delegate.getQueryProvider();
  }

  @Override public @Nullable Object get(String name) {
    if (name.startsWith("?")) {
      int index = parseInt(name.substring(1));
      if (index >= OFFSET && index < OFFSET + parameters.length) {
        return parameters[index - OFFSET];
      }
    }
    return delegate.get(name);
  }
}
