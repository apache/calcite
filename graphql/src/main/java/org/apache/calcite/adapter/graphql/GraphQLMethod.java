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
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;

/**
 * Built-in methods in the GraphQL adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
@SuppressWarnings("ImmutableEnumChecker")
enum GraphQLMethod {
  GRAPHQL_QUERY(GraphQLTable.class, "query", String.class);

  final Method method;

  static final ImmutableMap<Method, GraphQLMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, GraphQLMethod> builder =
        ImmutableMap.builder();
    for (GraphQLMethod value : GraphQLMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  /** Defines a method. */
  GraphQLMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
