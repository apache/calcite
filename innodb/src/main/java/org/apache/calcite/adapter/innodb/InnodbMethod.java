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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Builtin methods in InnoDB data source.
 */
public enum InnodbMethod {
  /** Method signature to call for code generation. */
  INNODB_QUERYABLE_QUERY(InnodbTable.InnodbQueryable.class, "query",
      List.class, List.class, IndexCondition.class, Boolean.class);

  @Immutable
  public final Method method;

  public static final ImmutableMap<Method, InnodbMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, InnodbMethod> builder =
        ImmutableMap.builder();
    for (InnodbMethod value : InnodbMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  InnodbMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
