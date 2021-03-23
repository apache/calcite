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
package org.apache.calcite.rel.metadata.janino;

import org.apache.calcite.linq4j.Ord;

import java.lang.reflect.Method;

/**
 * Common functions for code generation.
 */
class CodeGeneratorUtil {

  private CodeGeneratorUtil() {
  }

  /** Returns e.g. ",\n boolean ignoreNulls". */
  static StringBuilder paramList(StringBuilder buff, Method method, int startIndex) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())
        .subList(startIndex, method.getParameterCount())) {
      buff.append(",\n      ").append(t.e.getName()).append(" a").append(t.i);
    }
    return buff;
  }

  /** Returns e.g. ", ignoreNulls". */
  static StringBuilder argList(StringBuilder buff, Method method, int startIndex) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())
        .subList(startIndex, method.getParameterCount())) {
      buff.append(", a").append(t.i);
    }
    return buff;
  }
}
