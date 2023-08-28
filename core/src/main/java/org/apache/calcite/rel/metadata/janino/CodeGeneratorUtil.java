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

import java.lang.reflect.Method;

/**
 * Common functions for code generation.
 */
class CodeGeneratorUtil {

  private CodeGeneratorUtil() {
  }

  /** Returns e.g. ",\n boolean ignoreNulls".  This ignores the first 2 arguments. */
  static StringBuilder paramList(StringBuilder buff, Method method) {
    Class<?>[] parameterTypes = method.getParameterTypes();
    for (int i = 2; i < parameterTypes.length; i++) {
      Class<?> t = parameterTypes[i];
      buff.append(",\n      ").append(t.getName()).append(" a").append(i);
    }
    return buff;
  }

  /** Returns e.g. ", a2, a3". This ignores the first 2 arguments. */
  static StringBuilder argList(StringBuilder buff, Method method) {
    Class<?>[] argTypes = method.getParameterTypes();
    for (int i = 2; i < argTypes.length; i++) {
      buff.append(", a").append(i);
    }
    return buff;
  }
}
