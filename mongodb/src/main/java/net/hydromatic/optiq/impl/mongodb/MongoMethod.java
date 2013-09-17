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
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.linq4j.expressions.Types;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;

/**
 * Builtin methods in the MongoDB adapter.
 */
public enum MongoMethod {
  MONGO_TABLE_FIND(MongoTable.class, "find", String.class, String.class,
      List.class),
  MONGO_TABLE_AGGREGATE(MongoTable.class, "aggregate", List.class, List.class);

  public final Method method;

  private static final HashMap<Method, MongoMethod> MAP =
      new HashMap<Method, MongoMethod>();

  static {
    for (MongoMethod builtinMethod : MongoMethod.values()) {
      MAP.put(builtinMethod.method, builtinMethod);
    }
  }

  MongoMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }

  public static MongoMethod lookup(Method method) {
    return MAP.get(method);
  }
}

// End MongoMethod.java
