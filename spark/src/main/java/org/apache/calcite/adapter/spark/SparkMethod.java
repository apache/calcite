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
package org.apache.calcite.adapter.spark;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Types;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.lang.reflect.Method;
import java.util.HashMap;

/**
 * Built-in methods in the Spark adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
public enum SparkMethod {
  AS_ENUMERABLE(SparkRuntime.class, "asEnumerable", JavaRDD.class),
  ARRAY_TO_RDD(SparkRuntime.class, "createRdd", JavaSparkContext.class,
      Object[].class),
  CREATE_RDD(SparkRuntime.class, "createRdd", JavaSparkContext.class,
      Enumerable.class),
  GET_SPARK_CONTEXT(SparkRuntime.class, "getSparkContext", DataContext.class),
  RDD_FLAT_MAP(JavaRDD.class, "flatMap", FlatMapFunction.class),
  FLAT_MAP_FUNCTION_CALL(FlatMapFunction.class, "call", Object.class);

  public final Method method;

  private static final HashMap<Method, SparkMethod> MAP = new HashMap<>();

  static {
    for (SparkMethod method : SparkMethod.values()) {
      MAP.put(method.method, method);
    }
  }

  SparkMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }

  public static SparkMethod lookup(Method method) {
    return MAP.get(method);
  }
}

// End SparkMethod.java
