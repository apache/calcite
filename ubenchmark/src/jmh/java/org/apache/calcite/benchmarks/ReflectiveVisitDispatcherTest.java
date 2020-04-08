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
package org.apache.calcite.benchmarks;

import org.apache.calcite.benchmarks.helper.People;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for {@link org.apache.calcite.util.ReflectiveVisitDispatcherImpl}
 */
@State(Scope.Benchmark)
public class ReflectiveVisitDispatcherTest {

  private ReflectiveVisitor visitor = new People();

  @Param({ "0", "128" })
  int cacheMaxSize;

  @Setup
  public void setup() {
    System.setProperty("calcite.reflect.visit.dispatcher.method.cache.maxSize",
        String.valueOf(cacheMaxSize));
  }

  @Benchmark
  public String testReflectiveVisitorDispatcherInvoke() {
    ReflectUtil.MethodDispatcher<String> dispatcher = ReflectUtil.createMethodDispatcher(
        String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ReflectiveVisitDispatcherTest.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
