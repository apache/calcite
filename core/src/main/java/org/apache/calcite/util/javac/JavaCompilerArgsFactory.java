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
package org.apache.calcite.util.javac;

/**
 * A <code>JavaCompilerArgsFactory</code> holds an instance of {@link JavaCompilerArgs}.
 */
public class JavaCompilerArgsFactory {

  private JavaCompilerArgsFactory() {}

  private static final ThreadLocal<JavaCompilerArgs> DEFAULT_COMPILER_ARGS =
      ThreadLocal.withInitial(() -> new JavaCompilerArgs());

  /**
   * Return the default compiler args.
   *
   * @return {@link JavaCompilerArgs}
   */
  public static JavaCompilerArgs getDefaultJavaCompilerArgs() {
    return DEFAULT_COMPILER_ARGS.get();
  }

}
