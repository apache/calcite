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
package org.apache.calcite.model;

import java.util.List;

/**
 * Function schema element.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonFunction {
  /** Name of this function.
   *
   * <p>Required.
   */
  public String name;

  /** Name of the class that implements this function.
   *
   * <p>Required.
   */
  public String className;

  /** Name of the method that implements this function.
   *
   * <p>Optional.
   *
   * <p>If specified, the method must exist (case-sensitive) and Calcite
   * will create a scalar function. The method may be static or non-static, but
   * if non-static, the class must have a public constructor with no parameters.
   *
   * <p>If "*", Calcite creates a function for every method
   * in this class.
   *
   * <p>If not specified, Calcite looks for a method called "eval", and
   * if found, creates a a table macro or scalar function.
   * It also looks for methods "init", "add", "merge", "result", and
   * if found, creates an aggregate function.
   */
  public String methodName;

  /** Path for resolving this function.
   *
   * <p>Optional.
   */
  public List<String> path;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }
}

// End JsonFunction.java
