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
package org.apache.calcite.util;

/**
 * Four states that describe whether a particular behavior or
 * property is allowed and/or not allowed.
 */
public enum Optionality {
  /** A property is <em>mandatory</em> if an instance must possess it;
   * it is an error if it does not. */
  MANDATORY,

  /** A property is <em>optional</em> if an instance may or may not possess it;
   * neither state is an error. */
  OPTIONAL,

  /** A property is <em>ignored</em> if an instance may or may not possess it;
   * if it possesses the property, the effect is as if it does not. */
  IGNORED,

  /** A property is <em>forbidden</em> if an instance must not possess it;
   * it is an error if the instance has the property. */
  FORBIDDEN
}

// End Optionality.java
