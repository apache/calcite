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
package org.apache.calcite.linq4j.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * States which nullable fields are non-null when the method returns a given boolean.
 *
 * <p>It turns a {@code hasFoo()} predicate into something a caller can rely on:
 *
 * <blockquote><pre>
 * &#64;EnsuresNonNullIf(value = "hints", result = true)
 * public boolean hasHints() { return hints != null &amp;&amp; !hints.isEmpty(); }
 * </pre></blockquote>
 *
 * <p>The fields are guaranteed only on the branch that matches {@link #result()}.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface EnsuresNonNullIf {
  /** Returns the fields that are non-null when the method returns {@link #result()}.
   *
   * @return field names, optionally qualified with {@code this.} */
  String[] value();

  /** Returns the result for which the guarantee holds.
   *
   * @return the boolean result that makes the fields non-null */
  boolean result();
}
