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
 * States how the nullness of a method's result follows from the nullness of its arguments.
 *
 * <p>The value holds one or more clauses separated by {@code ;}. A clause lists one constraint per
 * parameter, then {@code ->}, then what the method guarantees:
 *
 * <blockquote><pre>
 * &#64;Contract("!null, _ -&gt; !null")
 * public static &#64;Nullable Integer plus(&#64;Nullable Integer b0, int b1) { ... }
 * </pre></blockquote>
 *
 * <p>A constraint is {@code null}, {@code !null}, {@code true}, {@code false} or {@code _} for
 * "anything". The guarantee is one of those, or {@code fail} for a method that always throws.
 *
 * <p>This replaces the {@code @PolyNull} qualifier of the Checker Framework, which has no JSpecify
 * equivalent. It cannot describe a receiver parameter, a varargs method, or a type argument such
 * as {@code Enumerable<@Nullable T>}.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface Contract {
  /** Returns the contract clauses.
   *
   * @return contract clauses, separated by {@code ;} */
  String value();
}
