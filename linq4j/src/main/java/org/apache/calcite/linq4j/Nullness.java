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
package org.apache.calcite.linq4j;

import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;

/**
 * The methods in this class allow to cast nullable reference to a non-nullable one.
 * This is an internal class, and it is not meant to be used as a public API.
 *
 * <p>The class enables to remove checker-qual runtime dependency, and helps IDEs to see
 * the resulting types of {@code castNonNull} better.
 */
@SuppressWarnings({"cast.unsafe", "RedundantCast", "contracts.postcondition.not.satisfied"})
public class Nullness {
  private Nullness() {
  }

  /**
   * Allows you to treat a nullable type as non-nullable with no assertions.
   *
   * <p>It is useful in the case you have a nullable lately-initialized field
   * like the following:
   *
   * <pre><code>
   * class Wrapper&lt;T&gt; {
   *   &#64;Nullable T value;
   * }
   * </code></pre>
   *
   * <p>That signature allows you to use {@code Wrapper} with both nullable or
   * non-nullable types: {@code Wrapper<@Nullable Integer>}
   * vs {@code Wrapper<Integer>}. Suppose you need to implement
   *
   * <pre><code>
   * T get() { return value; }
   * </code></pre>
   *
   * <p>The issue is checkerframework does not permit that because {@code T}
   * has unknown nullability, so the following needs to be used:
   *
   * <pre><code>
   * T get() { return castNonNull(value); }
   * </code></pre>
   *
   * @param <T>     the type of the reference
   * @param ref     a reference of @Nullable type, that is non-null at run time
   *
   * @return the argument, cast to have the type qualifier @NonNull
   */
  @Pure
  public static @EnsuresNonNull("#1")
  <T extends @Nullable Object> @NonNull T castNonNull(
      @Nullable T ref) {
    //noinspection ConstantConditions
    return (@NonNull T) ref;
  }

  /**
   * Allows you to treat an array of nullable values as an array of non-nullable
   * values.
   *
   * @param <T>     Type of the array elements
   * @param ts      Array
   * @return the argument, cast so that elements are @NonNull
   */
  @SuppressWarnings({"unchecked", "ConstantConditions"})
  @Pure
  public static <T> @NonNull T[] castNonNullArray(
      @Nullable T[] ts) {
    return (@NonNull T []) (Object) ts;
  }

  /**
   * Allows you to treat a list of nullable values as an list of non-nullable
   * values.
   *
   * @param <T>     Type of the list elements
   * @param ts      List
   * @return the argument, cast so that elements are @NonNull
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Pure
  public static <T> List<@NonNull T> castNonNullList(
      List<? extends @Nullable T> ts) {
    return (List) (Object) ts;
  }

  /**
   * Allows you to treat an uninitialized or under-initialization object as
   * initialized with no assertions.
   *
   * @param <T>     The type of the reference
   * @param ref     A reference that was @Uninitialized at some point but is
   *                now fully initialized
   *
   * @return the argument, cast to have type qualifier @Initialized
   */
  @SuppressWarnings({"unchecked"})
  @Pure
  public static <T> T castToInitialized(@UnderInitialization T ref) {
    // To throw CheckerFramework off the scent, we put the object into an array,
    // cast the array to an Object, and cast back to an array.
    Object src = new Object[] {ref};
    Object[] dest = (Object[]) src;
    return (T) dest[0];
  }
}
