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
package org.apache.calcite.rel.hint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents hint within a relation expression.
 *
 * <p>Every hint has a {@code inheritPath} (integers list) which records its propagate path
 * from the root node,
 * number `0` represents the hint is propagated from the first(left) child,
 * number `1` represents the hint is propagated from the second(right) child.
 *
 * <p>Given a relational expression tree with initial attached hints:
 *
 * <blockquote><pre>
 *            Filter (Hint1)
 *                |
 *               Join
 *              /    \
 *            Scan  Project (Hint2)
 *                     |
 *                    Scan2
 * </pre></blockquote>
 *
 * <p>The plan would have hints path as follows
 * (assumes each hint can be propagated to all child nodes):
 * <ul>
 *   <li>Filter would have hints {Hint1[]}</li>
 *   <li>Join would have hints {Hint1[0]}</li>
 *   <li>Scan would have hints {Hint1[0, 0]}</li>
 *   <li>Project would have hints {Hint1[0,1], Hint2[]}</li>
 *   <li>Scan2 would have hints {[Hint1[0, 1, 0], Hint2[0]}</li>
 * </ul>
 *
 * <p>The {@code listOptions} and {@code kvOptions} are supposed to contain the same information,
 * they are mutually exclusive, that means, they can not both be non-empty.
 *
 * <p>The <code>RelHint</code> is immutable.
 */
public class RelHint {
  //~ Instance fields --------------------------------------------------------

  public final ImmutableList<Integer> inheritPath;
  public final String hintName;
  public final List<String> listOptions;
  public final Map<String, String> kvOptions;
  public final Map<Object, Object> literalKVOptions;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a {@code RelHint}.
   *
   * @param inheritPath Hint inherit path
   * @param hintName    Hint name
   * @param listOption  Hint options as string list
   * @param kvOptions   Hint options as string key value pair
   */
  private RelHint(
      Iterable<Integer> inheritPath,
      String hintName,
      @Nullable List<String> listOption,
      @Nullable Map<String, String> kvOptions,
      @Nullable Map<Object, Object> literalKVOptions) {
    Objects.requireNonNull(inheritPath);
    Objects.requireNonNull(hintName);
    this.inheritPath = ImmutableList.copyOf(inheritPath);
    this.hintName = hintName;
    this.listOptions = listOption == null ? ImmutableList.of() : ImmutableList.copyOf(listOption);
    this.kvOptions = kvOptions == null ? ImmutableMap.of() : ImmutableMap.copyOf(kvOptions);
    this.literalKVOptions =
        literalKVOptions == null ? ImmutableMap.of() : ImmutableMap.copyOf(literalKVOptions);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a {@link RelHint} with {@code inheritPath} and hint name.
   *
   * @param inheritPath Hint inherit path
   * @param hintName    Hint name
   * @return The {@link RelHint} instance with empty options
   */
  public static RelHint of(Iterable<Integer> inheritPath, String hintName) {
    return new RelHint(inheritPath, hintName, null, null, null);
  }

  /**
   * Creates a {@link RelHint} with {@code inheritPath}, hint name and list of string options.
   *
   * @param inheritPath Hint inherit path
   * @param hintName    Hint name
   * @param listOption  Hint options as a string list
   * @return The {@link RelHint} instance with options as string list
   */
  public static RelHint of(Iterable<Integer> inheritPath, String hintName,
      List<String> listOption) {
    return new RelHint(inheritPath, hintName, Objects.requireNonNull(listOption), null, null);
  }

  /**
   * Creates a {@link RelHint} with {@code inheritPath}, hint name
   * and options as string key-values.
   *
   * @param inheritPath Hint inherit path
   * @param hintName    Hint name
   * @param kvOptions   Hint options as string key value pairs
   * @return The {@link RelHint} instance with options as string key value pairs
   */
  public static RelHint of(Iterable<Integer> inheritPath, String hintName,
      Map<String, String> kvOptions) {
    return new RelHint(inheritPath, hintName, null, Objects.requireNonNull(kvOptions), null);
  }

  public static RelHint of(Iterable<Integer> inheritPath, Map<Object, Object> kvOptions,
                           String hintName) {
    return new RelHint(inheritPath, hintName, null, null, Objects.requireNonNull(kvOptions));
  }

  /**
   * Represents a copy of this hint that has a specified inherit path.
   *
   * @param inheritPath Hint path
   * @return the new {@code RelHint}
   */
  public RelHint copy(List<Integer> inheritPath) {
    Objects.requireNonNull(inheritPath);
    return new RelHint(inheritPath, hintName, listOptions, kvOptions, literalKVOptions);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof RelHint)) {
      return false;
    }
    final RelHint that = (RelHint) obj;
    return this.hintName.equals(that.hintName)
        && this.inheritPath.equals(that.inheritPath)
        && Objects.equals(this.listOptions, that.listOptions)
        && Objects.equals(this.kvOptions, that.kvOptions);
  }

  @Override public int hashCode() {
    return Objects.hash(this.hintName, this.inheritPath,
        this.listOptions, this.kvOptions);
  }

  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[")
        .append(this.hintName)
        .append(" inheritPath:")
        .append(this.inheritPath);
    if (this.listOptions.size() > 0 || this.kvOptions.size() > 0) {
      builder.append(" options:");
      if (this.listOptions.size() > 0) {
        builder.append(this.listOptions.toString());
      } else {
        builder.append(this.kvOptions.toString());
      }
    }
    builder.append("]");
    return builder.toString();
  }
}

// End RelHint.java
