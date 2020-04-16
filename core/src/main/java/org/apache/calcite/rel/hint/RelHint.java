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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
      @Nullable Map<String, String> kvOptions) {
    Objects.requireNonNull(inheritPath);
    Objects.requireNonNull(hintName);
    this.inheritPath = ImmutableList.copyOf(inheritPath);
    this.hintName = hintName;
    this.listOptions = listOption == null ? ImmutableList.of() : ImmutableList.copyOf(listOption);
    this.kvOptions = kvOptions == null ? ImmutableMap.of() : ImmutableMap.copyOf(kvOptions);
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a hint builder with specified hint name. */
  public static Builder builder(String hintName) {
    return new Builder(hintName);
  }

  /**
   * Returns a copy of this hint with specified inherit path.
   *
   * @param inheritPath Hint path
   * @return the new {@code RelHint}
   */
  public RelHint copy(List<Integer> inheritPath) {
    Objects.requireNonNull(inheritPath);
    return new RelHint(inheritPath, hintName, listOptions, kvOptions);
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

  //~ Inner Class ------------------------------------------------------------

  /** Builder for {@link RelHint}. */
  public static class Builder {
    private String hintName;
    private List<Integer> inheritPath;

    private List<String> listOptions;
    private Map<String, String> kvOptions;

    private Builder(String hintName) {
      this.listOptions = new ArrayList<>();
      this.kvOptions = new LinkedHashMap<>();
      this.hintName = hintName;
      this.inheritPath = ImmutableList.of();
    }

    /** Sets up the inherit path with given integer list. */
    public Builder inheritPath(Iterable<Integer> inheritPath) {
      this.inheritPath = ImmutableList.copyOf(Objects.requireNonNull(inheritPath));
      return this;
    }

    /** Sets up the inherit path with given integer array. */
    public Builder inheritPath(Integer... inheritPath) {
      this.inheritPath = Arrays.asList(inheritPath);
      return this;
    }

    /** Add a hint option as string. */
    public Builder hintOption(String hintOption) {
      Objects.requireNonNull(hintOption);
      Preconditions.checkState(this.kvOptions.size() == 0,
          "List options and key value options can not be mixed in");
      this.listOptions.add(hintOption);
      return this;
    }

    /** Add multiple string hint options. */
    public Builder hintOptions(Iterable<String> hintOptions) {
      Objects.requireNonNull(hintOptions);
      Preconditions.checkState(this.kvOptions.size() == 0,
          "List options and key value options can not be mixed in");
      this.listOptions = ImmutableList.copyOf(hintOptions);
      return this;
    }

    /** Add a hint option as string key-value pair. */
    public Builder hintOption(String optionKey, String optionValue) {
      Objects.requireNonNull(optionKey);
      Objects.requireNonNull(optionValue);
      Preconditions.checkState(this.listOptions.size() == 0,
          "List options and key value options can not be mixed in");
      this.kvOptions.put(optionKey, optionValue);
      return this;
    }

    /** Add multiple string key-value pair hint options. */
    public Builder hintOptions(Map<String, String> kvOptions) {
      Objects.requireNonNull(kvOptions);
      Preconditions.checkState(this.listOptions.size() == 0,
          "List options and key value options can not be mixed in");
      this.kvOptions = kvOptions;
      return this;
    }

    public RelHint build() {
      return new RelHint(this.inheritPath, this.hintName, this.listOptions, this.kvOptions);
    }
  }
}
