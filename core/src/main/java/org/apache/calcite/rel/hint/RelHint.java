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
 * Represents a hint within a relation expression.
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
  public RelHint(
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

  /**
   * Represents a copy of this hint that has a specified inherit path.
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
}

// End RelHint.java
