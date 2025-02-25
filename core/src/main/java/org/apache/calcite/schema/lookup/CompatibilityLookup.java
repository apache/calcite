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
package org.apache.calcite.schema.lookup;

import org.apache.calcite.linq4j.function.Predicate1;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class warps a {@code Function} and a {@code Supplier} into a
 * {@code Lookup} interface.
 *
 * <p>This class can be used to implement the methods {@code Schema.tables()}
 * and {@code Schema.subSchemas()} of existing schemas.
 *
 * <p>Existing schema classes are implementing a pair of {@code getTable()}
 * and {@code getTableNames()} methods. But these schemas should
 * also provide a {@code tables()} method. This class can be used
 * to implement this method. See {@code Schema.tables()} for
 * an example.
 *
 * @param <T> Element type
 */
public class CompatibilityLookup<T> extends IgnoreCaseLookup<T> {

  private final Function<String, @Nullable T> get;
  private final Supplier<Set<String>> getNames;

  /**
   * Constructor to create a {@code Lookup} instance using a `Function` and a `Supplier`.
   *
   * @param get a function to lookup tables or sub schemas by name
   * @param getNames a function to list all tables or sub schemas
   */
  public CompatibilityLookup(Function<String, @Nullable T> get, Supplier<Set<String>> getNames) {
    this.get = get;
    this.getNames = getNames;
  }

  @Override public @Nullable T get(String name) {
    return get.apply(name);
  }

  @Override public Set<String> getNames(LikePattern pattern) {
    final Predicate1<String> matcher = pattern.matcher();
    return getNames.get().stream()
        .filter(name -> matcher.apply(name))
        .collect(Collectors.toSet());
  }
}
