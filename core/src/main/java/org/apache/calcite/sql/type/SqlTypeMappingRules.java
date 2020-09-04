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
package org.apache.calcite.sql.type;

import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This class defines some utilities to build type mapping matrix
 * which would then use to construct the {@link SqlTypeMappingRule} rules.
 */
public abstract class SqlTypeMappingRules {

  /** Returns the {@link SqlTypeMappingRule} instance based on
   * the specified {@code coerce} to indicate whether to return as a type coercion rule.
   *
   * @param  coerce Whether to return rules with type coercion
   * @return {@link SqlTypeCoercionRule} instance if {@code coerce} is true; else
   * returns a {@link SqlTypeAssignmentRule} instance
   */
  public static SqlTypeMappingRule instance(boolean coerce) {
    if (coerce) {
      return SqlTypeCoercionRule.instance();
    } else {
      return SqlTypeAssignmentRule.instance();
    }
  }

  /** Returns a {@link Builder} to build the type mappings. */
  public static Builder builder() {
    return new Builder();
  }

  /** Keeps state while building the type mappings. */
  public static class Builder {
    final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;
    final LoadingCache<Set<SqlTypeName>, ImmutableSet<SqlTypeName>> sets;

    /** Creates an empty {@link Builder}. */
    Builder() {
      this.map = new HashMap<>();
      this.sets =
          CacheBuilder.newBuilder()
              .build(CacheLoader.from(set -> Sets.immutableEnumSet(set)));
    }

    /** Add a map entry to the existing {@link Builder} mapping. */
    void add(SqlTypeName fromType, Set<SqlTypeName> toTypes) {
      try {
        map.put(fromType, sets.get(toTypes));
      } catch (UncheckedExecutionException | ExecutionException e) {
        throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e));
      }
    }

    /** Put all the type mappings to the {@link Builder}. */
    void addAll(Map<SqlTypeName, ImmutableSet<SqlTypeName>> typeMapping) {
      try {
        map.putAll(typeMapping);
      } catch (UncheckedExecutionException e) {
        throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e));
      }
    }

    /** Copy the map values from key {@code typeName} and
     * returns as a {@link ImmutableSet.Builder}. */
    ImmutableSet.Builder<SqlTypeName> copyValues(SqlTypeName typeName) {
      return ImmutableSet.<SqlTypeName>builder()
          .addAll(map.get(typeName));
    }
  }
}
