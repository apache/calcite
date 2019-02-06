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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Rules that determine whether a type is assignable from another type.
 */
public class SqlTypeAssignmentRules {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlTypeAssignmentRules INSTANCE;
  private static final SqlTypeAssignmentRules COERCE_INSTANCE;

  private final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;

  //~ Constructors -----------------------------------------------------------

  private SqlTypeAssignmentRules(
      Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
    this.map = ImmutableMap.copyOf(map);
  }

  static {
    final Builder rules = new Builder();

    final Set<SqlTypeName> rule = new HashSet<>();

    // IntervalYearMonth is assignable from...
    for (SqlTypeName interval : SqlTypeName.YEAR_INTERVAL_TYPES) {
      rules.add(interval, SqlTypeName.YEAR_INTERVAL_TYPES);
    }
    for (SqlTypeName interval : SqlTypeName.DAY_INTERVAL_TYPES) {
      rules.add(interval, SqlTypeName.DAY_INTERVAL_TYPES);
    }
    for (SqlTypeName interval : SqlTypeName.DAY_INTERVAL_TYPES) {
      final Set<SqlTypeName> dayIntervalTypes = SqlTypeName.DAY_INTERVAL_TYPES;
      rules.add(interval, dayIntervalTypes);
    }

    // MULTISET is assignable from...
    rules.add(SqlTypeName.MULTISET, EnumSet.of(SqlTypeName.MULTISET));

    // TINYINT is assignable from...
    rules.add(SqlTypeName.TINYINT, EnumSet.of(SqlTypeName.TINYINT));

    // SMALLINT is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rules.add(SqlTypeName.SMALLINT, rule);

    // INTEGER is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rules.add(SqlTypeName.INTEGER, rule);

    // BIGINT is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rules.add(SqlTypeName.BIGINT, rule);

    // FLOAT (up to 64 bit floating point) is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rules.add(SqlTypeName.FLOAT, rule);

    // REAL (32 bit floating point) is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rules.add(SqlTypeName.REAL, rule);

    // DOUBLE is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.DOUBLE);
    rules.add(SqlTypeName.DOUBLE, rule);

    // DECIMAL is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.DOUBLE);
    rule.add(SqlTypeName.DECIMAL);
    rules.add(SqlTypeName.DECIMAL, rule);

    // VARBINARY is assignable from...
    rule.clear();
    rule.add(SqlTypeName.VARBINARY);
    rule.add(SqlTypeName.BINARY);
    rules.add(SqlTypeName.VARBINARY, rule);

    // CHAR is assignable from...
    rules.add(SqlTypeName.CHAR, EnumSet.of(SqlTypeName.CHAR));

    // VARCHAR is assignable from...
    rule.clear();
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    rules.add(SqlTypeName.VARCHAR, rule);

    // BOOLEAN is assignable from...
    rules.add(SqlTypeName.BOOLEAN, EnumSet.of(SqlTypeName.BOOLEAN));

    // BINARY is assignable from...
    rule.clear();
    rule.add(SqlTypeName.BINARY);
    rule.add(SqlTypeName.VARBINARY);
    rules.add(SqlTypeName.BINARY, rule);

    // DATE is assignable from...
    rule.clear();
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIMESTAMP);
    rules.add(SqlTypeName.DATE, rule);

    // TIME is assignable from...
    rule.clear();
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIMESTAMP);
    rules.add(SqlTypeName.TIME, rule);

    // TIME WITH LOCAL TIME ZONE is assignable from...
    rules.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
        EnumSet.of(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));

    // TIMESTAMP is assignable from ...
    rules.add(SqlTypeName.TIMESTAMP, EnumSet.of(SqlTypeName.TIMESTAMP));

    // TIMESTAMP WITH LOCAL TIME ZONE is assignable from...
    rules.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        EnumSet.of(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));

    // GEOMETRY is assignable from ...
    rules.add(SqlTypeName.GEOMETRY, EnumSet.of(SqlTypeName.GEOMETRY));

    // ARRAY is assignable from ...
    rules.add(SqlTypeName.ARRAY, EnumSet.of(SqlTypeName.ARRAY));

    // ANY is assignable from ...
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIMESTAMP);
    rules.add(SqlTypeName.ANY, rule);

    // we use coerceRules when we're casting
    final Builder coerceRules = new Builder(rules);

    // Make numbers symmetrical,
    // and make VARCHAR and CHAR castable to/from numbers
    rule.clear();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.DOUBLE);

    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);

    coerceRules.add(SqlTypeName.TINYINT, rule);
    coerceRules.add(SqlTypeName.SMALLINT, rule);
    coerceRules.add(SqlTypeName.INTEGER, rule);
    coerceRules.add(SqlTypeName.BIGINT, rule);
    coerceRules.add(SqlTypeName.FLOAT, rule);
    coerceRules.add(SqlTypeName.REAL, rule);
    coerceRules.add(SqlTypeName.DECIMAL, rule);
    coerceRules.add(SqlTypeName.DOUBLE, rule);
    coerceRules.add(SqlTypeName.CHAR, rule);
    coerceRules.add(SqlTypeName.VARCHAR, rule);

    // Exact numeric types are castable from intervals
    for (SqlTypeName exactType : SqlTypeName.EXACT_TYPES) {
      coerceRules.add(exactType,
          coerceRules.copyValues(exactType)
              .addAll(SqlTypeName.INTERVAL_TYPES)
              .build());
    }

    // Intervals are castable from exact numeric
    for (SqlTypeName typeName : SqlTypeName.INTERVAL_TYPES) {
      coerceRules.add(typeName,
          coerceRules.copyValues(typeName)
              .add(SqlTypeName.TINYINT)
              .add(SqlTypeName.SMALLINT)
              .add(SqlTypeName.INTEGER)
              .add(SqlTypeName.BIGINT)
              .add(SqlTypeName.DECIMAL)
              .add(SqlTypeName.VARCHAR)
              .build());
    }

    // VARCHAR is castable from BOOLEAN, DATE, TIMESTAMP, numeric types and
    // intervals
    coerceRules.add(SqlTypeName.VARCHAR,
        coerceRules.copyValues(SqlTypeName.VARCHAR)
            .add(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIMESTAMP)
            .addAll(SqlTypeName.INTERVAL_TYPES)
            .build());

    // CHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP and numeric types
    coerceRules.add(SqlTypeName.CHAR,
        coerceRules.copyValues(SqlTypeName.CHAR)
            .add(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIMESTAMP)
            .addAll(SqlTypeName.INTERVAL_TYPES)
            .build());

    // BOOLEAN is castable from CHAR and VARCHAR
    coerceRules.add(SqlTypeName.BOOLEAN,
        coerceRules.copyValues(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    // DATE, TIME, and TIMESTAMP are castable from
    // CHAR and VARCHAR.

    // DATE is castable from...
    coerceRules.add(SqlTypeName.DATE,
        coerceRules.copyValues(SqlTypeName.DATE)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    // TIME is castable from...
    coerceRules.add(SqlTypeName.TIME,
        coerceRules.copyValues(SqlTypeName.TIME)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    // TIME WITH LOCAL TIME ZONE is castable from...
    coerceRules.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
        coerceRules.copyValues(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    // TIMESTAMP is castable from...
    coerceRules.add(SqlTypeName.TIMESTAMP,
        coerceRules.copyValues(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
    coerceRules.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        coerceRules.copyValues(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .build());

    INSTANCE = new SqlTypeAssignmentRules(rules.map);
    COERCE_INSTANCE = new SqlTypeAssignmentRules(coerceRules.map);
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns an instance that does not coerce. */
  public static synchronized SqlTypeAssignmentRules instance() {
    return instance(false);
  }

  /** Returns an instance. */
  public static synchronized SqlTypeAssignmentRules instance(boolean coerce) {
    return coerce ? COERCE_INSTANCE : INSTANCE;
  }

  @Deprecated
  public boolean canCastFrom(
      SqlTypeName to,
      SqlTypeName from,
      boolean coerce) {
    return instance(coerce).canCastFrom(to, from);
  }

  /** Returns whether it is valid to cast a value of from type {@code from} to
   * type {@code to}. */
  public boolean canCastFrom(
      SqlTypeName to,
      SqlTypeName from) {
    Objects.requireNonNull(to);
    Objects.requireNonNull(from);

    if (to == SqlTypeName.NULL) {
      return false;
    } else if (from == SqlTypeName.NULL) {
      return true;
    }

    final Set<SqlTypeName> rule = map.get(to);
    if (rule == null) {
      // if you hit this assert, see the constructor of this class on how
      // to add new rule
      throw new AssertionError("No assign rules for " + to + " defined");
    }

    return rule.contains(from);
  }


  /** Keeps state while maps are building build. */
  private static class Builder {
    final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;
    final LoadingCache<Set<SqlTypeName>, ImmutableSet<SqlTypeName>> sets;

    /** Creates an empty Builder. */
    Builder() {
      this.map = new HashMap<>();
      this.sets =
          CacheBuilder.newBuilder()
              .build(CacheLoader.from(set -> Sets.immutableEnumSet(set)));
    }

    /** Creates a Builder as a copy of another Builder. */
    Builder(Builder builder) {
      this.map = new HashMap<>(builder.map);
      this.sets = builder.sets; // share the same canonical sets
    }

    void add(SqlTypeName fromType, Set<SqlTypeName> toTypes) {
      try {
        map.put(fromType, sets.get(toTypes));
      } catch (ExecutionException e) {
        throw new RuntimeException("populating SqlTypeAssignmentRules", e);
      }
    }

    ImmutableSet.Builder<SqlTypeName> copyValues(SqlTypeName typeName) {
      return ImmutableSet.<SqlTypeName>builder()
          .addAll(map.get(typeName));
    }
  }
}

// End SqlTypeAssignmentRules.java
