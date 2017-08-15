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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class to hold rules to determine if a type is assignable from another type.
 *
 * <p>REVIEW 7/05/04 Wael: We should split this up in Cast rules, symmetric and
 * asymmetric assignable rules
 */
public class SqlTypeAssignmentRules {
  //~ Static fields/initializers ---------------------------------------------

  private static SqlTypeAssignmentRules instance = null;

  private final Map<SqlTypeName, Set<SqlTypeName>> rules;
  private final Map<SqlTypeName, Set<SqlTypeName>> coerceRules;

  //~ Constructors -----------------------------------------------------------

  private SqlTypeAssignmentRules() {
    rules = new HashMap<>();

    Set<SqlTypeName> rule;

    // IntervalYearMonth is assignable from...
    for (SqlTypeName interval : SqlTypeName.YEAR_INTERVAL_TYPES) {
      rules.put(interval, SqlTypeName.YEAR_INTERVAL_TYPES);
    }
    for (SqlTypeName interval : SqlTypeName.DAY_INTERVAL_TYPES) {
      rules.put(interval, SqlTypeName.DAY_INTERVAL_TYPES);
    }

    // Multiset is assignable from...
    rules.put(SqlTypeName.MULTISET, EnumSet.of(SqlTypeName.MULTISET));

    // Tinyint is assignable from...
    rules.put(SqlTypeName.TINYINT, EnumSet.of(SqlTypeName.TINYINT));

    // Smallint is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rules.put(SqlTypeName.SMALLINT, rule);

    // Int is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rules.put(SqlTypeName.INTEGER, rule);

    // BigInt is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rules.put(SqlTypeName.BIGINT, rule);

    // Float is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rules.put(SqlTypeName.FLOAT, rule);

    // Real is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rules.put(SqlTypeName.REAL, rule);

    // Double is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.DECIMAL);
    rule.add(SqlTypeName.FLOAT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.DOUBLE);
    rules.put(SqlTypeName.DOUBLE, rule);

    // Decimal is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TINYINT);
    rule.add(SqlTypeName.SMALLINT);
    rule.add(SqlTypeName.INTEGER);
    rule.add(SqlTypeName.BIGINT);
    rule.add(SqlTypeName.REAL);
    rule.add(SqlTypeName.DOUBLE);
    rule.add(SqlTypeName.DECIMAL);
    rules.put(SqlTypeName.DECIMAL, rule);

    // VarBinary is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.VARBINARY);
    rule.add(SqlTypeName.BINARY);
    rules.put(SqlTypeName.VARBINARY, rule);

    // Char is assignable from...
    rules.put(SqlTypeName.CHAR, EnumSet.of(SqlTypeName.CHAR));

    // VarChar is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    rules.put(SqlTypeName.VARCHAR, rule);

    // Boolean is assignable from...
    rules.put(SqlTypeName.BOOLEAN, EnumSet.of(SqlTypeName.BOOLEAN));

    // Binary is assignable from...
    rule = new HashSet<>();
    rule.add(SqlTypeName.BINARY);
    rule.add(SqlTypeName.VARBINARY);
    rules.put(SqlTypeName.BINARY, rule);

    // Date is assignable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIMESTAMP);
    rules.put(SqlTypeName.DATE, rule);

    // Time is assignable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIMESTAMP);
    rules.put(SqlTypeName.TIME, rule);

    // Time with local time-zone is assignable from ...
    rules.put(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
        EnumSet.of(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));

    // Timestamp is assignable from ...
    rules.put(SqlTypeName.TIMESTAMP, EnumSet.of(SqlTypeName.TIMESTAMP));

    // Timestamp with local time-zone is assignable from ...
    rules.put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        EnumSet.of(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));

    // Geometry is assignable from ...
    rules.put(SqlTypeName.GEOMETRY, EnumSet.of(SqlTypeName.GEOMETRY));

    // Array is assignable from ...
    rules.put(SqlTypeName.ARRAY, EnumSet.of(SqlTypeName.ARRAY));

    // Any is assignable from ...
    rule = new HashSet<>();
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
    rules.put(SqlTypeName.ANY, rule);

    // we use coerceRules when we're casting
    coerceRules = copy(rules);

    // Make numbers symmetrical and
    // make varchar/char castable to/from numbers
    rule = new HashSet<>();
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

    coerceRules.put(
        SqlTypeName.TINYINT,
        copy(rule));
    coerceRules.put(
        SqlTypeName.SMALLINT,
        copy(rule));
    coerceRules.put(
        SqlTypeName.INTEGER,
        copy(rule));
    coerceRules.put(
        SqlTypeName.BIGINT,
        copy(rule));
    coerceRules.put(
        SqlTypeName.FLOAT,
        copy(rule));
    coerceRules.put(
        SqlTypeName.REAL,
        copy(rule));
    coerceRules.put(
        SqlTypeName.DECIMAL,
        copy(rule));
    coerceRules.put(
        SqlTypeName.DOUBLE,
        copy(rule));
    coerceRules.put(
        SqlTypeName.CHAR,
        copy(rule));
    coerceRules.put(
        SqlTypeName.VARCHAR,
        copy(rule));

    // Exact Numerics are castable from intervals
    for (SqlTypeName exactType : SqlTypeName.EXACT_TYPES) {
      rule = coerceRules.get(exactType);
      rule.addAll(SqlTypeName.INTERVAL_TYPES);
    }

    // intervals are castable from Exact Numeric
    for (SqlTypeName typeName : SqlTypeName.INTERVAL_TYPES) {
      rule = coerceRules.get(typeName);
      rule.add(SqlTypeName.TINYINT);
      rule.add(SqlTypeName.SMALLINT);
      rule.add(SqlTypeName.INTEGER);
      rule.add(SqlTypeName.BIGINT);
      rule.add(SqlTypeName.DECIMAL);
      rule.add(SqlTypeName.VARCHAR);
    }

    // varchar is castable from Boolean, Date, time, timestamp, numbers and
    // intervals
    rule = coerceRules.get(SqlTypeName.VARCHAR);
    rule.add(SqlTypeName.BOOLEAN);
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.addAll(SqlTypeName.INTERVAL_TYPES);

    // char is castable from Boolean, Date, time and timestamp and numbers
    rule = coerceRules.get(SqlTypeName.CHAR);
    rule.add(SqlTypeName.BOOLEAN);
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.addAll(SqlTypeName.INTERVAL_TYPES);

    // Boolean is castable from char and varchar
    rule = coerceRules.get(SqlTypeName.BOOLEAN);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);

    // Date, time, and timestamp are castable from
    // char and varchar
    // Date is castable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    coerceRules.put(SqlTypeName.DATE, rule);

    // Time is castable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    coerceRules.put(SqlTypeName.TIME, rule);

    // Time with local time-zone is castable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    coerceRules.put(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, rule);

    // Timestamp is castable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    coerceRules.put(SqlTypeName.TIMESTAMP, rule);

    // Timestamp with local time-zone is castable from ...
    rule = new HashSet<>();
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.DATE);
    rule.add(SqlTypeName.TIME);
    rule.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    rule.add(SqlTypeName.CHAR);
    rule.add(SqlTypeName.VARCHAR);
    coerceRules.put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, rule);
  }

  //~ Methods ----------------------------------------------------------------

  public static synchronized SqlTypeAssignmentRules instance() {
    if (instance == null) {
      instance = new SqlTypeAssignmentRules();
    }
    return instance;
  }

  public boolean canCastFrom(
      SqlTypeName to,
      SqlTypeName from,
      boolean coerce) {
    assert to != null;
    assert from != null;

    Map<SqlTypeName, Set<SqlTypeName>> ruleset =
        coerce ? coerceRules : rules;

    if (to == SqlTypeName.NULL) {
      return false;
    } else if (from == SqlTypeName.NULL) {
      return true;
    }

    final Set<SqlTypeName> rule = ruleset.get(to);
    if (rule == null) {
      // if you hit this assert, see the constructor of this class on how
      // to add new rule
      throw new AssertionError("No assign rules for " + to + " defined");
    }

    return rule.contains(from);
  }

  @SuppressWarnings("unchecked")
  private static <K, V> Map<K, V> copy(Map<K, V> map) {
    Map<K, V> copy = new HashMap<>();
    for (Map.Entry<K, V> e : map.entrySet()) {
      if (e.getValue() instanceof Set) {
        copy.put(e.getKey(), (V) copy((Set) e.getValue()));
      } else {
        copy.put(e.getKey(), e.getValue());
      }
    }
    return copy;
  }

  private static <T> HashSet<T> copy(Set<T> set) {
    return new HashSet<T>(set);
  }
}

// End SqlTypeAssignmentRules.java
