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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Rules that determine whether a type is castable from another type.
 *
 * <p>These rules specify the conversion matrix with explicit CAST.
 *
 * <p>The implicit type coercion matrix should be a sub-set of this explicit one.
 * We do not define an implicit type coercion matrix, instead we have specific
 * coercion rules for all kinds of SQL contexts which actually define the "matrix".
 *
 * <p>To add a new implementation to this class, follow
 * these steps:
 *
 * <ol>
 *   <li>Initialize a {@link SqlTypeMappingRules.Builder} instance
 *   with default mappings of {@link SqlTypeCoercionRule#INSTANCE}.</li>
 *   <li>Modify the mappings with the Builder.</li>
 *   <li>Construct a new {@link SqlTypeCoercionRule} instance with method
 *   {@link #instance(Map)}.</li>
 *   <li>Set the {@link SqlTypeCoercionRule} instance into the
 *   {@link org.apache.calcite.sql.validate.SqlValidator}.</li>
 * </ol>
 *
 * <p>The code snippet below illustrates how to implement a customized instance.
 *
 * <pre>
 *     // Initialize a Builder instance with the default mappings.
 *     Builder builder = SqlTypeMappingRules.builder();
 *     builder.addAll(SqlTypeCoercionRules.instance().getTypeMapping());
 *
 *     // Do the tweak, for example, if we want to add a rule to allow
 *     // coerce BOOLEAN to TIMESTAMP.
 *     builder.add(SqlTypeName.TIMESTAMP,
 *         builder.copyValues(SqlTypeName.TIMESTAMP)
 *             .add(SqlTypeName.BOOLEAN).build());
 *
 *     // Initialize a SqlTypeCoercionRules with the new builder mappings.
 *     SqlTypeCoercionRules typeCoercionRules = SqlTypeCoercionRules.instance(builder.map);
 *
 *     // Set the SqlTypeCoercionRules instance into the SqlValidator.
 *     SqlValidator validator ...;
 *     validator.setSqlTypeCoercionRules(typeCoercionRules);
 * </pre>
 */
public class SqlTypeCoercionRule implements SqlTypeMappingRule {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlTypeCoercionRule INSTANCE;

  public static final ThreadLocal<SqlTypeCoercionRule> THREAD_PROVIDERS =
      ThreadLocal.withInitial(() -> SqlTypeCoercionRule.INSTANCE);

  //~ Instance fields --------------------------------------------------------

  private final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a {@code SqlTypeCoercionRules} with specified type mappings {@code map}.
   *
   * <p>Make this constructor private intentionally, use {@link #instance()}.
   *
   * @param map The type mapping, for each map entry, the values types can be coerced to
   *            the key type
   */
  private SqlTypeCoercionRule(Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
    this.map = ImmutableMap.copyOf(map);
  }

  static {
    // We use coerceRules when we're casting
    final SqlTypeMappingRules.Builder coerceRules = SqlTypeMappingRules.builder();
    coerceRules.addAll(SqlTypeAssignmentRule.instance().getTypeMapping());

    final Set<SqlTypeName> rule = new HashSet<>();

    // Make numbers symmetrical,
    // and make VARCHAR, CHAR, BOOLEAN and TIMESTAMP castable to/from numbers
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
    rule.add(SqlTypeName.BOOLEAN);
    rule.add(SqlTypeName.TIMESTAMP);
    rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

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
              .add(SqlTypeName.CHAR)
              .add(SqlTypeName.VARCHAR)
              .build());
    }

    // BINARY is castable from VARBINARY, CHARACTERS.
    coerceRules.add(SqlTypeName.BINARY,
        coerceRules.copyValues(SqlTypeName.BINARY)
            .add(SqlTypeName.VARBINARY)
            .addAll(SqlTypeName.CHAR_TYPES)
            .build());

    // VARBINARY is castable from BINARY, CHARACTERS.
    coerceRules.add(SqlTypeName.VARBINARY,
        coerceRules.copyValues(SqlTypeName.VARBINARY)
            .add(SqlTypeName.BINARY)
            .addAll(SqlTypeName.CHAR_TYPES)
            .build());

    // VARCHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
    // intervals
    coerceRules.add(SqlTypeName.VARCHAR,
        coerceRules.copyValues(SqlTypeName.VARCHAR)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .addAll(SqlTypeName.BINARY_TYPES)
            .addAll(SqlTypeName.NUMERIC_TYPES)
            .addAll(SqlTypeName.INTERVAL_TYPES)
            .build());

    // CHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
    // intervals
    coerceRules.add(SqlTypeName.CHAR,
        coerceRules.copyValues(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .add(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .addAll(SqlTypeName.BINARY_TYPES)
            .addAll(SqlTypeName.NUMERIC_TYPES)
            .addAll(SqlTypeName.INTERVAL_TYPES)
            .build());

    // BOOLEAN is castable from ...
    coerceRules.add(SqlTypeName.BOOLEAN,
        coerceRules.copyValues(SqlTypeName.BOOLEAN)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.NUMERIC_TYPES)
            .build());

    // DATE, TIME, and TIMESTAMP are castable from
    // CHAR and VARCHAR.

    // DATE is castable from...
    coerceRules.add(SqlTypeName.DATE,
        coerceRules.copyValues(SqlTypeName.DATE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.BINARY_TYPES)
            .build());

    // TIME is castable from...
    coerceRules.add(SqlTypeName.TIME,
        coerceRules.copyValues(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.BINARY_TYPES)
            .build());

    // TIME WITH LOCAL TIME ZONE is castable from...
    coerceRules.add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
        coerceRules.copyValues(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.BINARY_TYPES)
            .build());

    // TIMESTAMP is castable from...
    coerceRules.add(SqlTypeName.TIMESTAMP,
        coerceRules.copyValues(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.BINARY_TYPES)
            .addAll(SqlTypeName.NUMERIC_TYPES)
            .build());

    // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
    coerceRules.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        coerceRules.copyValues(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.DATE)
            .add(SqlTypeName.TIME)
            .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            .add(SqlTypeName.CHAR)
            .add(SqlTypeName.VARCHAR)
            .addAll(SqlTypeName.BINARY_TYPES)
            .addAll(SqlTypeName.NUMERIC_TYPES)
            .build());

    INSTANCE = new SqlTypeCoercionRule(coerceRules.map);
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns an instance. */
  public static SqlTypeCoercionRule instance() {
    return THREAD_PROVIDERS.get();
  }

  /** Returns an instance with specified type mappings. */
  public static SqlTypeCoercionRule instance(
      Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
    return new SqlTypeCoercionRule(map);
  }

  @Override public Map<SqlTypeName, ImmutableSet<SqlTypeName>> getTypeMapping() {
    return this.map;
  }
}

// End SqlTypeCoercionRule.java
