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
package org.apache.calcite.config;

import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;
import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 * Properties that may be specified on the JDBC connect string.
 */
public enum CalciteConnectionProperty implements ConnectionProperty {
  /** Whether approximate results from {@code COUNT(DISTINCT ...)} aggregate
   * functions are acceptable. */
  APPROXIMATE_DISTINCT_COUNT("approximateDistinctCount", Type.BOOLEAN, false,
      false),

  /** Whether approximate results from "Top N" queries
   * ({@code ORDER BY aggFun DESC LIMIT n}) are acceptable. */
  APPROXIMATE_TOP_N("approximateTopN", Type.BOOLEAN, false, false),

  /** Whether approximate results from aggregate functions on
   * DECIMAL types are acceptable. */
  APPROXIMATE_DECIMAL("approximateDecimal", Type.BOOLEAN, false, false),

  /** Whether to treat empty strings as null for Druid Adapter.
   */
  NULL_EQUAL_TO_EMPTY("nullEqualToEmpty", Type.BOOLEAN, true, false),

  /** Whether to store query results in temporary tables. */
  AUTO_TEMP("autoTemp", Type.BOOLEAN, false, false),

  /** Whether Calcite should use materializations. */
  MATERIALIZATIONS_ENABLED("materializationsEnabled", Type.BOOLEAN, true,
      false),

  /** Whether Calcite should create materializations. */
  CREATE_MATERIALIZATIONS("createMaterializations", Type.BOOLEAN, true, false),

  /** How NULL values should be sorted if neither NULLS FIRST nor NULLS LAST are
   * specified. The default, HIGH, sorts NULL values the same as Oracle. */
  DEFAULT_NULL_COLLATION("defaultNullCollation", Type.ENUM, NullCollation.HIGH,
      true, NullCollation.class),

  /** How many rows the Druid adapter should fetch at a time when executing
   * "select" queries. */
  DRUID_FETCH("druidFetch", Type.NUMBER, 16384, false),

  /** URI of the model. */
  MODEL("model", Type.STRING, null, false),

  /** Lexical policy. */
  LEX("lex", Type.ENUM, Lex.ORACLE, false),

  /** Collection of built-in functions and operators. Valid values include
   * "standard", "bigquery", "calcite", "hive", "mssql", "mysql", "oracle",
   * "postgresql", "redshift", "snowflake","spark", "spatial" and
   * "all"(operators that could be used in all libraries except "standard" and "spatial"),
   * and also comma-separated lists, for example "oracle,spatial". */
  FUN("fun", Type.STRING, "standard", true),

  /** How identifiers are quoted.
   * If not specified, value from {@link #LEX} is used. */
  QUOTING("quoting", Type.ENUM, null, false, Quoting.class),

  /** How identifiers are stored if they are quoted.
   * If not specified, value from {@link #LEX} is used. */
  QUOTED_CASING("quotedCasing", Type.ENUM, null, false, Casing.class),

  /** How identifiers are stored if they are not quoted.
   * If not specified, value from {@link #LEX} is used. */
  UNQUOTED_CASING("unquotedCasing", Type.ENUM, null, false, Casing.class),

  /** Whether identifiers are matched case-sensitively.
   * If not specified, value from {@link #LEX} is used. */
  CASE_SENSITIVE("caseSensitive", Type.BOOLEAN, null, false),

  /** Parser factory.
   *
   * <p>The name of a class that implements
   * {@link org.apache.calcite.sql.parser.SqlParserImplFactory}. */
  PARSER_FACTORY("parserFactory", Type.PLUGIN, null, false),

  /** MetaTableFactory plugin. */
  META_TABLE_FACTORY("metaTableFactory", Type.PLUGIN, null, false),

  /** MetaColumnFactory plugin. */
  META_COLUMN_FACTORY("metaColumnFactory", Type.PLUGIN, null, false),

  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null, false),

  /** Schema factory.
   *
   * <p>The name of a class that implements
   * {@link org.apache.calcite.schema.SchemaFactory}.
   *
   * <p>Ignored if {@link #MODEL} is specified. */
  SCHEMA_FACTORY("schemaFactory", Type.PLUGIN, null, false),

  /** Schema type.
   *
   * <p>Value may be null, "MAP", "JDBC", or "CUSTOM"
   * (implicit if {@link #SCHEMA_FACTORY} is specified).
   *
   * <p>Ignored if {@link #MODEL} is specified. */
  SCHEMA_TYPE("schemaType", Type.ENUM, null, false, JsonSchema.Type.class),

  /** Specifies whether Spark should be used as the engine for processing that
   * cannot be pushed to the source system. If false (the default), Calcite
   * generates code that implements the Enumerable interface. */
  SPARK("spark", Type.BOOLEAN, false, false),

  /** Returns the time zone from the connect string, for example 'gmt-3'.
   * If the time zone is not set then the JVM time zone is returned.
   * Never null. */
  TIME_ZONE("timeZone", Type.STRING, TimeZone.getDefault().getID(), false),

  /** Returns the locale from the connect string.
   * If the locale is not set, returns the root locale.
   * Never null.
   * Examples of valid locales: 'en', 'en_US',
   * 'de_DE', '_GB', 'en_US_WIN', 'de__POSIX', 'fr__MAC', ''. */
  LOCALE("locale", Type.STRING, Locale.ROOT.toString(), false),

  /** If the planner should try de-correlating as much as it is possible.
   * If true (the default), Calcite de-correlates the plan. */
  FORCE_DECORRELATE("forceDecorrelate", Type.BOOLEAN, true, false),

  /** Type system. The name of a class that implements
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem} and has a public
   * default constructor or an {@code INSTANCE} constant. */
  TYPE_SYSTEM("typeSystem", Type.PLUGIN, null, false),

  /** SQL conformance level.
   *
   * <p>Controls the semantics of ISO standard SQL features that are implemented
   * in non-standard ways in some other systems.
   *
   * <p>For example, the {@code SUBSTRING(string FROM start [FOR length])}
   * operator treats negative {@code start} values as 1, but BigQuery's
   * implementation regards negative {@code starts} as counting from the end.
   * If {@code conformance=BIG_QUERY} we will use BigQuery's behavior.
   *
   * <p>This property only affects ISO standard SQL features. For example, the
   * {@code SUBSTR} function is non-standard, so is controlled by the
   * {@link #FUN fun} property. If you set {@code fun=oracle} you will get
   * {@code SUBSTR} with Oracle's semantics; if you set {@code fun=postgres} you
   * will get {@code SUBSTR} with PostgreSQL's (slightly different)
   * semantics. */
  CONFORMANCE("conformance", Type.ENUM, SqlConformanceEnum.DEFAULT, false),

  /** Whether to make implicit type coercion when type mismatch
   * for validation, default true. */
  TYPE_COERCION("typeCoercion", Type.BOOLEAN, true, false),

  /** Whether to make create implicit functions if functions do not exist
   * in the operator table, default false. */
  LENIENT_OPERATOR_LOOKUP("lenientOperatorLookup", Type.BOOLEAN, false, false),

  /** Whether to enable top-down optimization in Volcano planner. */
  TOPDOWN_OPT("topDownOpt", Type.BOOLEAN, CalciteSystemProperty.TOPDOWN_OPT.value(), false);

  private final String camelName;
  private final Type type;
  @SuppressWarnings("ImmutableEnumChecker")
  private final @Nullable Object defaultValue;
  private final boolean required;
  private final @Nullable Class valueClass;

  private static final Map<String, CalciteConnectionProperty> NAME_TO_PROPS;

  /** Deprecated; use {@link #TIME_ZONE}. */
  @Deprecated // to be removed before 2.0
  public static final CalciteConnectionProperty TIMEZONE = TIME_ZONE;

  static {
    NAME_TO_PROPS = new HashMap<>();
    for (CalciteConnectionProperty p : CalciteConnectionProperty.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(Locale.ROOT), p);
      NAME_TO_PROPS.put(p.name(), p);
    }
  }

  CalciteConnectionProperty(String camelName, Type type, @Nullable Object defaultValue,
      boolean required) {
    this(camelName, type, defaultValue, required, null);
  }

  CalciteConnectionProperty(String camelName, Type type, @Nullable Object defaultValue,
      boolean required, @Nullable Class valueClass) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
    this.required = required;
    this.valueClass = type.deduceValueClass(defaultValue, valueClass);
    if (!type.valid(defaultValue, this.valueClass)) {
      throw new AssertionError(camelName);
    }
  }

  @Override public String camelName() {
    return camelName;
  }

  @Override public @Nullable Object defaultValue() {
    return defaultValue;
  }

  @Override public Type type() {
    return type;
  }

  @Override public @Nullable Class valueClass() {
    return valueClass;
  }

  @Override public boolean required() {
    return required;
  }

  @Override public PropEnv wrap(Properties properties) {
    return new PropEnv(parse(properties, NAME_TO_PROPS), this);
  }
}
