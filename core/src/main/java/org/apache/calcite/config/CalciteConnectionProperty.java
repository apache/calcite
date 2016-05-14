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
import org.apache.calcite.util.Bug;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;
import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 * Properties that may be specified on the JDBC connect string.
 */
public enum CalciteConnectionProperty implements ConnectionProperty {
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
      true),

  /** How many rows the Druid adapter should fetch at a time when executing
   * "select" queries. */
  DRUID_FETCH("druidFetch", Type.STRING, "16384",
      Bug.upgrade("convert to Type.NUMBER after [CALCITE-1207]")),

  /** URI of the model. */
  MODEL("model", Type.STRING, null, false),

  /** Lexical policy. */
  LEX("lex", Type.ENUM, Lex.ORACLE, false),

  /** Collection of built-in functions and operators. Valid values include
   * "standard" and "oracle". */
  FUN("fun", Type.STRING, "standard", true),

  /** How identifiers are quoted.
   *  If not specified, value from {@link #LEX} is used. */
  QUOTING("quoting", Type.ENUM, null, false),

  /** How identifiers are stored if they are quoted.
   *  If not specified, value from {@link #LEX} is used. */
  QUOTED_CASING("quotedCasing", Type.ENUM, null, false),

  /** How identifiers are stored if they are not quoted.
   *  If not specified, value from {@link #LEX} is used. */
  UNQUOTED_CASING("unquotedCasing", Type.ENUM, null, false),

  /** Whether identifiers are matched case-sensitively.
   *  If not specified, value from {@link #LEX} is used. */
  CASE_SENSITIVE("caseSensitive", Type.BOOLEAN, null, false),

  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null, false),

  /** Specifies whether Spark should be used as the engine for processing that
   * cannot be pushed to the source system. If false (the default), Calcite
   * generates code that implements the Enumerable interface. */
  SPARK("spark", Type.BOOLEAN, false, false),

  /** Time zone, for example 'gmt-3'. Default is the JVM's time zone. */
  TIME_ZONE("timeZone", Type.STRING, null, false),

  /** If the planner should try de-correlating as much as it is possible.
   * If true (the default), Calcite de-correlates the plan. */
  FORCE_DECORRELATE("forceDecorrelate", Type.BOOLEAN, true, false),

  /** Type system. The name of a class that implements
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem} and has a public
   * default constructor or an {@code INSTANCE} constant. */
  TYPE_SYSTEM("typeSystem", Type.PLUGIN, null, false),

  /** Whether to support SELECT without FROM, e.g. SELECT 2+2. */
  FROM_REQUIRED("fromRequired", Type.BOOLEAN, false, false);

  private final String camelName;
  private final Type type;
  private final Object defaultValue;
  private final boolean required;

  private static final Map<String, CalciteConnectionProperty> NAME_TO_PROPS;

  /** Deprecated; use {@link #TIME_ZONE}. */
  @Deprecated // to be removed before 2.0
  public static final CalciteConnectionProperty TIMEZONE = TIME_ZONE;

  static {
    NAME_TO_PROPS = new HashMap<>();
    for (CalciteConnectionProperty p : CalciteConnectionProperty.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(), p);
      NAME_TO_PROPS.put(p.name(), p);
    }
  }

  CalciteConnectionProperty(String camelName, Type type, Object defaultValue,
      boolean required) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
    this.required = required;
    assert defaultValue == null || type.valid(defaultValue);
  }

  public String camelName() {
    return camelName;
  }

  public Object defaultValue() {
    return defaultValue;
  }

  public Type type() {
    return type;
  }

  public boolean required() {
    return required;
  }

  public PropEnv wrap(Properties properties) {
    return new PropEnv(parse(properties, NAME_TO_PROPS), this);
  }

}

// End CalciteConnectionProperty.java
