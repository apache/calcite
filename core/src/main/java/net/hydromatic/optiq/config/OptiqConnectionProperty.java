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
package net.hydromatic.optiq.config;

import net.hydromatic.avatica.ConnectionProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static net.hydromatic.avatica.ConnectionConfigImpl.*;

/**
 * Properties that may be specified on the JDBC connect string.
 */
public enum OptiqConnectionProperty implements ConnectionProperty {
  /** Whether to store query results in temporary tables. */
  AUTO_TEMP("autoTemp", Type.BOOLEAN, false),

  /** Whether Optiq should use materializations. */
  MATERIALIZATIONS_ENABLED("materializationsEnabled", Type.BOOLEAN, true),

  /** Whether Optiq should create materializations. */
  CREATE_MATERIALIZATIONS("createMaterializations", Type.BOOLEAN, true),

  /** URI of the model. */
  MODEL("model", Type.STRING, null),

  /** Lexical policy. */
  LEX("lex", Type.ENUM, Lex.ORACLE),

  /** How identifiers are quoted.
   *  If not specified, value from {@link #LEX} is used. */
  QUOTING("quoting", Type.ENUM, null),

  /** How identifiers are stored if they are quoted.
   *  If not specified, value from {@link #LEX} is used. */
  QUOTED_CASING("quotedCasing", Type.ENUM, null),

  /** How identifiers are stored if they are not quoted.
   *  If not specified, value from {@link #LEX} is used. */
  UNQUOTED_CASING("unquotedCasing", Type.ENUM, null),

  /** Whether identifiers are matched case-sensitively.
   *  If not specified, value from {@link #LEX} is used. */
  CASE_SENSITIVE("caseSensitive", Type.BOOLEAN, null),

  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null),

  /** Specifies whether Spark should be used as the engine for processing that
   * cannot be pushed to the source system. If false (the default), Optiq
   * generates code that implements the Enumerable interface. */
  SPARK("spark", Type.BOOLEAN, false),

  /** Timezone, for example 'gmt-3'. Default is the JVM's time zone. */
  TIMEZONE("timezone", Type.STRING, null),

  /** Type system. The name of a class that implements
   * {@code org.eigenbase.reltype.RelDataTypeSystem} and has a public default
   * constructor or an {@code INSTANCE} constant. */
  TYPE_SYSTEM("typeSystem", Type.PLUGIN, null);

  private final String camelName;
  private final Type type;
  private final Object defaultValue;

  private static final Map<String, OptiqConnectionProperty> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<String, OptiqConnectionProperty>();
    for (OptiqConnectionProperty property : OptiqConnectionProperty.values()) {
      NAME_TO_PROPS.put(property.camelName.toUpperCase(), property);
      NAME_TO_PROPS.put(property.name(), property);
    }
  }

  OptiqConnectionProperty(String camelName, Type type, Object defaultValue) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
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


  public PropEnv wrap(Properties properties) {
    return new PropEnv(parse(properties, NAME_TO_PROPS), this);
  }
}

// End OptiqConnectionProperty.java
