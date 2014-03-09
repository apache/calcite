/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.avatica;

import java.util.*;

/**
 * Properties that may be specified on the JDBC connect string.
 */
public enum ConnectionProperty {
  /** Whether to store query results in temporary tables. */
  AUTO_TEMP("autoTemp", Type.BOOLEAN, "false"),

  /** Whether materializations are enabled. */
  MATERIALIZATIONS_ENABLED("materializationsEnabled", Type.BOOLEAN, "true"),

  /** URI of the model. */
  MODEL("model", Type.STRING, null),

  /** Lexical policy. */
  LEX("lex", Type.ENUM, "ORACLE"),

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
  SPARK("spark", Type.BOOLEAN, "false"),

  /** Timezone, for example 'gmt-3'. Default is the JVM's time zone. */
  TIMEZONE("timezone", Type.STRING, null);

  private final String camelName;
  private final Type type;
  private final String defaultValue;

  private static final Map<String, ConnectionProperty> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<String, ConnectionProperty>();
    for (ConnectionProperty property : ConnectionProperty.values()) {
      NAME_TO_PROPS.put(property.camelName.toUpperCase(), property);
      NAME_TO_PROPS.put(property.name(), property);
    }
  }

  ConnectionProperty(String camelName, Type type, String defaultValue) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  private <T> T get_(Properties properties, Converter<T> converter,
      String defaultValue) {
    final Map<ConnectionProperty, String> map = parse(properties);
    final String s = map.get(this);
    if (s != null) {
      return converter.apply(this, s);
    }
    return converter.apply(this, defaultValue);
  }

  /** Returns the string value of this property, or null if not specified and
   * no default. */
  public String getString(Properties properties) {
    return getString(properties, defaultValue);
  }

  /** Returns the string value of this property, or null if not specified and
   * no default. */
  public String getString(Properties properties, String defaultValue) {
    assert type == Type.STRING;
    return get_(properties, IDENTITY_CONVERTER, defaultValue);
  }

  /** Returns the boolean value of this property. Throws if not set and no
   * default. */
  public boolean getBoolean(Properties properties) {
    return getBoolean(properties, Boolean.valueOf(defaultValue));
  }

  /** Returns the boolean value of this property. Throws if not set and no
   * default. */
  public boolean getBoolean(Properties properties, boolean defaultValue) {
    assert type == Type.BOOLEAN;
    return get_(properties, BOOLEAN_CONVERTER, Boolean.toString(defaultValue));
  }

  /** Returns the enum value of this property. Throws if not set and no
   * default. */
  public <E extends Enum<E>> E getEnum(Properties properties,
      Class<E> enumClass) {
    return getEnum(properties, enumClass,
        Enum.valueOf(enumClass, defaultValue));
  }

  /** Returns the enum value of this property. Throws if not set and no
   * default. */
  public <E extends Enum<E>> E getEnum(Properties properties,
      Class<E> enumClass, E defaultValue) {
    assert type == Type.ENUM;
    //noinspection unchecked
    return get_(properties, enumConverter(enumClass), defaultValue.name());
  }

  /** Converts a {@link java.util.Properties} object containing (name, value)
   * pairs into a map whose keys are
   * {@link net.hydromatic.avatica.InternalProperty} objects.
   *
   * <p>Matching is case-insensitive. Throws if a property is not known.
   * If a property occurs more than once, takes the last occurrence.</p>
   *
   * @param properties Properties
   * @return Map
   * @throws RuntimeException if a property is not known
   */
  static Map<ConnectionProperty, String> parse(Properties properties) {
    final Map<ConnectionProperty, String> map =
        new LinkedHashMap<ConnectionProperty, String>();
    for (String name : properties.stringPropertyNames()) {
      final ConnectionProperty connectionProperty =
          NAME_TO_PROPS.get(name.toUpperCase());
      if (connectionProperty == null) {
        // For now, don't throw. It messes up sub-projects.
        //throw new RuntimeException("Unknown property '" + name + "'");
        continue;
      }
      map.put(connectionProperty, properties.getProperty(name));
    }
    return map;
  }

  /** Callback to parse a property from string to its native type. */
  interface Converter<T> {
    T apply(ConnectionProperty connectionProperty, String s);
  }

  private static final Converter<Boolean> BOOLEAN_CONVERTER =
      new Converter<Boolean>() {
        public Boolean apply(ConnectionProperty connectionProperty, String s) {
          if (s == null) {
            throw new RuntimeException("Required property '"
                + connectionProperty.camelName + "' not specified");
          }
          return Boolean.parseBoolean(s);
        }
      };

  private static final Converter<String> IDENTITY_CONVERTER =
      new Converter<String>() {
        public String apply(ConnectionProperty connectionProperty, String s) {
          return s;
        }
      };

  private static <E extends Enum> Converter<E> enumConverter(
      final Class<E> enumClass) {
    return new Converter<E>() {
      public E apply(ConnectionProperty connectionProperty, String s) {
        if (s == null) {
          throw new RuntimeException("Required property '"
              + connectionProperty.camelName + "' not specified");
        }
        try {
          return (E) Enum.valueOf(enumClass, s);
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("Property '" + s + "' not valid for enum "
              + enumClass.getName());
        }
      }
    };
  }

  /** Data type of property. */
  enum Type {
    BOOLEAN,
    STRING,
    ENUM
  }
}

// End ConnectionProperty.java
