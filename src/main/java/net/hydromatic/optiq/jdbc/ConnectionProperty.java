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
package net.hydromatic.optiq.jdbc;

import java.util.*;

/**
 * Properties that may be specified on the JDBC connect string.
 */
enum ConnectionProperty {
  /** Whether to store query results in temporary tables. */
  AUTO_TEMP("autoTemp", Type.BOOLEAN, "false"),

  /** URI of the model. */
  MODEL("model", Type.STRING, null),

  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null);

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

  private String _get(Properties properties) {
    final Map<ConnectionProperty, String> map = parse(properties);
    final String s = map.get(this);
    if (s != null) {
      return s;
    }
    return defaultValue;
  }

  /** Returns the string value of this property, or null if not specified and
   * no default. */
  public String getString(Properties properties) {
    assert type == Type.STRING;
    return _get(properties);
  }

  /** Returns the boolean value of this property. Throws if not set and no
   * default. */
  public boolean getBoolean(Properties properties) {
    assert type == Type.BOOLEAN;
    String s = _get(properties);
    if (s == null) {
      throw new RuntimeException(
          "Required property '" + camelName + "' not specified");
    }
    return Boolean.parseBoolean(s);
  }

  /** Converts a {@link Properties} object containing (name, value) pairs
   * into a map whose keys are {@link ConnectionProperty} objects.
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
      }
      map.put(connectionProperty, properties.getProperty(name));
    }
    return map;
  }

  enum Type {
    BOOLEAN,
    STRING
  }
}

// End ConnectionProperty.java
