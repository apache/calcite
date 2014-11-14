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
package net.hydromatic.avatica;

import java.util.Properties;

/**
 * Definition of a property that may be specified on the JDBC connect string.
 * {@link BuiltInConnectionProperty} enumerates built-in properties, but
 * there may be others; the list is not closed.
 */
public interface ConnectionProperty {
  /** The name of this property. (E.g. "MATERIALIZATIONS_ENABLED".) */
  String name();

  /** The name of this property in camel-case.
   * (E.g. "materializationsEnabled".) */
  String camelName();

  /** Returns the default value of this property. The type must match its data
   * type. */
  Object defaultValue();

  /** Returns the data type of this property. */
  Type type();

  /** Wraps this property with a properties object from which its value can be
   * obtained when needed. */
  ConnectionConfigImpl.PropEnv wrap(Properties properties);

  /** Data type of property. */
  enum Type {
    BOOLEAN,
    STRING,
    ENUM,
    PLUGIN;

    public boolean valid(Object defaultValue) {
      switch (this) {
      case BOOLEAN:
        return defaultValue instanceof Boolean;
      case STRING:
      case PLUGIN:
        return defaultValue instanceof String;
      default:
        return defaultValue instanceof Enum;
      }
    }
  }
}

// End ConnectionProperty.java
