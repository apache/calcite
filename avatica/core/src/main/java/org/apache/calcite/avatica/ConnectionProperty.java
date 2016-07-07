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
package org.apache.calcite.avatica;

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

  /** Whether the property is mandatory. */
  boolean required();

  /** Class of values that this property can take. Most useful for
   * {@link Type#ENUM} properties. */
  Class valueClass();

  /** Data type of property. */
  enum Type {
    BOOLEAN,
    STRING,
    NUMBER,
    ENUM,
    PLUGIN;

    /** Deduces the class of a property of this type, given the default value
     * and the user-specified value class (each of which may be null, unless
     * this is an enum or a plugin). */
    public Class deduceValueClass(Object defaultValue, Class valueClass) {
      if (valueClass != null) {
        return valueClass;
      }
      if (defaultValue != null) {
        final Class<?> c = defaultValue.getClass();
        if (c.isAnonymousClass()) {
          // for default values that are anonymous enums
          return c.getSuperclass();
        }
        return c;
      }
      return defaultValueClass();
    }

    /** Returns whether a default value and value types are valid for this
     * kind of property. */
    public boolean valid(Object defaultValue, Class clazz) {
      switch (this) {
      case BOOLEAN:
        return clazz == Boolean.class
            && (defaultValue == null || defaultValue instanceof Boolean);
      case NUMBER:
        return Number.class.isAssignableFrom(clazz)
            && (defaultValue == null || defaultValue instanceof Number);
      case STRING:
        return clazz == String.class
            && (defaultValue == null || defaultValue instanceof String);
      case PLUGIN:
        return clazz != null
            && (defaultValue == null || defaultValue instanceof String);
      case ENUM:
        return Enum.class.isAssignableFrom(clazz)
            && (defaultValue == null || clazz.isInstance(defaultValue));
      default:
        throw new AssertionError();
      }
    }

    public Class defaultValueClass() {
      switch (this) {
      case BOOLEAN:
        return Boolean.class;
      case NUMBER:
        return Number.class;
      case STRING:
        return String.class;
      case PLUGIN:
        return Object.class;
      default:
        throw new AssertionError("must specify value class for an ENUM");
      }
    }
  }
}

// End ConnectionProperty.java
