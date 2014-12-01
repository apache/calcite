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

import org.apache.calcite.avatica.ConnectionProperty.Type;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;

import java.util.Map;

/**
 * Definitions of properties that drive the behavior of
 * {@link org.apache.calcite.avatica.AvaticaDatabaseMetaData}.
 */
public enum InternalProperty {
  /** Whether identifiers are matched case-sensitively. */
  CASE_SENSITIVE(Type.BOOLEAN, true),

  /** Character that quotes identifiers. */
  SQL_KEYWORDS(Type.STRING, null),

  /** How identifiers are quoted. */
  QUOTING(Quoting.class, Quoting.DOUBLE_QUOTE),

  /** How identifiers are stored if they are quoted. */
  QUOTED_CASING(Casing.class, Casing.UNCHANGED),

  /** How identifiers are stored if they are not quoted. */
  UNQUOTED_CASING(Casing.class, Casing.TO_UPPER),

  /** How identifiers are stored if they are not quoted. */
  NULL_SORTING(NullSorting.class, NullSorting.END);

  private final Type type;
  private final Class enumClass;
  private final Object defaultValue;

  /** Creates an InternalProperty based on an enum. */
  <E extends Enum> InternalProperty(Class<E> enumClass, E defaultValue) {
    this(Type.ENUM, enumClass, defaultValue);
  }

  /** Creates an InternalProperty based on a non-enum type. */
  InternalProperty(Type type, Object defaultValue) {
    this(type, null, defaultValue);
  }

  private InternalProperty(Type type, Class enumClass, Object defaultValue) {
    this.type = type;
    this.enumClass = enumClass;
    this.defaultValue = defaultValue;
  }

  private <T> T get_(Map<InternalProperty, Object> map, T defaultValue) {
    final Object s = map.get(this);
    if (s != null) {
      return (T) s;
    }
    if (defaultValue != null) {
      return (T) defaultValue;
    }
    throw new RuntimeException("Required property '" + name()
        + "' not specified");
  }

  /** Returns the string value of this property, or null if not specified and
   * no default. */
  public String getString(Map<InternalProperty, Object> map) {
    assert type == Type.STRING;
    return get_(map, (String) defaultValue);
  }

  /** Returns the boolean value of this property. Throws if not set and no
   * default. */
  public boolean getBoolean(Map<InternalProperty, Object> map) {
    assert type == Type.BOOLEAN;
    return get_(map, (Boolean) defaultValue);
  }

  /** Returns the enum value of this property. Throws if not set and no
   * default. */
  public <E extends Enum> E getEnum(Map<InternalProperty, Object> map,
      Class<E> enumClass) {
    assert type == Type.ENUM;
    //noinspection unchecked
    return get_(map, (E) defaultValue);
  }

  /** Where nulls appear in a sorted relation. */
  enum NullSorting {
    START, END, LOW, HIGH,
  }
}

// End InternalProperty.java
