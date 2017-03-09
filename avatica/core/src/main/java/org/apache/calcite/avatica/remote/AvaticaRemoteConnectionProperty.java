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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ConnectionProperty;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;
import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 * Enumeration of Avatica remote driver's built-in connection properties.
 */
public enum AvaticaRemoteConnectionProperty implements ConnectionProperty {
  /** Factory. */
  FACTORY("factory", Type.STRING, null);

  private final String camelName;
  private final Type type;
  private final Object defaultValue;

  private static final Map<String, AvaticaRemoteConnectionProperty> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<>();
    for (AvaticaRemoteConnectionProperty p
        : AvaticaRemoteConnectionProperty.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(Locale.ROOT), p);
      NAME_TO_PROPS.put(p.name(), p);
    }
  }

  AvaticaRemoteConnectionProperty(String camelName,
      Type type,
      Object defaultValue) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
    assert type.valid(defaultValue, type.defaultValueClass());
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

  public Class valueClass() {
    return type.defaultValueClass();
  }

  public PropEnv wrap(Properties properties) {
    return new PropEnv(parse(properties, NAME_TO_PROPS), this);
  }

  public boolean required() {
    return false;
  }
}

// End AvaticaRemoteConnectionProperty.java
