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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static net.hydromatic.avatica.ConnectionConfigImpl.*;

/**
 * Enumeration of Avatica's built-in connection properties.
 */
public enum BuiltInConnectionProperty implements ConnectionProperty {
  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null),

  /** Timezone, for example 'gmt-3'. Default is the JVM's time zone. */
  TIMEZONE("timezone", Type.STRING, null);

  private final String camelName;
  private final Type type;
  private final Object defaultValue;

  private static final Map<String, BuiltInConnectionProperty> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<String, BuiltInConnectionProperty>();
    for (BuiltInConnectionProperty p : BuiltInConnectionProperty.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(), p);
      NAME_TO_PROPS.put(p.name(), p);
    }
  }

  BuiltInConnectionProperty(String camelName, Type type, Object defaultValue) {
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

// End BuiltInConnectionProperty.java
