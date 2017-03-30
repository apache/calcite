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

import org.apache.calcite.avatica.remote.AvaticaHttpClientFactoryImpl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;
import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 * Enumeration of Avatica's built-in connection properties.
 */
public enum BuiltInConnectionProperty implements ConnectionProperty {
  /** Factory. */
  FACTORY("factory", Type.PLUGIN, null, false),

  /** Name of initial schema. */
  SCHEMA("schema", Type.STRING, null, false),

  /** Time zone, for example 'gmt-3'. Default is the JVM's time zone. */
  TIME_ZONE("timeZone", Type.STRING, null, false),

  /** Remote URL. */
  URL("url", Type.STRING, null, false),

  /** Serialization used over remote connections */
  SERIALIZATION("serialization", Type.STRING, "json", false),

  /** The type of authentication to be used */
  AUTHENTICATION("authentication", Type.STRING, null, false),

  /** Avatica-based authentication user name */
  AVATICA_USER("avatica_user", Type.STRING, null, false),

  /** Avatica-based authentication password */
  AVATICA_PASSWORD("avatica_password", Type.STRING, null, false),

  /** Factory for constructing http clients. */
  HTTP_CLIENT_FACTORY("httpclient_factory", Type.PLUGIN,
      AvaticaHttpClientFactoryImpl.class.getName(), false),

  /** HttpClient implementation class name. */
  HTTP_CLIENT_IMPL("httpclient_impl", Type.STRING, null, false),

  /** Principal to use to perform Kerberos login. */
  PRINCIPAL("principal", Type.STRING, null, false),

  /** Keytab to use to perform Kerberos login. */
  KEYTAB("keytab", Type.STRING, null, false),

  /** Truststore for SSL/TLS communication */
  TRUSTSTORE("truststore", Type.STRING, null, false),

  /** Password for the truststore */
  TRUSTSTORE_PASSWORD("truststore_password", Type.STRING, null, false);

  private final String camelName;
  private final Type type;
  private final Object defaultValue;
  private Class valueClass;
  private final boolean required;

  /** Deprecated; use {@link #TIME_ZONE}. */
  @Deprecated // to be removed before 2.0
  public static final BuiltInConnectionProperty TIMEZONE = TIME_ZONE;

  private static final Map<String, BuiltInConnectionProperty> NAME_TO_PROPS;
  private static final Set<String> LOCAL_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<>();
    for (BuiltInConnectionProperty p : BuiltInConnectionProperty.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(Locale.ROOT), p);
      NAME_TO_PROPS.put(p.name(), p);
    }

    LOCAL_PROPS = new HashSet<>();
    for (BuiltInConnectionProperty p : BuiltInConnectionProperty.values()) {
      LOCAL_PROPS.add(p.camelName());
    }
  }

  BuiltInConnectionProperty(String camelName, Type type, Object defaultValue,
      boolean required) {
    this(camelName, type, defaultValue, type.defaultValueClass(), required);
  }

  BuiltInConnectionProperty(String camelName, Type type, Object defaultValue,
      Class valueClass, boolean required) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
    this.valueClass = valueClass;
    this.required = required;
    assert type.valid(defaultValue, valueClass);
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

  public Class valueClass() {
    return valueClass;
  }

  public PropEnv wrap(Properties properties) {
    return new PropEnv(parse(properties, NAME_TO_PROPS), this);
  }

  /**
   * Checks if the given property only applicable to the remote driver (should not be sent to the
   * Avatica server).
   *
   * @param propertyName Name of the property
   * @return True if the property denoted by the given name is only relevant locally, otherwise
   *    false.
   */
  public static boolean isLocalProperty(Object propertyName) {
    return LOCAL_PROPS.contains(propertyName);
  }
}

// End BuiltInConnectionProperty.java
