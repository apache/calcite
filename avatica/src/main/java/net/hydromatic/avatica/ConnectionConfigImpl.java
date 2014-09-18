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

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Implementation of {@link ConnectionConfig}. */
public class ConnectionConfigImpl implements ConnectionConfig {
  protected final Properties properties;

  public ConnectionConfigImpl(Properties properties) {
    this.properties = properties;
  }

  public String schema() {
    return BuiltInConnectionProperty.SCHEMA.wrap(properties).getString();
  }

  public String timeZone() {
    return BuiltInConnectionProperty.TIMEZONE.wrap(properties).getString();
  }

  /** Converts a {@link Properties} object containing (name, value)
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
  public static Map<ConnectionProperty, String> parse(Properties properties,
      Map<String, ? extends ConnectionProperty> nameToProps) {
    final Map<ConnectionProperty, String> map =
        new LinkedHashMap<ConnectionProperty, String>();
    for (String name : properties.stringPropertyNames()) {
      final ConnectionProperty connectionProperty =
          nameToProps.get(name.toUpperCase());
      if (connectionProperty == null) {
        // For now, don't throw. It messes up sub-projects.
        //throw new RuntimeException("Unknown property '" + name + "'");
        continue;
      }
      map.put(connectionProperty, properties.getProperty(name));
    }
    return map;
  }

  /** The combination of a property definition and a map of property values. */
  public static class PropEnv {
    final Map<? extends ConnectionProperty, String> map;
    private final ConnectionProperty property;

    public PropEnv(Map<? extends ConnectionProperty, String> map,
        ConnectionProperty property) {
      this.map = map;
      this.property = property;
    }

    private <T> T get_(Converter<T> converter, String defaultValue) {
      final String s = map.get(property);
      if (s != null) {
        return converter.apply(property, s);
      }
      return converter.apply(property, defaultValue);
    }

    /** Returns the string value of this property, or null if not specified and
     * no default. */
    public String getString() {
      return getString((String) property.defaultValue());
    }

    /** Returns the string value of this property, or null if not specified and
     * no default. */
    public String getString(String defaultValue) {
      assert property.type() == ConnectionProperty.Type.STRING;
      return get_(IDENTITY_CONVERTER, defaultValue);
    }

    /** Returns the boolean value of this property. Throws if not set and no
     * default. */
    public boolean getBoolean() {
      return getBoolean((Boolean) property.defaultValue());
    }

    /** Returns the boolean value of this property. Throws if not set and no
     * default. */
    public boolean getBoolean(boolean defaultValue) {
      assert property.type() == ConnectionProperty.Type.BOOLEAN;
      return get_(BOOLEAN_CONVERTER, Boolean.toString(defaultValue));
    }

    /** Returns the enum value of this property. Throws if not set and no
     * default. */
    public <E extends Enum<E>> E getEnum(Class<E> enumClass) {
      //noinspection unchecked
      return getEnum(enumClass, (E) property.defaultValue());
    }

    /** Returns the enum value of this property. Throws if not set and no
     * default. */
    public <E extends Enum<E>> E getEnum(Class<E> enumClass, E defaultValue) {
      assert property.type() == ConnectionProperty.Type.ENUM;
      //noinspection unchecked
      return get_(enumConverter(enumClass), defaultValue.name());
    }

    /** Returns an instance of a plugin.
     *
     * <p>Throws if not set and no default.
     * Also throws if the class does not implement the required interface,
     * or if it does not have a public default constructor or an public static
     * field called {@code #INSTANCE}. */
    public <T> T getPlugin(Class<T> pluginClass, T defaultInstance) {
      return getPlugin(pluginClass, (String) property.defaultValue(),
          defaultInstance);
    }

    /** Returns an instance of a plugin, using a given class name if none is
     * set.
     *
     * <p>Throws if not set and no default.
     * Also throws if the class does not implement the required interface,
     * or if it does not have a public default constructor or an public static
     * field called {@code #INSTANCE}. */
    public <T> T getPlugin(Class<T> pluginClass, String defaultClassName,
        T defaultInstance) {
      assert property.type() == ConnectionProperty.Type.PLUGIN;
      return get_(pluginConverter(pluginClass, defaultInstance),
          defaultClassName);
    }
  }

  /** Callback to parse a property from string to its native type. */
  public interface Converter<T> {
    T apply(ConnectionProperty connectionProperty, String s);
  }

  public static final Converter<Boolean> BOOLEAN_CONVERTER =
      new Converter<Boolean>() {
        public Boolean apply(ConnectionProperty connectionProperty, String s) {
          if (s == null) {
            throw new RuntimeException("Required property '"
                + connectionProperty.camelName() + "' not specified");
          }
          return Boolean.parseBoolean(s);
        }
      };

  public static final Converter<String> IDENTITY_CONVERTER =
      new Converter<String>() {
        public String apply(ConnectionProperty connectionProperty, String s) {
          return s;
        }
      };

  public static <E extends Enum> Converter<E> enumConverter(
      final Class<E> enumClass) {
    return new Converter<E>() {
      public E apply(ConnectionProperty connectionProperty, String s) {
        if (s == null) {
          throw new RuntimeException("Required property '"
              + connectionProperty.camelName() + "' not specified");
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

  public static <T> Converter<T> pluginConverter(final Class<T> pluginClass,
      final T defaultInstance) {
    return new Converter<T>() {
      public T apply(ConnectionProperty connectionProperty, String s) {
        if (s == null) {
          if (defaultInstance != null) {
            return defaultInstance;
          }
          throw new RuntimeException("Required property '"
              + connectionProperty.camelName() + "' not specified");
        }
        // First look for a C.INSTANCE field, then do new C().
        try {
          //noinspection unchecked
          final Class<T> clazz = (Class) Class.forName(s);
          assert pluginClass.isAssignableFrom(clazz);
          try {
            // We assume that if there is an INSTANCE field it is static and
            // has the right type.
            final Field field = clazz.getField("INSTANCE");
            return pluginClass.cast(field.get(null));
          } catch (NoSuchFieldException e) {
            // ignore
          }
          return clazz.newInstance();
        } catch (Exception e) {
          throw new RuntimeException("Property '" + s
              + "' not valid for plugin type " + pluginClass.getName(), e);
        }
      }
    };
  }
}

// End ConnectionConfigImpl.java
