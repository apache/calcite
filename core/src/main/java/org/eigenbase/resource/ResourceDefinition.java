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
package org.eigenbase.resource;

import java.text.DateFormat;
import java.text.Format;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Definition of a resource such as a parameterized message or exception.
 *
 * <p>A resource is identified within a {@link ResourceBundle} by a text
 * <em>key</em>, and has a <em>message</em> in its base locale (which is
 * usually US-English (en_US)). It may also have a set of properties, which are
 * represented as name-value pairs.
 *
 * <p>A resource definition is immutable.
 */
public class ResourceDefinition {
  public final String key;

  /** The message in the base locale. (To find the message in another locale,
   * you will need to load a resource bundle for that locale.) */
  public final String baseMessage;
  private final String[] props;

  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  /** Type of an argument within a format string. */
  enum ArgType {
    STRING, NUMBER, DATE
  }

  /**
   * Creates a resource definition with no properties.
   *
   * @param key Unique name for this resource definition.
   * @param baseMessage Message for this resource definition in the base
   *    locale.
   */
  public ResourceDefinition(String key, String baseMessage) {
    this(key, baseMessage, null);
  }

  /**
   * Creates a resource definition.
   *
   * @param key Unique name for this resource definition.
   * @param baseMessage Message for this resource definition in the base
   *    locale.
   * @param props Array of property name/value pairs.
   *    <code>null</code> means the same as an empty array.
   */
  public ResourceDefinition(String key, String baseMessage, String[] props) {
    this.key = key;
    this.baseMessage = baseMessage;
    if (props == null) {
      props = EMPTY_STRING_ARRAY;
    }
    assert props.length % 2 == 0
        : "Must have even number of property names/values";
    this.props = props;
  }

  /**
   * Returns this resource definition's key.
   *
   * @return Key
   */
  public String getKey() {
    return key;
  }

  /**
   * Returns the properties of this resource definition.
   *
   * @return Properties
   */
  public Properties getProperties() {
    final Properties properties = new Properties();
    for (int i = 0; i < props.length; i++) {
      String prop = props[i];
      String value = props[++i];
      properties.setProperty(prop, value);
    }
    return properties;
  }

  /**
   * Returns the types of arguments.
   *
   * @return Argument types
   */
  public List<ArgType> getArgTypes() {
    return getArgTypes(baseMessage);
  }

  /**
   * Creates an instance of this definition with a set of parameters.
   * This is a factory method, which may be overridden by a derived class.
   *
   * @param bundle Resource bundle the resource instance will belong to
   *   (This contains the locale, among other things.)
   * @param args Arguments to populate the message's parameters.
   *   The arguments must be consistent in number and type with the results
   *   of {@link #getArgTypes}.
   * @return Resource instance
   */
  public ResourceInstance instantiate(ResourceBundle bundle, Object[] args) {
    return new Instance(bundle, this, args);
  }

  /**
   * Parses a message for the arguments inside it, and
   * returns an array with the types of those arguments.
   *
   * <p>For example, <code>getArgTypes("I bought {0,number} {2}s",
   * new String[] {"string", "number", "date", "time"})</code>
   * yields {"number", null, "string"}.
   * Note the null corresponding to missing message #1.
   *
   * @param message Message to be parsed.
   * @return Array of type names
   */
  protected static List<ArgType> getArgTypes(String message) {
    MessageFormat format = new MessageFormat(message);
    Format[] argFormats = format.getFormatsByArgumentIndex();
    List<ArgType> argTypes = new ArrayList<ArgType>();
    for (Format argFormat : argFormats) {
      argTypes.add(formatToType(argFormat));
    }
    return argTypes;
  }

  /**
   * Converts a {@link Format} to a type code.
   */
  private static ArgType formatToType(Format format) {
    if (format == null) {
      return ArgType.STRING;
    } else if (format instanceof NumberFormat) {
      return ArgType.NUMBER;
    } else if (format instanceof DateFormat) {
      // might be date or time, but assume it's date
      return ArgType.DATE;
    } else {
      return ArgType.STRING;
    }
  }

  /**
   * Default implementation of {@link ResourceInstance}.
   */
  private static class Instance implements ResourceInstance {
    ResourceDefinition definition;
    ResourceBundle bundle;
    Object[] args;

    public Instance(
        ResourceBundle bundle,
        ResourceDefinition definition,
        Object[] args) {
      this.definition = definition;
      this.bundle = bundle;
      this.args = args;
    }

    public String toString() {
      String message = bundle.getString(definition.key);
      MessageFormat format = new MessageFormat(message);
      format.setLocale(bundle.getLocale());
      return format.format(args);
    }
  }
}

// End ResourceDefinition.java
