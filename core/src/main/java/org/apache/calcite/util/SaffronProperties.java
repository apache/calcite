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
package org.apache.calcite.util;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.BooleanProp;
import org.apache.calcite.runtime.Resources.Default;
import org.apache.calcite.runtime.Resources.Resource;
import org.apache.calcite.runtime.Resources.StringProp;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessControlException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Provides an environment for debugging information, et cetera, used by
 * saffron.
 *
 * <p>It is a singleton, accessed via the {@link #INSTANCE} object. It is
 * populated from System properties if saffron is invoked via a <code>
 * main()</code> method, from a <code>javax.servlet.ServletContext</code> if
 * saffron is invoked from a servlet, and so forth. If there is a file called
 * <code>"saffron.properties"</code> in the current directory, it is read too.
 *
 * <p>Every property used in saffron code must have a method in this interface.
 * The method must return a sub-class of
 * {@link org.apache.calcite.runtime.Resources.Prop}. The javadoc
 * comment must describe the name of the property (for example,
 * "net.sf.saffron.connection.PoolSize") and the default value, if any. <em>
 * Developers, please make sure that this remains so!</em>
 */
public interface SaffronProperties {
  /**
   * The boolean property "saffron.opt.allowInfiniteCostConverters" determines
   * whether the optimizer will consider adding converters of infinite cost in
   * order to convert a relational expression from one calling convention to
   * another. The default value is <code>true</code>.
   */
  @Resource("saffron.opt.allowInfiniteCostConverters")
  @Default("true")
  BooleanProp allowInfiniteCostConverters();

  /**
   * The string property "saffron.default.charset" is the name of the default
   * character set. The default is "ISO-8859-1". It is used in
   * {@link org.apache.calcite.sql.validate.SqlValidator}.
   */
  @Resource("saffron.default.charset")
  @Default("ISO-8859-1")
  StringProp defaultCharset();

  /**
   * The string property "saffron.default.nationalcharset" is the name of the
   * default national character set which is used with the N'string' construct
   * which may or may not be different from the {@link #defaultCharset}. The
   * default is "ISO-8859-1". It is used in
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}
   */
  @Resource("saffron.default.nationalcharset")
  @Default("ISO-8859-1")
  StringProp defaultNationalCharset();

  /**
   * The string property "saffron.default.collation.name" is the name of the
   * default collation. The default is "ISO-8859-1$en_US". Used in
   * {@link org.apache.calcite.sql.SqlCollation} and
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}
   */
  @Resource("saffron.default.collation.name")
  @Default("ISO-8859-1$en_US")
  StringProp defaultCollation();

  /**
   * The string property "saffron.default.collation.strength" is the strength
   * of the default collation. The default is "primary". Used in
   * {@link org.apache.calcite.sql.SqlCollation} and
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}
   */
  @Resource("saffron.default.collation.strength")
  @Default("primary")
  StringProp defaultCollationStrength();

  SaffronProperties INSTANCE = Helper.instance();

  /** Helper class. */
  class Helper {
    private Helper() {}

    /**
     * Retrieves the singleton instance of {@link SaffronProperties}.
     */
    static SaffronProperties instance() {
      Properties properties = new Properties();

      // read properties from the file "saffron.properties", if it exists in classpath
      try (InputStream stream = Helper.class.getClassLoader()
          .getResourceAsStream("saffron.properties")) {
        if (stream != null) {
          properties.load(stream);
        }
      } catch (IOException e) {
        throw new RuntimeException("while reading from saffron.properties file", e);
      } catch (AccessControlException e) {
        // we're in a sandbox
      }

      // copy in all system properties which start with "saffron."
      Properties source = System.getProperties();
      for (Enumeration keys = source.keys(); keys.hasMoreElements();) {
        String key = (String) keys.nextElement();
        String value = source.getProperty(key);
        if (key.startsWith("saffron.") || key.startsWith("net.sf.saffron.")) {
          properties.setProperty(key, value);
        }
      }
      return Resources.create(properties, SaffronProperties.class);
    }
  }
}

// End SaffronProperties.java
