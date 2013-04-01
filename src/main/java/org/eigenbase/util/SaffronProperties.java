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
package org.eigenbase.util;

import java.io.*;

import java.security.*;

import java.util.*;

import org.eigenbase.util.property.*;


/**
 * Provides an environment for debugging information, et cetera, used by
 * saffron.
 *
 * <p>{@link #getIntProperty} and {@link #getBooleanProperty} are convenience
 * methods.</p>
 *
 * <p>It is a singleton, accessed via the {@link #instance} method. It is
 * populated from System properties if saffron is invoked via a <code>
 * main()</code> method, from a <code>javax.servlet.ServletContext</code> if
 * saffron is invoked from a servlet, and so forth. If there is a file called
 * <code>"saffron.properties"</code> in the current directory, it is read too.
 * </p>
 *
 * <p>Every property used in saffron code must have a member in this class. The
 * member must be public and final, and be of type {@link
 * org.eigenbase.util.property.Property} or some subtype. The javadoc comment
 * must describe the name of the property (for example,
 * "net.sf.saffron.connection.PoolSize") and the default value, if any. <em>
 * Developers, please make sure that this remains so!</em></p>
 */
public class SaffronProperties
    extends Properties
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton properties object.
     */
    private static SaffronProperties properties;

    //~ Instance fields --------------------------------------------------------

    /**
     * The string property "saffron.class.dir" is the path of the directory to
     * compile classes to.
     */
    public final StringProperty classDir =
        new StringProperty(this, "saffron.class.dir", null);

    /**
     * The string property "saffron.java.compiler.class" is the name of the Java
     * compiler to use. It must implement {@link
     * org.eigenbase.javac.JavaCompiler}. The default value is
     * "JP.ac.tsukuba.openjava.SunJavaCompiler".
     */
    public final StringProperty javaCompilerClass =
        new StringProperty(
            this,
            "saffron.java.compiler.class",
            "JP.ac.tsukuba.openjava.SunJavaCompiler");

    /**
     * The string property "saffron.package.name" is the package in which to
     * include temporary classes. The default is "saffron.runtime".
     */
    public final StringProperty packageName =
        new StringProperty(this, "saffron.package.name", "saffron.runtime");

    /**
     * The string property "saffron.java.dir" is the directory to generate
     * temporary java files to. The default is {@link #classDir the class root}.
     */
    public final StringProperty javaDir =
        new StringProperty(this, "saffron.java.dir", null) {
            public String getDefaultValue()
            {
                return classDir.get();
            }
        };

    /**
     * The string property "saffron.java.compiler.args" is the argument string
     * for the {@link #javaCompilerClass java compiler}. {@link
     * org.eigenbase.javac.JavaCompilerArgs#setString} describes how these
     * arguments are interpreted.
     */
    public final StringProperty javaCompilerArgs =
        new StringProperty(this, "saffron.java.compiler.args", null);

    /**
     * The boolean property "saffron.stupid" determines whether to optimize
     * variable assignments. If it is true, records are assigned to a variable
     * even if they are never used. Default is false.
     */
    public final BooleanProperty stupid =
        new BooleanProperty(this, "saffron.stupid", false);

    /**
     * The integer property "saffron.debug.level" determines how much debugging
     * information is printed. The default, 0, means no debugging.
     */
    public final IntegerProperty debugLevel =
        new IntegerProperty(this, "saffron.debug.level", 0);

    /**
     * The string property "saffron.debug.out" is the name of the file to send
     * debugging information to. <code>"out"</code> (the default), means send to
     * {@link System#out}; <code>"err"</code> means send to {@link System#err}.
     */
    public final StringProperty debugOut =
        new StringProperty(this, "saffron.debug.out", "out");

    /**
     * The string property "saffron.test.Name" is used by {@link
     * net.sf.farrago.test.FarragoTestCase} to filter tests.
     */
    public final StringProperty testName =
        new StringProperty(this, "saffron.test.Name", null);

    /**
     * The string property "saffron.test.Class" is used by {@link
     * net.sf.farrago.test.FarragoTestCase} to filter tests.
     */
    public final StringProperty testClass =
        new StringProperty(this, "saffron.test.Class", null);

    /**
     * The string property "saffron.test.Suite" is used by {@link
     * net.sf.farrago.test.FarragoTestCase} to filter tests.
     */
    public final StringProperty testSuite =
        new StringProperty(this, "saffron.test.Suite", null);

    /**
     * The string property "saffron.test.everything" is used by {@link
     * net.sf.farrago.test.FarragoTestCase} to filter tests.
     */
    public final BooleanProperty testEverything =
        new BooleanProperty(this, "saffron.test.everything", false);

    /**
     * The string property "saffron.test.jdbc.url" is the URL of the JDBC
     * database which contains the EMP and DEPT tables used for testing.
     */
    public final StringProperty testJdbcUrl =
        new StringProperty(this, "saffron.test.jdbc.url", null);

    /**
     * The string property "saffron.test.jdbc.drivers" is a comma-separated list
     * of class names to be used as JDBC drivers.
     */
    public final StringProperty testJdbcDrivers =
        new StringProperty(this, "saffron.test.jdbc.drivers", null);

    /**
     * The boolean property "saffron.opt.allowInfiniteCostConverters" determines
     * whether the optimizer will consider adding converters of infinite cost in
     * order to convert a relational expression from one calling convention to
     * another. The default value is <code>true</code>.
     */
    public final BooleanProperty allowInfiniteCostConverters =
        new BooleanProperty(
            this,
            "saffron.opt.allowInfiniteCostConverters",
            true);

    /**
     * The string property "saffron.default.charset" is the name of the default
     * character set. The default is "ISO-8859-1". It is used in {@link
     * org.eigenbase.sql.validate.SqlValidator}.
     */
    public final StringProperty defaultCharset =
        new StringProperty(this, "saffron.default.charset", "ISO-8859-1");

    /**
     * The string property "saffron.default.nationalcharset" is the name of the
     * default national character set which is used with the N'string' construct
     * which may or may not be different from the {@link #defaultCharset}. The
     * default is "ISO-8859-1". It is used in {@link
     * org.eigenbase.sql.SqlLiteral#SqlLiteral}
     */
    public final StringProperty defaultNationalCharset =
        new StringProperty(
            this,
            "saffron.default.nationalcharset",
            "ISO-8859-1");

    /**
     * The string property "saffron.default.collation.name" is the name of the
     * default collation. The default is "ISO-8859-1$en_US". Used in {@link
     * org.eigenbase.sql.SqlCollation} and {@link
     * org.eigenbase.sql.SqlLiteral#SqlLiteral}
     */
    public final StringProperty defaultCollation =
        new StringProperty(
            this,
            "saffron.default.collation.name",
            "ISO-8859-1$en_US");

    /**
     * The string property "saffron.default.collation.strength" is the strength
     * of the default collation. The default is "primary". Used in {@link
     * org.eigenbase.sql.SqlCollation} and {@link
     * org.eigenbase.sql.SqlLiteral#SqlLiteral}
     */
    public final StringProperty defaultCollationStrength =
        new StringProperty(
            this,
            "saffron.default.collation.strength",
            "primary");

    /**
     * The boolean property "saffron.calc.comments.generate" determines if to
     * generate comments in calculator programs in order to make debugging
     * easier. The default is "true". Used in {@link
     * org.eigenbase.sql.SqlCollation} and {@link
     * org.eigenbase.sql.SqlLiteral#SqlLiteral}
     */
    public final BooleanProperty generateCalcProgramComments =
        new BooleanProperty(this, "saffron.calc.comments.generate", true);

    //~ Constructors -----------------------------------------------------------

    /**
     * This constructor is private; please use {@link #instance} to create a
     * {@link SaffronProperties}.
     */
    private SaffronProperties()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the singleton instance of {@link SaffronProperties}.
     */
    public static SaffronProperties instance()
    {
        if (properties == null) {
            properties = new SaffronProperties();

            // read properties from the file "saffron.properties", if it exists
            File file = new File("saffron.properties");
            try {
                if (file.exists()) {
                    try {
                        properties.load(new FileInputStream(file));
                    } catch (IOException e) {
                        throw Util.newInternal(e, "while reading from " + file);
                    }
                }
            } catch (AccessControlException e) {
                // we're in a sandbox
            }

            // copy in all system properties which start with "saffron."
            properties.loadSaffronProperties(System.getProperties());
        }
        return properties;
    }

    /**
     * Adds all saffron-related properties found in the source list. This means
     * all properties whose names start with "saffron." or "net.sf.saffron." The
     * added properties can replace existing properties.
     *
     * @param source a Properties list
     */
    public void loadSaffronProperties(Properties source)
    {
        for (Enumeration keys = source.keys(); keys.hasMoreElements();) {
            String key = (String) keys.nextElement();
            String value = source.getProperty(key);
            if (key.startsWith("saffron.")
                || key.startsWith("net.sf.saffron."))
            {
                properties.setProperty(key, value);
            }
        }
    }

    /**
     * Retrieves a boolean property. Returns <code>true</code> if the property
     * exists, and its value is <code>1</code>, <code>true</code> or <code>
     * yes</code>; returns <code>false</code> otherwise.
     */
    public boolean getBooleanProperty(String key)
    {
        return getBooleanProperty(key, false);
    }

    /**
     * Retrieves a boolean property, or a default value if the property does not
     * exist. Returns <code>true</code> if the property exists, and its value is
     * <code>1</code>, <code>true</code> or <code>yes</code>; the default value
     * if it does not exist; <code>false</code> otherwise.
     */
    public boolean getBooleanProperty(
        String key,
        boolean defaultValue)
    {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value.equalsIgnoreCase("1") || value.equalsIgnoreCase("true")
            || value.equalsIgnoreCase("yes");
    }

    /**
     * Retrieves an integer property. Returns -1 if the property is not found,
     * or if its value is not an integer.
     */
    public int getIntProperty(String key)
    {
        String value = getProperty(key);
        if (value == null) {
            return -1;
        }
        int i = Integer.valueOf(value).intValue();
        return i;
    }
}

// End SaffronProperties.java
