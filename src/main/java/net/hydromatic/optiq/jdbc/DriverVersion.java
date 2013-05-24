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
package net.hydromatic.optiq.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Driver version information.
 *
 * <p>Each driver implementation must provide an instance of this class, in
 * order to implement {@link Driver#createDriverVersion()}.</p>
 *
 * <p>There are two typical ways for a driver to instantiate its version
 * information:</p>
 *
 * <ul>
 *
 * <li>A driver might create a subclass in a with a constructor that provides
 * all of the arguments for the base class. The instance is held in a separate
 * file, so that that version information can be generated.</li>
 *
 * <li>A driver might store the version information in a .properties file and
 * load it using {@link #load}.</li>
 *
 * </ul>
 */
public class DriverVersion {
    public final int majorVersion;
    public final int minorVersion;
    public final String name;
    public final String versionString;
    public final String productName;
    public final String productVersion;
    public final boolean jdbcCompliant;
    public final int databaseMajorVersion;
    public final int databaseMinorVersion;

    /** Creates a DriverVersion. */
    public DriverVersion(
        String name,
        String versionString,
        String productName,
        String productVersion,
        boolean jdbcCompliant,
        int majorVersion,
        int minorVersion,
        int databaseMajorVersion,
        int databaseMinorVersion)
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.name = name;
        this.versionString = versionString;
        this.productName = productName;
        this.productVersion = productVersion;
        this.jdbcCompliant = jdbcCompliant;
        this.databaseMajorVersion = databaseMajorVersion;
        this.databaseMinorVersion = databaseMinorVersion;
    }

    /** Loads a driver version from a properties file, read from the classpath.
     * The arguments provide defaults if the properties cannot be loaded.
     *
     * @param driverClass Class of driver; used to find resource
     * @param resourceName Name of resource file
     * @param driverName Fallback name of driver
     * @param driverVersion Fallback version of driver
     * @param productName Fallback product name
     * @param productVersion Fallback product version
     * @return A populated driver version object, never null
     */
    public static DriverVersion load(
        Class<Driver> driverClass,
        String resourceName,
        String driverName,
        String driverVersion,
        String productName,
        String productVersion)
    {
        boolean jdbcCompliant = true;
        int majorVersion = 0;
        int minorVersion = 0;
        int databaseMajorVersion = 0;
        int databaseMinorVersion = 0;
        try {
            final InputStream inStream =
                driverClass.getClassLoader().getResourceAsStream(resourceName);
            if (inStream != null) {
                final Properties properties = new Properties();
                properties.load(inStream);
                driverName = properties.getProperty("driver.name");
                driverVersion = properties.getProperty("driver.version");
                productName = properties.getProperty("product.name");
                productVersion = properties.getProperty("product.version");
                jdbcCompliant =
                    Boolean.valueOf(properties.getProperty("jdbc.compliant"));
                majorVersion =
                    Integer.valueOf(
                        properties.getProperty("driver.version.major"));
                minorVersion =
                    Integer.valueOf(
                        properties.getProperty("driver.version.minor"));
                databaseMajorVersion =
                    Integer.valueOf(
                        properties.getProperty("database.version.major"));
                databaseMinorVersion =
                    Integer.valueOf(
                        properties.getProperty("database.version.minor"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new DriverVersion(
            driverName, driverVersion, productName, productVersion,
            jdbcCompliant, majorVersion, minorVersion, databaseMajorVersion,
            databaseMinorVersion);
    }
}

// End DriverVersion.java
