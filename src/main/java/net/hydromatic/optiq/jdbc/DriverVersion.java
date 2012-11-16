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

/**
 * Driver version information.
 *
 * <p>Each driver implementation must provide an instance of this class, in
 * order to implement {@link Driver#createDriverVersion()}.
 * Typically, drivers create a subclass in a with a constructor that provides
 * all of the arguments for the base class. The instance is held in a separate
 * file, so that that version information can be generated.</p>
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
}

// End DriverVersion.java
