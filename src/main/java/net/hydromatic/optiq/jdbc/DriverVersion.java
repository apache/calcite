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
 * <p>Kept in a separate file so that that version information can be
 * generated.</p>
 */
class DriverVersion {
    final int majorVersion = 0;
    final int minorVersion = 1;
    final String name = "Optiq JDBC Driver";
    final String versionString = "0.1";
    final String productName = "Optiq";
    final String productVersion = "0.1";
    public int databaseMajorVersion = 0;
    public int databaseMinorVersion = 1;
}

// End DriverVersion.java
