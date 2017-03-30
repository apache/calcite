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

import java.sql.DatabaseMetaData;
import java.util.Properties;

/**
 * Avatica additions to the JDBC {@link DatabaseMetaData} interface. An instance of this is can be
 * obtained by using {@link #unwrap(Class)} to cast an instance of {@link DatabaseMetaData} to
 * {@link AvaticaSpecificDatabaseMetaData}. {@link #isWrapperFor(Class)} can be used to ensure that
 * the generic interface can be cast to the desired class.
 *
 * <p>A list of all available server-side properties is enumerated by
 * {@link org.apache.calcite.avatica.Meta.DatabaseProperty}. The name of the enum value will be
 * the name of the key in the {@link Properties} returned.
 *
 * <p>Some properties defined in {@link org.apache.calcite.avatica.Meta.DatabaseProperty} do not
 * correspond to a typical JDBC method/property. Those are enumerated here:
 * <table summary="Avatica-Specific Properties">
 *   <tr><th>Property</th><th>Method</th></tr>
 *   <tr><td>AVATICA_VERSION</td><td>getAvaticaServerVersion()</td></tr>
 * </table>
 */
public interface AvaticaSpecificDatabaseMetaData extends DatabaseMetaData {

  /**
   * Retrieves all Avatica-centric properties from the server. See
   * {@link org.apache.calcite.avatica.Meta.DatabaseProperty} for a list of properties that will be
   * returned.
   *
   * @return A {@link Properties} instance containing Avatica properties.
   */
  Properties getRemoteAvaticaProperties();

  /**
   * Retrieves the Avatica version from the server.
   * @return A string corresponding to the server's version.
   */
  String getAvaticaServerVersion();
}

// End AvaticaSpecificDatabaseMetaData.java
