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
package org.apache.calcite.chinook;

import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Version;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Tests against parameters in prepared statement when using underlying JDBC
 * sub-schema.
 */
class RemotePreparedStatementParametersTest {

  @Test void testSimpleStringParameterShouldWorkWithCalcite() throws Exception {
    assumeFalse(TestUtil.getJavaMajorVersion() >= 23
            && TestUtil.AVATICA_VERSION.compareTo(Version.of(1, 25)) > 0,
        "Cannot run on JDK 23 and higher with Avatica version 1.25 or lower; "
            + "enable when [CALCITE-6588] is fixed in Avatica");

    // given
    ChinookAvaticaServer server = new ChinookAvaticaServer();
    server.startWithCalcite();
    Connection connection = DriverManager.getConnection(server.getURL());
    // when
    PreparedStatement pS =
        connection.prepareStatement("select * from chinook.artist where name = ?");
    pS.setString(1, "AC/DC");
    // then
    ResultSet resultSet = pS.executeQuery();
    server.stop();
  }

  @Test void testSeveralParametersShouldWorkWithCalcite() throws Exception {
    assumeFalse(TestUtil.getJavaMajorVersion() >= 23
            && TestUtil.AVATICA_VERSION.compareTo(Version.of(1, 25)) > 0,
        "Cannot run on JDK 23 and higher with Avatica version 1.25 or lower; "
            + "enable when [CALCITE-6588] is fixed in Avatica");

    // given
    ChinookAvaticaServer server = new ChinookAvaticaServer();
    server.startWithCalcite();
    Connection connection = DriverManager.getConnection(server.getURL());
    // when
    PreparedStatement pS =
        connection.prepareStatement(
            "select * from chinook.track where name = ? or milliseconds > ?");
    pS.setString(1, "AC/DC");
    pS.setInt(2, 10);
    // then
    ResultSet resultSet = pS.executeQuery();
    server.stop();
  }

  @Test void testParametersShouldWorkWithRaw() throws Exception {
    assumeFalse(TestUtil.getJavaMajorVersion() >= 23
            && TestUtil.AVATICA_VERSION.compareTo(Version.of(1, 25)) > 0,
        "Cannot run on JDK 23 and higher with Avatica version 1.25 or lower; "
            + "enable when [CALCITE-6588] is fixed in Avatica");

    // given
    ChinookAvaticaServer server = new ChinookAvaticaServer();
    server.startWithRaw();
    Connection connection = DriverManager.getConnection(server.getURL());
    // when
    PreparedStatement pS =
        connection.prepareStatement("select * from \"Artist\" where \"Name\" = ?");
    pS.setString(1, "AC/DC");
    // then
    ResultSet resultSet = pS.executeQuery();
    server.stop();
  }
}
