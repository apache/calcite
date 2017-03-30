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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test class for HTTP Basic authentication.
 */
public class BasicAuthHttpServerTest extends HttpAuthBase {

  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;
  private static HttpServer server;
  private static String url;

  @BeforeClass public static void startServer() throws Exception {
    final String userPropertiesFile = BasicAuthHttpServerTest.class
        .getResource("/auth-users.properties").getFile();
    assertNotNull("Could not find properties file for basic auth users", userPropertiesFile);

    // Create a LocalService around HSQLDB
    final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
        CONNECTION_SPEC.username, CONNECTION_SPEC.password);
    LocalService service = new LocalService(jdbcMeta);

    server = new HttpServer.Builder()
        .withBasicAuthentication(userPropertiesFile, new String[] { "users" })
        .withHandler(service, Driver.Serialization.PROTOBUF)
        .withPort(0)
        .build();
    server.start();

    url = "jdbc:avatica:remote:url=http://localhost:" + server.getPort()
        + ";authentication=BASIC;serialization=PROTOBUF";

    // Create and grant permissions to our users
    createHsqldbUsers();
  }

  @AfterClass public static void stopServer() throws Exception {
    if (null != server) {
      server.stop();
    }
  }

  @Test public void testDisallowedAvaticaAllowedDbUser() throws Exception {
    // Allowed by avatica and hsqldb
    final Properties props = new Properties();
    props.put("avatica_user", "USER2");
    props.put("avatica_password", "foo");
    props.put("user", "USER2");
    props.put("password", "password2");

    try {
      readWriteData(url, "INVALID_AVATICA_USER_VALID_DB_USER", props);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("HTTP/401"));
    }
  }

  @Test public void testDisallowedAvaticaNoDbUser() throws Exception {
    // Allowed by avatica and hsqldb
    final Properties props = new Properties();
    props.put("avatica_user", "USER2");
    props.put("avatica_password", "password2");

    readWriteData(url, "INVALID_AVATICA_USER_NO_DB_USER", props);
  }

  @Test public void testValidUser() throws Exception {
    // Allowed by avatica and hsqldb
    final Properties props = new Properties();
    props.put("avatica_user", "USER2");
    props.put("avatica_password", "password2");
    props.put("user", "USER2");
    props.put("password", "password2");

    readWriteData(url, "VALID_USER", props);
  }

  @Test public void testInvalidUser() throws Exception {
    // Denied by avatica
    final Properties props = new Properties();
    props.put("user", "foo");
    props.put("password", "bar");

    try {
      readWriteData(url, "INVALID_USER", props);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("HTTP/401"));
    }
  }

  @Test public void testUserWithDisallowedRole() throws Exception {
    // Disallowed by avatica
    final Properties props = new Properties();
    props.put("avatica_user", "USER4");
    props.put("avatica_password", "password4");

    try {
      readWriteData(url, "DISALLOWED_AVATICA_USER", props);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("HTTP/403"));
    }
  }

  @Test public void testDisallowedDbUser() throws Exception {
    // Disallowed by hsqldb, allowed by avatica
    final Properties props = new Properties();
    props.put("avatica_user", "USER1");
    props.put("avatica_password", "password1");
    props.put("user", "USER1");
    props.put("password", "password1");

    try {
      readWriteData(url, "DISALLOWED_DB_USER", props);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertEquals("Remote driver error: RuntimeException: "
          + "java.sql.SQLInvalidAuthorizationSpecException: invalid authorization specification"
          + " - not found: USER1"
          + " -> SQLInvalidAuthorizationSpecException: invalid authorization specification - "
          + "not found: USER1"
          + " -> HsqlException: invalid authorization specification - not found: USER1",
          e.getMessage());
    }
  }
}

// End BasicAuthHttpServerTest.java
