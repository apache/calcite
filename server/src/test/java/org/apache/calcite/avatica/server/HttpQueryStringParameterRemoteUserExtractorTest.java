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

import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaServersForTest;
import org.apache.calcite.avatica.remote.Driver.Serialization;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test class for HTTP Basic authentication with an (insecure) specification of the "real" user
 * via an HTTP query string parameter.
 *
 * @see HttpQueryStringParameterRemoteUserExtractor
 */
@RunWith(Parameterized.class)
public class HttpQueryStringParameterRemoteUserExtractorTest extends HttpAuthBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(HttpQueryStringParameterRemoteUserExtractorTest.class);
  private static final AvaticaServersForTest SERVERS = new AvaticaServersForTest();
  private static final Properties PROXY_SERVER_PROPERTIES = new Properties();

  private final HttpServer server;
  private final String missingDoAsUrl;
  private final String disallowedDoAsUserUrl;
  private final String allowedDoAsUserUrl;

  @Parameters(name = "{0}")
  public static List<Object[]> parameters() throws Exception {
    SERVERS.startServers(SERVER_CONFIG);
    return SERVERS.getJUnitParameters();
  }

  @BeforeClass
  public static void loadProxyServerProperties() {
    // The connecting system has one set of credentials.
    PROXY_SERVER_PROPERTIES.put("avatica_user", "USER1");
    PROXY_SERVER_PROPERTIES.put("avatica_password", "password1");
  }

  public HttpQueryStringParameterRemoteUserExtractorTest(Serialization serialization,
      HttpServer server) throws Exception {
    this.server = server;
    int port = this.server.getPort();
    // Create some JDBC urls for basic authentication with varying "doAs" configuration
    missingDoAsUrl = SERVERS.getJdbcUrl(port, serialization) + ";authentication=BASIC";
    // USER4 is valid for HTTP basic auth, but disallowed by the server config (below)
    disallowedDoAsUserUrl = SERVERS.getJdbcUrl(port, serialization, "?doAs=USER4")
        + ";authentication=BASIC";
    allowedDoAsUserUrl = SERVERS.getJdbcUrl(port,  serialization, "?doAs=USER2")
        + ";authenticati0n=BASIC";
    // Create and grant permissions to our users
    createHsqldbUsers();
  }

  private static final AvaticaServerConfiguration SERVER_CONFIG = new AvaticaServerConfiguration() {
    @Override public AuthenticationType getAuthenticationType() {
      // HTTP Basic authentication
      return AuthenticationType.BASIC;
    }

    @Override public String getKerberosRealm() {
      return null;
    }

    @Override public String getKerberosPrincipal() {
      return null;
    }

    @Override public boolean supportsImpersonation() {
      // Impersonation is allowed
      return true;
    }

    @Override public <T> T doAsRemoteUser(String remoteUserName, String remoteAddress,
                                          Callable<T> action) throws Exception {
      // We disallow the remote user "USER4", allow all others
      if (remoteUserName.equals("USER4")) {
        throw new RemoteUserDisallowedException("USER4 is a disallowed user!");
      } else {
        return action.call();
      }
    }

    @Override public RemoteUserExtractor getRemoteUserExtractor() {
      // We extract the "real" user via the "doAs" query string parameter
      return new HttpQueryStringParameterRemoteUserExtractor("doAs");
    }

    @Override public String[] getAllowedRoles() {
      return new String[] { "users" };
    }

    @Override public String getHashLoginServiceRealm() {
      return "Avatica";
    }

    @Override public String getHashLoginServiceProperties() {
      try {
        final String userPropertiesFile =
            URLDecoder.decode(HttpQueryStringParameterRemoteUserExtractorTest.class
                .getResource("/auth-users.properties").getFile(), "UTF-8");
        assertNotNull("Could not find properties file for basic auth users", userPropertiesFile);
        return userPropertiesFile;
      } catch (UnsupportedEncodingException e) {
        LOG.error("Failed to decode path to Jetty users file", e);
        throw new RuntimeException(e);
      }
    }
  };

  @AfterClass public static void stopServer() throws Exception {
    if (null != SERVERS) {
      SERVERS.stopServers();
    }
  }

  @Test public void testUserWithDisallowedRole() throws Exception {
    try {
      readWriteData(missingDoAsUrl, "MISSING_DO_AS", PROXY_SERVER_PROPERTIES);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      LOG.info("Caught expected exception", e);
      assertThat(e.getMessage(), containsString("Failed to execute HTTP Request, got HTTP/401"));
    }
  }

  @Test public void testUserWithDisallowedDoAsRole() throws Exception {
    try {
      readWriteData(disallowedDoAsUserUrl, "DISALLOWED_USER", PROXY_SERVER_PROPERTIES);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      LOG.info("Caught expected exception", e);
      assertThat(e.getMessage(), containsString("Failed to execute HTTP Request, got HTTP/403"));
    }
  }

  @Test public void testAllowedDoAsUser() throws Exception {
    // When we connect with valid credentials and provide a valid "doAs" user, things should work
    readWriteData(allowedDoAsUserUrl, "ALLOWED_USER", PROXY_SERVER_PROPERTIES);
  }
}

// End HttpQueryStringParameterRemoteUserExtractorTest.java
