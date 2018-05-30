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
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;

import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import javax.servlet.http.HttpServletRequest;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Test class for providing CustomAvaticaServerConfiguration to the HTTP Server
 */
public class CustomAuthHttpServerTest extends HttpAuthBase {
  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;
  private static HttpServer server;
  private static String url;

  // Counters to keep track of number of function calls
  private static int methodCallCounter1 = 0;
  private static int methodCallCounter2 = 0;
  private static int methodCallCounter3 = 0;

  @Before
  public void before() {
    methodCallCounter1 = 0;
    methodCallCounter2 = 0;
    methodCallCounter3 = 0;
  }

  @After
  public void stopServer() {
    if (null != server) {
      server.stop();
    }
  }

  @Test
  public void testCustomImpersonationConfig() throws Exception {
    AvaticaServerConfiguration configuration = new CustomImpersonationConfig();
    createServer(configuration, false);

    readWriteData(url, "CUSTOM_CONFIG_1_TABLE", new Properties());
    Assert.assertEquals("supportsImpersonation should be called same number of "
            + "times as doAsRemoteUser method", methodCallCounter1, methodCallCounter2);
    Assert.assertEquals("supportsImpersonation should be called same number of "
            + "times as getRemoteUserExtractor method", methodCallCounter1, methodCallCounter3);
  }

  @Test
  public void testCustomBasicImpersonationConfigWithAllowedUser() throws Exception {
    AvaticaServerConfiguration configuration = new CustomBasicImpersonationConfig();
    createServer(configuration, true);

    final Properties props = new Properties();
    props.put("avatica_user", "USER2");
    props.put("avatica_password", "password2");
    props.put("user", "USER2");
    props.put("password", "password2");

    readWriteData(url, "CUSTOM_CONFIG_2_ALLOWED_TABLE", props);
    Assert.assertEquals("supportsImpersonation should be called same number of "
            + "times as doAsRemoteUser method", methodCallCounter1, methodCallCounter2);
    Assert.assertEquals("supportsImpersonation should be called same number of "
            + "times as getRemoteUserExtractor method", methodCallCounter1, methodCallCounter3);
  }

  @Test
  public void testCustomBasicImpersonationConfigWithDisallowedUser() throws Exception {
    AvaticaServerConfiguration configuration = new CustomBasicImpersonationConfig();
    createServer(configuration, true);

    final Properties props = new Properties();
    props.put("avatica_user", "USER1");
    props.put("avatica_password", "password1");
    props.put("user", "USER1");
    props.put("password", "password1");

    try {
      readWriteData(url, "CUSTOM_CONFIG_2_DISALLOWED_TABLE", props);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Failed to execute HTTP Request, got HTTP/403"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCustomConfigDisallowsWithHandlerMethod() {
    AvaticaServerConfiguration configuration = new CustomBasicImpersonationConfig();
    server = new HttpServer.Builder()
            .withCustomAuthentication(configuration)
            .withHandler(Mockito.mock(AvaticaHandler.class))
            .withPort(0)
            .build();
    server.start();
  }

  public static HttpServer getAvaticaServer() {
    return server;
  }

  @SuppressWarnings("unchecked") // needed for the mocked customizers, not the builder
  protected void createServer(AvaticaServerConfiguration config, boolean isBasicAuth)
      throws SQLException {
    final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
      CONNECTION_SPEC.username, CONNECTION_SPEC.password);
    LocalService service = new LocalService(jdbcMeta);

    ConnectorCustomizer connectorCustomizer = new ConnectorCustomizer();
    BasicAuthHandlerCustomizer basicAuthCustomizer =
            new BasicAuthHandlerCustomizer(config, service, isBasicAuth);

    server = new HttpServer.Builder()
            .withCustomAuthentication(config)
            .withPort(0)
            .withServerCustomizers(
                    Arrays.asList(connectorCustomizer, basicAuthCustomizer), Server.class)
            .build();
    server.start();

    // Create and grant permissions to our users
    createHsqldbUsers();
    url = "jdbc:avatica:remote:url=http://localhost:" + connectorCustomizer.getLocalPort()
            + ";authentication=BASIC;serialization=PROTOBUF";
  }

  /**
   * Customizer to add ServerConnectors to the server
   */
  static class ConnectorCustomizer implements ServerCustomizer<Server> {

    ServerConnector connector;

    @Override public void customize(Server server) {
      HttpServer avaticaServer = getAvaticaServer();
      connector = avaticaServer.configureConnector(avaticaServer.getServerConnector(), 0);
      server.setConnectors(new Connector[] { connector });
    }

    public int getLocalPort() {
      return connector.getLocalPort();
    }

  }

  /**
   * Customizer to add handlers to the server (with or without BasicAuth)
   */
  static class BasicAuthHandlerCustomizer implements ServerCustomizer<Server> {

    AvaticaServerConfiguration configuration;
    LocalService service;
    boolean isBasicAuth;

    public BasicAuthHandlerCustomizer(AvaticaServerConfiguration configuration
            , LocalService service, boolean isBasicAuth) {
      this.configuration = configuration;
      this.service = service;
      this.isBasicAuth = isBasicAuth;
    }

    @Override public void customize(Server server) {
      HttpServer avaticaServer = getAvaticaServer();

      HandlerFactory factory = new HandlerFactory();
      Handler avaticaHandler = factory.getHandler(service,
              Driver.Serialization.PROTOBUF, null, configuration);

      if (isBasicAuth) {
        ConstraintSecurityHandler securityHandler =
                avaticaServer.configureBasicAuthentication(server, configuration);
        securityHandler.setHandler(avaticaHandler);
        avaticaHandler = securityHandler;
      }

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[] { avaticaHandler, new DefaultHandler()});
      server.setHandler(handlerList);
    }
  }

  /**
   * CustomImpersonationConfig doesn't authenticates the user but supports user impersonation
   */
  static class CustomImpersonationConfig implements AvaticaServerConfiguration {


    @Override public AuthenticationType getAuthenticationType() {
      return AuthenticationType.CUSTOM;
    }

    @Override public String getKerberosRealm() {
      return null;
    }

    @Override public String getKerberosPrincipal() {
      return null;
    }

    @Override public String[] getAllowedRoles() {
      return new String[0];
    }

    @Override public String getHashLoginServiceRealm() {
      return null;
    }

    @Override public String getHashLoginServiceProperties() {
      return null;
    }

    @Override public boolean supportsImpersonation() {
      methodCallCounter1++;
      return true;
    }

    @Override public <T> T doAsRemoteUser(String remoteUserName,
              String remoteAddress, Callable<T> action) throws Exception {
      methodCallCounter2++;
      return action.call();
    }
    @Override public RemoteUserExtractor getRemoteUserExtractor() {
      return new RemoteUserExtractor() {
        @Override public String extract(HttpServletRequest request) {
          methodCallCounter3++;
          return "randomUser";
        }
      };
    }

  }

  /**
   * CustomBasicImpersonationConfig supports BasicAuthentication with user impersonation
   */
  static class CustomBasicImpersonationConfig implements AvaticaServerConfiguration {


    @Override public AuthenticationType getAuthenticationType() {
      return AuthenticationType.CUSTOM;
    }

    @Override public String getKerberosRealm() {
      return null;
    }

    @Override public String getKerberosPrincipal() {
      return null;
    }

    @Override public String[] getAllowedRoles() {
      return new String[] { "users" };
    }

    @Override public String getHashLoginServiceRealm() {
      return "Avatica";
    }

    @Override public String getHashLoginServiceProperties() {
      return HttpAuthBase.getHashLoginServicePropertiesString();
    }

    @Override public boolean supportsImpersonation() {
      methodCallCounter1++;
      return true;
    }

    @Override public <T> T doAsRemoteUser(String remoteUserName,
      String remoteAddress, Callable<T> action) throws Exception {
      methodCallCounter2++;
      if (remoteUserName.equals("USER1")) {
        throw new RemoteUserDisallowedException("USER1 is a disallowed user!");
      }
      return action.call();
    }
    @Override public RemoteUserExtractor getRemoteUserExtractor() {
      return new RemoteUserExtractor() {
        @Override public String extract(HttpServletRequest request)
            throws RemoteUserExtractionException {
          methodCallCounter3++;
          if (request instanceof Request) {
            Authentication authentication = ((Request) request).getAuthentication();
            if (authentication instanceof UserAuthentication) {
              UserIdentity userIdentity = ((UserAuthentication) authentication).getUserIdentity();
              return userIdentity.getUserPrincipal().getName();
            }
          }
          throw new RemoteUserExtractionException("Request doesn't contain user credentials.");
        }
      };
    }
  }

}

// End CustomAuthHttpServerTest.java
