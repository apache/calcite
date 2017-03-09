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

import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.server.AvaticaHandler;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.DefaultHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.login.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Utility class for setting up SPNEGO
 */
public class SpnegoTestUtil {

  public static final String JGSS_KERBEROS_TICKET_OID = "1.2.840.113554.1.2.2";

  public static final String REALM = "EXAMPLE.COM";
  public static final String KDC_HOST = "localhost";
  public static final String CLIENT_PRINCIPAL = "client@" + REALM;
  public static final String SERVER_PRINCIPAL = "HTTP/" + KDC_HOST + "@" + REALM;

  private SpnegoTestUtil() {}

  public static int getFreePort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    try {
      s.setReuseAddress(true);
      int port = s.getLocalPort();
      return port;
    } finally {
      if (null != s) {
        s.close();
      }
    }
  }

  public static void setupUser(SimpleKdcServer kdc, File keytab, String principal)
      throws KrbException {
    kdc.createPrincipal(principal);
    kdc.exportPrincipal(principal, keytab);
  }

  /**
   * Recursively deletes a {@link File}.
   */
  public static void deleteRecursively(File d) {
    if (d.isDirectory()) {
      for (String name : d.list()) {
        File child = new File(d, name);
        if (child.isFile()) {
          child.delete();
        } else {
          deleteRecursively(d);
        }
      }
    }
    d.delete();
  }

  /**
   * Creates the SPNEGO JAAS configuration file for the Jetty server
   */
  public static void writeSpnegoConf(File configFile, File serverKeytab)
      throws Exception {
    try (BufferedWriter writer =
             new BufferedWriter(
                 new OutputStreamWriter(
                     new FileOutputStream(configFile),
                     StandardCharsets.UTF_8))) {
      // Server login
      writer.write("com.sun.security.jgss.accept {\n");
      writer.write(" com.sun.security.auth.module.Krb5LoginModule required\n");
      writer.write(" principal=\"" + SERVER_PRINCIPAL + "\"\n");
      writer.write(" useKeyTab=true\n");
      writer.write(" keyTab=\"" + serverKeytab.toURI() + "\"\n");
      writer.write(" storeKey=true \n");
      // Some extra debug information from JAAS
      //writer.write(" debug=true\n");
      writer.write(" isInitiator=false;\n");
      writer.write("};\n");
    }
  }

  public static void refreshJaasConfiguration() {
    // This is *extremely* important to make sure we get the right Configuration instance.
    // Configuration keeps a static instance of Configuration that it will return once it
    // has been initialized. We need to nuke that static instance to make sure our
    // serverSpnegoConfigFile gets read.
    AccessController.doPrivileged(new PrivilegedAction<Configuration>() {
      public Configuration run() {
        return Configuration.getConfiguration();
      }
    }).refresh();
  }

  /**
   * A simple handler which returns "OK " with the client's authenticated name and HTTP/200 or
   * HTTP/401 and the message "Not authenticated!".
   */
  public static class AuthenticationRequiredAvaticaHandler implements AvaticaHandler {
    private final Handler handler = new DefaultHandler();

    @Override public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      Authentication auth = baseRequest.getAuthentication();
      if (Authentication.UNAUTHENTICATED == auth) {
        throw new AssertionError("Unauthenticated users should not reach here!");
      }

      baseRequest.setHandled(true);
      UserAuthentication userAuth = (UserAuthentication) auth;
      UserIdentity userIdentity = userAuth.getUserIdentity();
      Principal userPrincipal = userIdentity.getUserPrincipal();

      response.getWriter().print("OK " + userPrincipal.getName());
      response.setStatus(200);
    }

    @Override public void setServer(Server server) {
      handler.setServer(server);
    }

    @Override public Server getServer() {
      return handler.getServer();
    }

    @Override public void destroy() {
      handler.destroy();
    }

    @Override public void start() throws Exception {
      handler.start();
    }

    @Override public void stop() throws Exception {
      handler.stop();
    }

    @Override public boolean isRunning() {
      return handler.isRunning();
    }

    @Override public boolean isStarted() {
      return handler.isStarted();
    }

    @Override public boolean isStarting() {
      return handler.isStarting();
    }

    @Override public boolean isStopping() {
      return handler.isStopping();
    }

    @Override public boolean isStopped() {
      return handler.isStopped();
    }

    @Override public boolean isFailed() {
      return handler.isFailed();
    }

    @Override public void addLifeCycleListener(Listener listener) {
      handler.addLifeCycleListener(listener);
    }

    @Override public void removeLifeCycleListener(Listener listener) {
      handler.removeLifeCycleListener(listener);
    }

    @Override public void setServerRpcMetadata(RpcMetadataResponse metadata) {}
  }
}

// End SpnegoTestUtil.java
