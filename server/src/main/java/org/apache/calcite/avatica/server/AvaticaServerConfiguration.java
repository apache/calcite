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

import java.util.concurrent.Callable;

/**
 * A generic configuration interface that users can implement to configure the {@link HttpServer}.
 */
public interface AvaticaServerConfiguration {

  /**
   * Returns the type of authentication the {@link HttpServer} should use.
   * @return An enum corresponding to an authentication mechanism
   */
  AuthenticationType getAuthenticationType();

  /**
   * Returns the Kerberos realm to use for the server's login. Only relevant when
   * {@link #getAuthenticationType()} returns {@link AuthenticationType#SPNEGO}.
   *
   * @return The Kerberos realm for the server login, or null if not applicable.
   */
  String getKerberosRealm();

  /**
   * Returns the Kerberos principal that the Avatica server should log in as.
   *
   * @return A Kerberos principal, or null if not applicable.
   */
  String getKerberosPrincipal();

  /**
   * Returns the array of allowed roles for login. Only applicable when
   * {@link #getAuthenticationType()} returns {@link AuthenticationType#BASIC} or
   * {@link AuthenticationType#DIGEST}.
   *
   * @return An array of allowed login roles, or null.
   */
  String[] getAllowedRoles();

  /**
   * Returns the name of the realm to use in coordination with the properties files specified
   * by {@link #getHashLoginServiceProperties()}. Only applicable when
   * {@link #getAuthenticationType()} returns {@link AuthenticationType#BASIC} or
   * {@link AuthenticationType#DIGEST}.
   *
   * @return A realm for the HashLoginService, or null.
   */
  String getHashLoginServiceRealm();

  /**
   * Returns the path to a properties file that contains users and realms. Only applicable when
   * {@link #getAuthenticationType()} returns {@link AuthenticationType#BASIC} or
   * {@link AuthenticationType#DIGEST}.
   *
   * @return A realm for the HashLoginService, or null.
   */
  String getHashLoginServiceProperties();

  /**
   * Returns true if the Avatica server should run user requests at that remote user. Otherwise,
   * all requests are run as the Avatica server user (which is the default).
   *
   * @return True if impersonation is enabled, false otherwise.
   */
  boolean supportsImpersonation();

  /**
   * Invokes the given <code>action</code> as the <code>remoteUserName</code>. This will only be
   * invoked if {@link #supportsImpersonation()} returns <code>true</code>.
   *
   * @param remoteUserName The remote user making a request to the Avatica server.
   * @param remoteAddress The address the remote user is making the request from.
   * @return The result from the Callable.
   */
  <T> T doAsRemoteUser(String remoteUserName, String remoteAddress, Callable<T> action)
      throws Exception;
}

// End AvaticaServerConfiguration.java
