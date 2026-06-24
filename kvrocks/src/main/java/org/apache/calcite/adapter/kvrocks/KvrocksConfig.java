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
package org.apache.calcite.adapter.kvrocks;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Connection configuration for the Kvrocks adapter.
 *
 * <p>Holds all parameters needed to establish a connection to a Kvrocks
 * instance: host, port, database index, optional administrator password,
 * and optional namespace token.
 */
public class KvrocksConfig {
  private final String host;
  private final int port;
  private final int database;
  private final @Nullable String password;
  private final @Nullable String namespace;

  KvrocksConfig(String host, int port, int database,
      @Nullable String password) {
    this(host, port, database, password, null);
  }

  KvrocksConfig(String host, int port, int database,
      @Nullable String password, @Nullable String namespace) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.namespace = namespace;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getDatabase() {
    return database;
  }

  public @Nullable String getPassword() {
    return password;
  }

  /**
   * Returns the token associated with a Kvrocks namespace.
   *
   * <p>Kvrocks selects a namespace by authenticating with its token; clients
   * do not send the namespace name separately.
   */
  public @Nullable String getNamespace() {
    return namespace;
  }

  /** Returns the credential used to authenticate the connection. */
  @Nullable String getAuthToken() {
    return namespace == null || namespace.isEmpty() ? password : namespace;
  }
}
