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

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import java.util.concurrent.locks.ReentrantLock;

/** Information necessary to create a JDBC connection. Specify one to run
 * tests against a different database. (hsqldb is the default.) */
public class ConnectionSpec {
  public final String url;
  public final String username;
  public final String password;
  public final String driver;

  // CALCITE-687 HSQLDB seems to fail oddly when multiple tests are run concurrently
  private static final ReentrantLock HSQLDB_LOCK = new ReentrantLock();

  public ConnectionSpec(String url, String username, String password,
      String driver) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.driver = driver;
  }

  public static final ConnectionSpec HSQLDB =
      new ConnectionSpec(ScottHsqldb.URI, ScottHsqldb.USER,
          ScottHsqldb.PASSWORD, "org.hsqldb.jdbcDriver");

  /**
   * Return a lock used for controlling concurrent access to the database as it has been observed
   * that concurrent access is causing problems with HSQLDB.
   */
  public static ReentrantLock getDatabaseLock() {
    return HSQLDB_LOCK;
  }
}

// End ConnectionSpec.java
