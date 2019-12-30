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
package org.apache.calcite.test;

import com.datastax.driver.core.SocketOptions;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Base class for CassandraCQLUnit.
 */
public abstract class BaseCassandraUnit implements BeforeAllCallback, AfterAllCallback {

  protected String configurationFileName;
  protected long startupTimeoutMillis;
  protected int readTimeoutMillis = 12000;

  public BaseCassandraUnit() {
    this(EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT);
  }

  public BaseCassandraUnit(long startupTimeoutMillis) {
    this.startupTimeoutMillis = startupTimeoutMillis;
  }

  @Override public void beforeAll(ExtensionContext context) throws Exception {
    /* start an embedded Cassandra */
    if (configurationFileName != null) {
      EmbeddedCassandraServerHelper.startEmbeddedCassandra(configurationFileName,
          startupTimeoutMillis);
    } else {
      EmbeddedCassandraServerHelper.startEmbeddedCassandra(startupTimeoutMillis);
    }

    /* create structure and load data */
    load();
  }

  @Override public void afterAll(ExtensionContext context) throws Exception {
    if (EmbeddedCassandraServerHelper.getCluster() != null) {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
  }

  protected abstract void load();

  /**
   * Gets a base SocketOptions with an overridden readTimeoutMillis.
   */
  protected SocketOptions getSocketOptions() {
    SocketOptions socketOptions = new SocketOptions();
    socketOptions.setReadTimeoutMillis(this.readTimeoutMillis);
    return socketOptions;
  }
}
