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
package org.apache.calcite.avatica.tck.tests;

import org.apache.calcite.avatica.tck.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.sql.Connection;

/**
 * Base class for TCK tests.
 */
public abstract class BaseTckTest {

  @Rule public TestName name = new TestName();

  protected Connection connection;

  @Before public void initializeConnection() throws Exception {
    connection = TestRunner.getConnection();
  }

  @After public void closeConnection() throws Exception {
    if (null != connection) {
      connection.close();
    }
  }

  protected Connection getConnection() {
    return connection;
  }

  protected String getTableName() {
    return getClass().getSimpleName() + "_" + name.getMethodName();
  }
}

// End BaseTckTest.java
