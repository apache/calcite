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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@code AvaticaConnection} relative to close behavior
 */
@RunWith(Parameterized.class)
public class AvaticaClosedConnectionTest extends AvaticaClosedTestBase<Connection> {

  /**
   * Verifier for clientinfo methods
   */
  public static final MethodVerifier ASSERT_CLIENTINFO = invocation -> {
    try {
      invocation.invoke();
      fail();
    } catch (SQLClientInfoException e) {
      // success
    }
  };


  private static final Predicate<? super Method> METHOD_FILTER = method -> {
    final String name = method.getName();
    switch (name) {
    case "close":
    case "isClosed":
    case "isValid":
      return false;

    default:
      return true;
    }
  };

  // Mapping between Connection method and the verifier to check close behavior
  private static final Function<Method, MethodVerifier> METHOD_MAPPING = method -> {
    String name = method.getName();
    switch (name) {
    case "setClientInfo":
      return ASSERT_CLIENTINFO;

    // Those methods are not supported yet
    case "abort":
    case "createBlob":
    case "createClob":
    case "createNClob":
    case "createSQLXML":
    case "createStruct":
    case "getTypeMap":
    case "nativeSQL":
    case "prepareCall":
    case "releaseSavepoint":
    case "setSavepoint":
    case "setTypeMap":
      return ASSERT_UNSUPPORTED;

    // prepareStatement is partially supported
    case "prepareStatement":
      Object[] prepareStatementArguments = method.getParameterTypes();
      if (Arrays.equals(prepareStatementArguments, new Object[] { String.class, int.class })
          || Arrays.equals(prepareStatementArguments, new Object[] { String.class, int[].class })
          || Arrays.equals(prepareStatementArguments,
              new Object[] { String.class, String[].class })) {
        return ASSERT_UNSUPPORTED;
      }
      return ASSERT_CLOSED;

    // rollback is partially supported
    case "rollback":
      if (method.getParameterCount() > 0) {
        return ASSERT_UNSUPPORTED;
      }
      return ASSERT_CLOSED;

    default:
      return ASSERT_CLOSED;
    }
  };

  @Parameters(name = "{index}: {0}")
  public static Iterable<? extends Object[]> getParameters() {
    return getMethodsToTest(Connection.class, AvaticaConnection.class, METHOD_FILTER,
        METHOD_MAPPING);
  }

  public AvaticaClosedConnectionTest(Method method, MethodVerifier verifier) {
    super(method, verifier);
  }

  @Override protected Connection newInstance() throws Exception {
    UnregisteredDriver driver = new TestDriver();
    AvaticaConnection connection = new AvaticaConnection(driver, driver.createFactory(),
        "jdbc:avatica", new Properties()) {
    };
    connection.close();
    assertTrue("Connection is not closed", connection.isClosed());
    return connection;
  }
}

// End AvaticaClosedConnectionTest.java
