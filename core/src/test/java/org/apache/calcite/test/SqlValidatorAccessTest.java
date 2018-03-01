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

import org.apache.calcite.access.Authorization;
import org.apache.calcite.access.AuthorizationRequest;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlAccessEnum;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.atLeastOnce;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;
import static org.mockito.BDDMockito.verify;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing tests for validating access
 */
public class SqlValidatorAccessTest {

  private final Authorization hrGuardMock = mock(Authorization.class);
  private final Authorization wrapperGuardMock = mock(Authorization.class);
  private final ArgumentCaptor<AuthorizationRequest> hrRequestCaptor
          = forClass(AuthorizationRequest.class);

  private String user;

  @Before public void init() {
    Mockito.reset(hrGuardMock, wrapperGuardMock);
  }

  @Test public void testShouldWorkWithoutUser() {
    // given
    givenIsUser(null);
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    // when
    Then then = when("select * from HR.DEPTS");
    // then
    then.executedProperly();
    verify(hrGuardMock).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  @Test public void testShouldWorkWithUserAndAccessGranted() {
    // given
    givenIsUser("someuser");
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    // when
    Then then = when("select * from HR.DEPTS");
    // then
    then.executedProperly();
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
    verify(hrGuardMock).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  @Test public void testShouldNotWorkWithUserButForbiddenAccess() {
    // given
    givenIsUser("someuser");
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(false);
    // when
    Then then = when("select * from HR.DEPTS");
    // then
    then.exceptionInStack("Not allowed to perform SELECT on \\[HR, depts\\]");
    verify(hrGuardMock).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  @Test public void testShouldWorkWithoutUserOnWrapper() {
    // given
    givenIsUser(null);
    given(wrapperGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    // when
    Then then = when("select * from WRAPPER.WRAPPEDDEPTS");
    // then
    then.executedProperly();
    verify(hrGuardMock, atLeastOnce()).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  @Test public void testShouldWorkWithUserAndAccessGrantedOnWrapper() {
    // given
    givenIsUser("someuser");
    given(wrapperGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    // when
    Then then = when("select * from WRAPPER.WRAPPEDDEPTS");
    // then
    then.executedProperly();
    verify(hrGuardMock, atLeastOnce()).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  @Test public void testShouldNotWorkWithUserButForbiddenAccessOnWrapper() {
    // given
    givenIsUser("someuser");
    given(wrapperGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(true);
    given(hrGuardMock.accessGranted(any(AuthorizationRequest.class))).willReturn(false);
    // when
    Then then = when("select * from WRAPPER.WRAPPEDDEPTS");
    // then
    then.exceptionInStack("Not allowed to perform INDIRECT_SELECT on \\[HR, depts\\]");
    verify(hrGuardMock, atLeastOnce()).accessGranted(hrRequestCaptor.capture());
    Assert.assertEquals("Should be select requested",
            hrRequestCaptor.getValue().getRequiredAccess(),
            SqlAccessEnum.SELECT);
  }

  private Then when(String query) {
    try {
      ResultSet rs = createConnection().prepareStatement(query).executeQuery();
      return new Then(rs, null);
    } catch (SQLException ex) {
      return new Then(null, ex);
    }
  }

  private void givenIsUser(String username) {
    this.user = username;
  }

  /**
   * Syntactic sugar for more descriptive and fluent tests
   */
  private static class Then {

    private final ResultSet rs;
    private final SQLException exception;

    private Then(ResultSet rs, SQLException exception) {
      this.rs = rs;
      this.exception = exception;
    }

    private void executedProperly() {
      Assert.assertTrue("Execution should be ok", rs != null && exception == null);
    }

    private void exceptionInStack(String pattern) {
      Throwable t = exception;
      while (t != null) {
        if (Pattern.matches(pattern, t.getMessage())) {
          return;
        }
        t = t.getCause();
      }
      Assert.fail("No desired exception on stack");
    }
  }

  private Connection createConnection() throws SQLException {
    final Properties info = new Properties();
    info.setProperty("lex", "MYSQL");
    info.setProperty("caseSensitive", "false");
    if (isNotBlank(user)) {
      info.setProperty("user", user);
    }
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection con = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = con.getRootSchema();
    SchemaPlus hrSchema = rootSchema.add("HR", new ReflectiveSchema(new JdbcTest.HrSchema()));
    hrSchema.setAuthorization(hrGuardMock);
    SchemaPlus wrapperSchema = rootSchema.add("WRAPPER", new AbstractSchema());
    wrapperSchema.setAuthorization(wrapperGuardMock);
    wrapperSchema.add(
            "WRAPPEDDEPTS",
            ViewTable.viewMacro(wrapperSchema, "select * from HR.DEPTS",
                    ImmutableList.<String>of(), ImmutableList.of("WRAPPER", "WRAPPEDDEPTS"),
                    null));
    return connection;
  }

  private void givenIsUser(Object object) {
    this.user = user;
  }

}

// End SqlValidatorAccessTest.java
