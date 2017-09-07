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

import org.hsqldb.jdbcDriver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for sequence support.
 */
public class SequenceTest {

  @Test public void testBigint() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE SEQUENCE S1 AS BIGINT;");

    baseStmt.close();
    baseConnection.commit();

    Connection calciteConnection = sequenceModelConnection(hsqldbMemUrl);
    PreparedStatement nextValuePs =
            calciteConnection.prepareStatement("select next value for s1");
    PreparedStatement currentValuePs =
            calciteConnection.prepareStatement("select current value for s1");
    ResultSet rs;

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(0L));

    rs = nextValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(1L));

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(1L));

    rs = nextValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(2L));

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(2L));

    rs.close();
    calciteConnection.close();
  }

  @Test public void testInt() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE SEQUENCE S2;");

    baseStmt.close();
    baseConnection.commit();

    Connection calciteConnection = sequenceModelConnection(hsqldbMemUrl);
    PreparedStatement nextValuePs =
            calciteConnection.prepareStatement("select next value for s2");
    PreparedStatement currentValuePs =
            calciteConnection.prepareStatement("select current value for s2");
    ResultSet rs;

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject(1), equalTo(0));

    rs = nextValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject(1), equalTo(1));

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject(1), equalTo(1));

    rs = nextValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject(1), equalTo(2));

    rs = currentValuePs.executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject(1), equalTo(2));

    rs.close();
    calciteConnection.close();
  }

  private Connection sequenceModelConnection(String hsqldbMemUrl) throws Exception {
    Properties info = new Properties();
    info.put("model",
            "inline:"
                    + "{\n"
                    + "  version: '1.0',\n"
                    + "  defaultSchema: 'BASEJDBC',\n"
                    + "  schemas: [\n"
                    + "     {\n"
                    + "       type: 'jdbc',\n"
                    + "       name: 'BASEJDBC',\n"
                    + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
                    + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
                    + "       jdbcCatalog: null,\n"
                    + "       jdbcSchema: null\n"
                    + "     }\n"
                    + "  ]\n"
                    + "}");

    return DriverManager.getConnection(
            "jdbc:calcite:", info);

  }

}

// End SequenceTest.java
