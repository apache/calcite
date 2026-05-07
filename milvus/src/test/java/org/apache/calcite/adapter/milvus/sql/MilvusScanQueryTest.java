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
package org.apache.calcite.adapter.milvus.sql;

import org.apache.calcite.adapter.milvus.MilvusBaseE2ETest;
import org.apache.calcite.adapter.milvus.extension.MilvusExtension;
import org.apache.calcite.adapter.milvus.util.TestEnvUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Milvus scan query functionality.
 */
@ExtendWith(MilvusExtension.class)
public class MilvusScanQueryTest extends MilvusBaseE2ETest {
  private static final String COLLECTION_NAME = "MilvusScanQueryTest";
  private Connection connection;

  @BeforeAll
  static void setupOnce() {
    TestEnvUtil testEnvUtil =
        new TestEnvUtil(COLLECTION_NAME, getMilvusServiceClientV1());
    testEnvUtil.createExampleCollection();
  }

  @BeforeEach
  public void setUp() throws Exception {
    connection = setupCalciteConnection();
  }

  @Test public void testStringConstant() throws SQLException {
    List<String> expected =
        Lists.newArrayList(
            "ABriefHistoryOfTime,Time is a mysterious phenomenon that is everywhere"
                + " yet hard to grasp.,[0.2, 0.4, 0.6, 0.8]",
            "ADreamOfRedMansions,A Dream of Red Mansions is a great work depicting the rise and fall of feudal society.,[0.7, 1.4, 2.1, 2.8]",
            "FortressBesieged,Those inside the fortress want to get out, while those outside want to get in. This is the paradox of life.,[0.5, 1.0, 1.5, 2.0]",
            "NorwegianWood,Norwegian Wood is filled with the confusion and hesitation of youth.,[0.90000004, 1.8000001, 2.7, 3.6000001]",
            "OneHundredYearsOfSolitude,Macondo is a small town full of magical colors, where many incredible stories happened.,[0.3, 0.6, 0.90000004, 1.2]",
            "OrdinaryWorld,Although life is ordinary, everyone has their own dreams and pursuits.,[0.6, 1.2, 1.8000001, 2.4]",
            "TheKiteRunner,The Kite Runner tells a moving story about friendship and redemption.,[1.0, 2.0, 3.0, 4.0]",
            "TheLittlePrince,Once upon a time there was a little prince who lived on a very small planet, where there was a rose he cherished very much.,[0.1, 0.2, 0.3, 0.4]",
            "ThreeBody,The arrival of the Trisolaran civilization changed humanity's understanding of the universe.,[0.8, 1.6, 2.4, 3.2]",
            "ToLive,Life is like a play, and we are all actors in it, experiencing joy and sorrow.,[0.4, 0.8, 1.2, 1.6]");
    String sql = "select * from milvus." + COLLECTION_NAME + " ";
    String executionPlan = getExecutionPlan(sql, connection);
    assertTrue(containsMilvusOperator(executionPlan, MILVUS_SCAN));
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> actual = new ArrayList<>();
      while (resultSet.next()) {
        String bookName = resultSet.getString(1);
        String distance = resultSet.getString(2);
        String vector = resultSet.getString(3);
        actual.add(bookName + "," + distance + "," + vector);
      }
      Assertions.assertEquals(expected, actual);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }
}
