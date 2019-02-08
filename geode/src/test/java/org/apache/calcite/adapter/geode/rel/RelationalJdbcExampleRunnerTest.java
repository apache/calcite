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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonRoot;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.jndi.InitialContextFactoryImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import javax.naming.InitialContext;

/**
 * Test to assert JsonRoot of calcite connection for RelationalJdbcExampleRunner
 */
public class RelationalJdbcExampleRunnerTest {

  private RelationalJdbcExampleRunner exampleRunner;
  private ObjectMapper mapper;
  private static final String REGION_NAMES = "BookMaster,BookCustomer,BookInventory,BookOrder";
  @Before
  public void testSetUp() throws Exception {
    exampleRunner = Mockito.spy(new RelationalJdbcExampleRunner());
    mapper = new YAMLMapper();

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.next()).thenReturn(false);

    Statement statement = Mockito.mock(Statement.class);
    Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);

    Connection connection = Mockito.mock(Connection.class);
    Mockito.when(connection.createStatement()).thenReturn(statement);

    Mockito.doReturn(connection).when(exampleRunner).getCalciteConnection(Mockito.any());
    Mockito.doNothing().when(exampleRunner).closeAll(Mockito.anyList());

    InitialContext context = Mockito.mock(InitialContext.class);
    Mockito.doNothing().when(context).bind(Mockito.anyString(), Mockito.any());

    Mockito.doReturn(context).when(exampleRunner).getContext(Mockito.any());

    GemFireCache gemFireCache = Mockito.mock(GemFireCache.class);
    Mockito.doReturn(gemFireCache).when(exampleRunner).createClientCache();
  }

  @Test public void testExampleWithLocatorHostPort() throws Exception {
    exampleRunner.doExample(0);

    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    Mockito.verify(exampleRunner).getCalciteConnection(propertiesCaptor.capture());
    Properties connProperties = propertiesCaptor.getValue();
    Assert.assertEquals("Connection properties should have only one property model",
        1, connProperties.size());
    String modelJson = (String) connProperties.get("model");

    JsonRoot actualJsonRoot = mapper.readValue(modelJson.substring("inline:".length()).trim(),
        JsonRoot.class);

    JsonRoot expectedJsonRoot = new JsonRoot();
    expectedJsonRoot.version = "1.0";

    JsonCustomSchema jsonCustomSchema = new JsonCustomSchema();
    jsonCustomSchema.name = "TEST";
    jsonCustomSchema.factory = GeodeSchemaFactory.class.getName();

    Map<String, Object> operand = new LinkedHashMap<String, Object>() {{
        put(GeodeSchemaFactory.LOCATOR_HOST, "localhost");
        put(GeodeSchemaFactory.LOCATOR_PORT, "10334");
        put(GeodeSchemaFactory.REGIONS, REGION_NAMES);
        put(GeodeSchemaFactory.PDX_SERIALIZABLE_PACKAGE_PATH,
            "org.apache.calcite.adapter.geode.domain.*");
      }};
    jsonCustomSchema.operand = operand;

    expectedJsonRoot.schemas.add(jsonCustomSchema);

    Assert.assertEquals(mapper.writeValueAsString(expectedJsonRoot),
        mapper.writeValueAsString(actualJsonRoot));
  }

  @Test public void testExampleWithJndiClientCacheObject() throws Exception {
    exampleRunner.doExample(1);

    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    Mockito.verify(exampleRunner).getCalciteConnection(propertiesCaptor.capture());
    Properties connProperties = propertiesCaptor.getValue();
    Assert.assertEquals("Connection properties should have only one property model",
        1, connProperties.size());
    String modelJson = (String) connProperties.get("model");

    JsonRoot actualJsonRoot = mapper.readValue(modelJson.substring("inline:".length()).trim(),
        JsonRoot.class);

    JsonRoot expectedJsonRoot = new JsonRoot();
    expectedJsonRoot.version = "1.0";

    JsonCustomSchema jsonCustomSchema = new JsonCustomSchema();
    jsonCustomSchema.name = "TEST";
    jsonCustomSchema.factory = GeodeSchemaFactory.class.getName();

    Map<String, Object> operand = new LinkedHashMap<String, Object>() {{
        put(GeodeSchemaFactory.JNDI_INITIAL_CONTEXT_FACTORY,
            InitialContextFactoryImpl.class.getName());
        put(GeodeSchemaFactory.JNDI_CLIENT_CACHE_OBJECT_KEY, "testClientCacheObject");
        put(GeodeSchemaFactory.REGIONS, REGION_NAMES);
      }};
    jsonCustomSchema.operand = operand;

    expectedJsonRoot.schemas.add(jsonCustomSchema);

    Assert.assertEquals(mapper.writeValueAsString(expectedJsonRoot),
        mapper.writeValueAsString(actualJsonRoot));
  }

}

// End RelationalJdbcExampleRunnerTest.java
