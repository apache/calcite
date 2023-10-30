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

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Map;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;

/**
 * Factory that creates a {@link GeodeSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class GeodeSchemaFactory implements SchemaFactory {

  public static final String LOCATOR_HOST = "locatorHost";
  public static final String LOCATOR_PORT = "locatorPort";
  public static final String REGIONS = "regions";
  public static final String PDX_SERIALIZABLE_PACKAGE_PATH = "pdxSerializablePackagePath";
  public static final String ALLOW_SPATIAL_FUNCTIONS = "spatialFunction";
  public static final String COMMA_DELIMITER = ",";

  public GeodeSchemaFactory() {
    // Do Nothing
  }

  @Override public synchronized Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    Map map = (Map) operand;
    String locatorHost = (String) map.get(LOCATOR_HOST);
    int locatorPort = Integer.valueOf((String) map.get(LOCATOR_PORT));
    String[] regionNames = ((String) map.get(REGIONS)).split(COMMA_DELIMITER);
    String pbxSerializablePackagePath = (String) map.get(PDX_SERIALIZABLE_PACKAGE_PATH);

    boolean allowSpatialFunctions = true;
    if (map.containsKey(ALLOW_SPATIAL_FUNCTIONS)) {
      allowSpatialFunctions = Boolean.valueOf((String) map.get(ALLOW_SPATIAL_FUNCTIONS));
    }

    if (allowSpatialFunctions) {
      ModelHandler.addFunctions(parentSchema, null, ImmutableList.of(),
          SpatialTypeFunctions.class.getName(), "*", true);
    }

    return new GeodeSchema(
        createClientCache(locatorHost, locatorPort, pbxSerializablePackagePath, true),
        Arrays.asList(regionNames));
  }
}
