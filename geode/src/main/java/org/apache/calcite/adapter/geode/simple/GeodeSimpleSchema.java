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
package org.apache.calcite.adapter.geode.simple;

import org.apache.calcite.adapter.geode.util.GeodeUtils;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.autodetectRelTypeFromRegion;

/**
 * Geode Simple Schema.
 */
public class GeodeSimpleSchema extends AbstractSchema {

  private String locatorHost;
  private int locatorPort;
  private String[] regionNames;
  private String pdxAutoSerializerPackageExp;
  private ClientCache clientCache;
  private ImmutableMap<String, Table> tableMap;

  public GeodeSimpleSchema(
      String locatorHost, int locatorPort,
      String[] regionNames, String pdxAutoSerializerPackageExp) {
    super();
    this.locatorHost = locatorHost;
    this.locatorPort = locatorPort;
    this.regionNames = regionNames;
    this.pdxAutoSerializerPackageExp = pdxAutoSerializerPackageExp;

    this.clientCache = GeodeUtils.createClientCache(
        locatorHost,
        locatorPort,
        pdxAutoSerializerPackageExp,
        true);
  }

  @Override protected Map<String, Table> getTableMap() {

    if (tableMap == null) {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      for (String regionName : regionNames) {

        Region region = GeodeUtils.createRegion(clientCache, regionName);

        Table table = new GeodeSimpleScannableTable(regionName, autodetectRelTypeFromRegion(region),
            clientCache);

        builder.put(regionName, table);
      }

      tableMap = builder.build();
    }
    return tableMap;
  }
}

// End GeodeSimpleSchema.java
