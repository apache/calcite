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

import org.apache.calcite.adapter.geode.util.GeodeUtils;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.trace.CalciteTrace;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;
import static org.apache.calcite.adapter.geode.util.GeodeUtils.createRelDataType;

/**
 * Schema mapped onto a Geode Region.
 */
public class GeodeSchema extends AbstractSchema {

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  final ClientCache clientCache;
  private final SchemaPlus parentSchema;
  private String[] regionNames;
  private ImmutableMap<String, Table> tableMap;

  public GeodeSchema(String locatorHost, int locatorPort,
      String[] regionNames, String pdxAutoSerializerPackageExp,
      SchemaPlus parentSchema) {
    super();
    this.regionNames = regionNames;
    this.parentSchema = parentSchema;

    this.clientCache = createClientCache(locatorHost, locatorPort,
        pdxAutoSerializerPackageExp, true);
  }

  @Override protected Map<String, Table> getTableMap() {

    if (tableMap == null) {

      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      // Extract the first entity of each Regions and use it to build a table types
      for (String regionName : regionNames) {
        Region region = GeodeUtils.createRegionProxy(clientCache, regionName);

        Iterator regionIterator = region.keySetOnServer().iterator();

        Object firstRegionEntry = region.get(regionIterator.next());
        // TODO: how to handle empty Regions? JMX?
        Table table = new GeodeTable(this, regionName, createRelDataType(firstRegionEntry),
            clientCache);

        builder.put(regionName, table);
      }

      tableMap = builder.build();
    }

    return tableMap;
  }
}

// End GeodeSchema.java
