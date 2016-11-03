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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

/**
 *
 */
public class BookMasterRegionTest {

  private BookMasterRegionTest() {
  }

  public static void main(String[] args) throws Exception {

    ClientCache clientCache = new ClientCacheFactory()
        .addPoolLocator("localhost", 10334)
        .setPdxSerializer(new ReflectionBasedAutoSerializer("org.apache.calcite.adapter.geode.*"))
        .create();

    // Using Key/Value
    Region bookMaster = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("BookMaster");

    System.out.println("BookMaster = " + bookMaster.get(789));

    // Using OQL
    QueryService queryService = clientCache.getQueryService();
    String oql = "select itemNumber, description, retailCost from /BookMaster";
    SelectResults result = (SelectResults) queryService.newQuery(oql).execute();
    System.out.println(result.asList());
  }
}

// End BookMasterRegionTest.java
