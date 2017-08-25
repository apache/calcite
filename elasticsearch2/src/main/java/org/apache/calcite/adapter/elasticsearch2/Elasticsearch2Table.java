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
package org.apache.calcite.adapter.elasticsearch2;

import org.apache.calcite.adapter.elasticsearch.AbstractElasticsearchTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;

import org.apache.calcite.util.Util;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table based on an Elasticsearch2 type.
 */
public class Elasticsearch2Table extends AbstractElasticsearchTable {
  private final Client client;

  /**
   * Creates an Elasticsearch2Table.
   */
  public Elasticsearch2Table(Client client, String indexName, String typeName) {
    super(indexName, typeName);
    this.client = client;
  }

  @Override protected Enumerable<Object> find(String index, List<String> ops,
      List<Map.Entry<String, Class>> fields) {
    final String dbName = index;

    final String queryString = "{" + Util.toString(ops, "", ", ", "") + "}";

    final Function1<SearchHit, Object> getter = Elasticsearch2Enumerator.getter(fields);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final Iterator<SearchHit> cursor = client.prepareSearch(dbName).setTypes(typeName)
            .setSource(queryString).execute().actionGet().getHits().iterator();
        return new Elasticsearch2Enumerator(cursor, getter);
      }
    };
  }
}

// End Elasticsearch2Table.java
