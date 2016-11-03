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

import org.apache.calcite.linq4j.Enumerator;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.util.Iterator;

/**
 * Geode Simple Enumerator.
 *
 * @param <E> Element type
 */
public abstract class GeodeSimpleEnumerator<E> implements Enumerator<E> {

  private Iterator results;

  private E current;
  private ClientCache clientCache;

  public GeodeSimpleEnumerator(ClientCache clientCache, String regionName) {
    this.clientCache = clientCache;
    QueryService queryService = clientCache.getQueryService();
    String oql = "select * from /" + regionName.trim();
    try {
      results = ((SelectResults) queryService.newQuery(oql).execute()).iterator();
    } catch (Exception e) {
      e.printStackTrace();
      results = null;
    }
  }

  @Override public E current() {
    return current;
  }

  @Override public boolean moveNext() {

    if (results.hasNext()) {
      current = convert(results.next());
      return true;
    }
    current = null;
    return false;
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    /*clientCache.close(); */
  }

  public abstract E convert(Object obj);
}

// End GeodeSimpleEnumerator.java
