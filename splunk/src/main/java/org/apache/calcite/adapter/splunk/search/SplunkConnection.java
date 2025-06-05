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
package org.apache.calcite.adapter.splunk.search;

import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Connection to Splunk.
 */
public interface SplunkConnection {
  void getSearchResults(String search, Map<String, String> otherArgs,
      List<String> fieldList, SearchResultListener srl);

  /**
   * Gets search result enumerator with explicit field information for _extra field collection.
   * @param search the search query
   * @param otherArgs additional search arguments
   * @param fieldList list of fields to retrieve
   * @param explicitFields set of fields explicitly defined in the table schema.
   *                      If empty, all Splunk fields will be collected into _extra.
   *                      If populated, only non-explicit fields will go to _extra.
   * @return enumerator for search results
   */
  Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields);
}
