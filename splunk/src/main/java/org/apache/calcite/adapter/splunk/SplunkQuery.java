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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Query against Splunk.
 *
 * @param <T> Element type
 */
public class SplunkQuery<T> extends AbstractEnumerable<T> {
  private final SplunkConnection splunkConnection;
  private final String search;
  private final String earliest;
  private final String latest;
  private final List<String> fieldList;
  private final Set<String> explicitFields;

  /** Creates a SplunkQuery. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList) {
    this(splunkConnection, search, earliest, latest, fieldList, Collections.emptySet());
  }

  /** Creates a SplunkQuery with explicit field information. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields) {
    this.splunkConnection = splunkConnection;
    this.search = search;
    this.earliest = earliest;
    this.latest = latest;
    this.fieldList = fieldList;
    this.explicitFields = explicitFields;
  }

  @Override public String toString() {
    return "SplunkQuery {" + search + "}";
  }

  @SuppressWarnings("unchecked")
  @Override public Enumerator<T> enumerator() {
    // Always use the 4-parameter method - explicitFields determines _extra field behavior:
    // - Empty explicitFields: All Splunk fields go to _extra
    // - Populated explicitFields: Only non-explicit fields go to _extra
    return (Enumerator<T>) splunkConnection.getSearchResultEnumerator(
        search, getArgs(), fieldList, explicitFields);
  }

  private Map<String, String> getArgs() {
    Map<String, String> args = new HashMap<>();
    String fields =
        StringUtils.encodeList(fieldList, ',').toString();
    args.put("field_list", fields);
    args.put("earliest_time", earliest);
    args.put("latest_time", latest);
    return args;
  }
}
