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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Query against Splunk.
 *
 * @param <T> Element type
 */
public class SplunkQuery<T> extends AbstractEnumerable<T> {
  private final SplunkConnection splunkConnection;
  private final String search;
  private final @Nullable String earliest;
  private final @Nullable String latest;
  private final @Nullable List<String> fieldList;

  /** Creates a SplunkQuery. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      @Nullable String earliest,
      @Nullable String latest,
      @Nullable List<String> fieldList) {
    this.splunkConnection =
        requireNonNull(splunkConnection, "splunkConnection");
    this.search = requireNonNull(search, "search");
    this.earliest = earliest;
    this.latest = latest;
    this.fieldList = fieldList;
  }

  @Override public String toString() {
    return "SplunkQuery {" + search + "}";
  }

  @Override public Enumerator<T> enumerator() {
    //noinspection unchecked
    return (Enumerator<T>) splunkConnection.getSearchResultEnumerator(
        search, getArgs(), fieldList);
  }

  private Map<String, String> getArgs() {
    Map<String, String> args = new HashMap<>();
    if (fieldList != null) {
      String fields =
          StringUtils.encodeList(fieldList, ',').toString();
      args.put("field_list", fields);
    }
    if (earliest != null) {
      args.put("earliest_time", earliest);
    }
    if (latest != null) {
      args.put("latest_time", latest);
    }
    return args;
  }
}
