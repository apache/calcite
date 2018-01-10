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
package org.apache.calcite.adapter.druid;

import org.joda.time.Interval;

import java.util.List;

/**
 * dummy scan query builder
 */
public class ScanQueryBuilder {
  private String dataSource;

  private List<Interval> intervals;

  private DruidJsonFilter jsonFilter;

  private List<VirtualColumn> virtualColumnList;

  private List<String> columns;

  private Integer fetchLimit;

  public ScanQueryBuilder setDataSource(String dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  public ScanQueryBuilder setIntervals(List<Interval> intervals) {
    this.intervals = intervals;
    return this;
  }

  public ScanQueryBuilder setJsonFilter(DruidJsonFilter jsonFilter) {
    this.jsonFilter = jsonFilter;
    return this;
  }

  public ScanQueryBuilder setVirtualColumnList(List<VirtualColumn> virtualColumnList) {
    this.virtualColumnList = virtualColumnList;
    return this;
  }

  public ScanQueryBuilder setColumns(List<String> columns) {
    this.columns = columns;
    return this;
  }

  public ScanQueryBuilder setFetchLimit(Integer fetchLimit) {
    this.fetchLimit = fetchLimit;
    return this;
  }

  public DruidQuery.ScanQuery createScanQuery() {
    return new DruidQuery.ScanQuery(dataSource, intervals, jsonFilter, virtualColumnList, columns,
        fetchLimit
    );
  }
}

// End ScanQueryBuilder.java
