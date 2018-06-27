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
package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Internal constants referenced in this package.
 */
interface ElasticsearchConstants {

  String INDEX = "_index";
  String TYPE = "_type";
  String FIELDS = "fields";
  String SOURCE_PAINLESS = "params._source";
  String SOURCE_GROOVY = "_source";
  String SOURCE = SOURCE_GROOVY;
  String ID = "_id";
  String UID = "_uid";

  /* Aggregation pushdown operations supported */
  String AGG_SUM = "SUM";
  String AGG_SUM0 = "$SUM0";
  String AGG_COUNT = "COUNT";
  String AGG_MIN = "MIN";
  String AGG_MAX = "MAX";
  String AGG_AVG = "AVG";

  Set<String> META_COLUMNS = ImmutableSet.of(UID, ID, TYPE, INDEX);

}

// End ElasticsearchConstants.java
