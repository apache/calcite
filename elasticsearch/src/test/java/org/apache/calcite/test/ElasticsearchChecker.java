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
package org.apache.calcite.test;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Internal util methods for ElasticSearch tests
 */
public class ElasticsearchChecker {

  private ElasticsearchChecker() {}


  /** Returns a function that checks that a particular Elasticsearch pipeline is
   * generated to implement a query.
   * @param strings expected expressions
   * @return validation function
   */
  public static Function<List, Void> elasticsearchChecker(final String... strings) {
    Preconditions.checkNotNull(strings, "strings");
    return new Function<List, Void>() {
      @Nullable
      @Override public Void apply(@Nullable List actual) {
        Object[] actualArray = actual == null || actual.isEmpty() ? null
            : ((List) actual.get(0)).toArray();
        CalciteAssert.assertArrayEqual("expected Elasticsearch query not found", strings,
            actualArray);
        return null;
      }
    };
  }
}

// End ElasticsearchChecker.java
