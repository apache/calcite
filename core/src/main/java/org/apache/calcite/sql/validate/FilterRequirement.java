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
package org.apache.calcite.sql.validate;

import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Class that encapsulates filtering requirements when overloading SemanticTable. <br>
 *
 * <p>A few examples of the behavior:<br>
 *
 * <p>Table <code>t</code> has a must-filter field <code>f</code> and bypass-fields <code>b0</code>
 * and <code>b1</code>.<br>
 * SQL: <code>select f from t;</code><br> -> Errors because there's no filter on f. <br>
 * SQL: <code>select * from (select f from t);</code><br> -> Errors at the inner query because
 * there's no filter on f. <br>
 * SQL: <code>select f from t where f = 1;</code><br> -> Valid because there is a filter on f.<br>
 * SQL: <code>select * from (select f from t) where f = 1;</code><br> -> Valid because there is a
 * filter on f. <br>
 * SQL: <code>select f from t where b0 = 1;</code><br> -> Valid because there is a filter on
 * bypass-field b0.<br>
 *
 * <p>Some notes on remnantFilterFields.<br>
 * remnantFilterFields is used to identify whether the query should error
 * at the top level query. It is populated with the filter-field value when a filter-field is not
 * selected or filtered on, but a bypass-field for the table is selected.
 * The remnantFilterFields are no longer accessible by the enclosing query and hence can no
 * longer be defused by filtering on it; however, it can be defused if the bypass-field is
 * filtered on, hence we need to keep track of it.

 * Example:<br>
 * Table <code>t</code> has a must-filter field <code>f</code> and bypass-fields <code>b0</code>
 *  and <code>b1</code>.<br>
 * SQL: <code>select b0, b1 from t;</code><br>
 *
 * <p>This results in: <br>
 * filterFields:[]<br>
 * bypassFields:[b0, b1]<br>
 * remnantFilterFields: [f]<br>
 * -> Errors because it is a top level query and remnantFilterFields is not empty. <br>
 *
 * <p>SQL: <code>select * from (select b0, b1 from t) where b0 = 1;</code><br>
 * When unwrapping the inner query we get the same FilterRequirement as the previous example:<br>
 * filterFields:[]<br>
 * bypassFields:[b0, b1]<br>
 * remnantFilterFields: [f]<br>
 * When unwrapping the top level query, the filter on b0 defuses the remnantFilterField requirement
 * of [f] because it originated from the same table, resulting in the following: <br>
 * filterFields:[]<br>
 * bypassFields:[b0, b1]<br>
 * remnantFilterFields: []<br>
 * -> Valid because remnantFilterFields is empty now.
 */
final class FilterRequirement {

  /** The ordinals (in the row type) of the "must-filter" fields,
   * fields that must be filtered in a query.*/
  private final ImmutableBitSet filterFields;
  /** The ordinals (in the row type) of the "bypass" fields,
   * fields that can defuse validation errors on filterFields if filtered on. */
  private final ImmutableBitSet bypassFields;
  /** Set of filterField SqlQualifieds that have not been defused
   * in the current query, but can still be defused by filtering on a bypass field in the
   * enclosing query.*/
  private final ImmutableSet<SqlQualified> remnantFilterFields;

  /**
   * Creates a <code>FilterRequirement</code>.
   *
   * @param filterFields Ordinals of the "must-filter" fields.
   * @param bypassFields Ordinals of the "bypass" fields.
   * @param remnantFilterFields Filter fields that can no longer be filtered on,
   * but can only be defused if a bypass field is filtered on.
   */
  FilterRequirement(ImmutableBitSet filterFields,
      ImmutableBitSet bypassFields, Set<SqlQualified> remnantFilterFields) {
    this.filterFields = ImmutableBitSet.of(filterFields);
    this.bypassFields = ImmutableBitSet.of(bypassFields);
    this.remnantFilterFields = ImmutableSet.copyOf(remnantFilterFields);
  }

  /** Creates an empty FilterRequirement. */
  FilterRequirement() {
    this(ImmutableBitSet.of(), ImmutableBitSet.of(), ImmutableSet.of());
  }
  /** Returns filterFields. */
  public ImmutableBitSet getFilterFields() {
    return filterFields;
  }

  /** Returns bypassFields. */
  public ImmutableBitSet getBypassFields() {
    return bypassFields;
  }

  /** Returns remnantFilterFields. */
  public ImmutableSet<SqlQualified> getRemnantFilterFields() {
    return remnantFilterFields;
  }
}
