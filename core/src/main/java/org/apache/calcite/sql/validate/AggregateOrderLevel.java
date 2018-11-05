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

/**
 * A level describes how does an aggregate call handle its aggregate order, which is typically
 * specified in <code>WITHIN GROUP</code> clause).
 *
 * {@link  SqlValidatorImpl#validateAggregateParams}
 */
public enum AggregateOrderLevel {
  /**
   * The aggregate call must have an aggregate order.
   *
   * E.g.
   *
   * Available usage:       MY_AGG_CALL(col) WITHIN GROUP (ORDER BY 1)
   * Usage lead to error:   MY_AGG_CALL(col)
   */
  MANDATORY,
  /**
   * The aggregate call can either include or not include an aggregate order.
   *
   * E.g.
   *
   * Available usage:       MY_AGG_CALL(col) WITHIN GROUP (ORDER BY 1)
   * Available usage:       MY_AGG_CALL(col)
   */
  OPTIONAL,
  /**
   * The aggregate call can either include or not include an aggregate order, however the executor
   * always ignores the ordering spec.
   *
   */
  IGNORED,
  /**
   * The aggregate call cannot include an aggregate order.
   *
   * E.g.
   *
   * Usage lead to error:   MY_AGG_CALL(col) WITHIN GROUP (ORDER BY 1)
   * Available usage:       MY_AGG_CALL(col)
   */
  FORBIDDEN
}

// End AggregateOrderLevel.java
