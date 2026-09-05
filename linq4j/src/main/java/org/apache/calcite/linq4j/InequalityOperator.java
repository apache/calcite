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
package org.apache.calcite.linq4j;

/** Comparison operator applied to the left and right keys of an inequality
 * join. */
public enum InequalityOperator {
  /** Left key is less than right key. */
  LESS_THAN,

  /** Left key is less than or equal to right key. */
  LESS_THAN_OR_EQUAL,

  /** Left key is greater than right key. */
  GREATER_THAN,

  /** Left key is greater than or equal to right key. */
  GREATER_THAN_OR_EQUAL
}
