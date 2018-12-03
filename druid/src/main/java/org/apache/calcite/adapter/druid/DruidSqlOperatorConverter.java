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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import javax.annotation.Nullable;

/**
 * Defines how to convert RexNode with a given calcite SQL operator to Druid expressions
 */
public interface DruidSqlOperatorConverter {

  /**
   * Returns the calcite SQL operator corresponding to Druid operator.
   *
   * @return operator
   */
  SqlOperator calciteOperator();


  /**
   * Translate rexNode to valid Druid expression.
   * @param rexNode rexNode to translate to Druid expression
   * @param rowType row type associated with rexNode
   * @param druidQuery druid query used to figure out configs/fields related like timeZone
   *
   * @return valid Druid expression or null if it can not convert the rexNode
   */
  @Nullable String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery druidQuery);
}

// End DruidSqlOperatorConverter.java
