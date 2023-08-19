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
package org.apache.calcite.plan.rule;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class JoinContext {
  private final JoinRexUtils.JoinRewriteType joinRewriteType;
  private final List<Integer> needCompensateNullColumn;
  private final Pair<List<RexInputRef>, List<RexInputRef>> joinColumnPair;
  private final List<RexInputRef> notNullColumns = new ArrayList<>();
  private final List<RelDataTypeField> inputDataTypeList;
  private final List<RelDataType> targetDataTypeList;

  public JoinContext(JoinRexUtils.JoinRewriteType joinRewriteType,
      List<Integer> needCompensateNullColumn,
      Pair<List<RexInputRef>, List<RexInputRef>> joinColumnPair,
      List<RelDataType> targetDataTypeList, List<RelDataTypeField> inputDataTypeList) {
    this.joinRewriteType = joinRewriteType;
    this.needCompensateNullColumn = needCompensateNullColumn;
    this.joinColumnPair = joinColumnPair;
    this.targetDataTypeList = targetDataTypeList;
    this.inputDataTypeList = inputDataTypeList;
  }

  public JoinRexUtils.JoinRewriteType getJoinRewriteType() {
    return joinRewriteType;
  }

  public List<Integer> getNeedCompensateNullColumn() {
    return needCompensateNullColumn;
  }

  public List<RexInputRef> getLeftJoinColumns() {
    return joinColumnPair.getKey();
  }

  public List<RexInputRef> getRightJoinColumns() {
    return joinColumnPair.getValue();
  }

  public List<RexInputRef> getNotNullColumns() {
    return notNullColumns;
  }

  public List<RelDataTypeField> getInputDataTypeList() {
    return inputDataTypeList;
  }

  public List<RelDataType> getTargetDataTypeList() {
    return targetDataTypeList;
  }
}
