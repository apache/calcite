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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;

import com.google.common.collect.ImmutableList;

/**
 * Strategy to infer the type of an operator call from the type of the operands
 * by using one {@link SqlReturnTypeInference} rule and a combination of
 * {@link SqlTypeTransform}s
 */
public class SqlTypeTransformCascade implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final SqlReturnTypeInference rule;
  private final ImmutableList<SqlTypeTransform> transforms;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlTypeTransformCascade from a rule and an array of one or more
   * transforms.
   */
  public SqlTypeTransformCascade(
      SqlReturnTypeInference rule,
      SqlTypeTransform... transforms) {
    assert rule != null;
    assert transforms.length > 0;
    this.rule = rule;
    this.transforms = ImmutableList.copyOf(transforms);
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType ret = rule.inferReturnType(opBinding);
    if (ret == null) {
      // inferReturnType may return null; transformType does not accept or
      // return null types
      return null;
    }
    for (SqlTypeTransform transform : transforms) {
      ret = transform.transformType(opBinding, ret);
    }
    return ret;
  }
}

// End SqlTypeTransformCascade.java
