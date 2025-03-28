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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Relational expression that returns the intersection of the rows of its
 * inputs.
 *
 * <p>If "all" is true, performs then multiset intersection; otherwise,
 * performs set set intersection (implying no duplicates in the results).
 */
public abstract class Intersect extends SetOp {
  /**
   * Creates an Intersect.
   */
  public Intersect(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, hints, inputs, SqlKind.INTERSECT, all);
  }

  /**
   * Creates an Intersect.
   */
  protected Intersect(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    this(cluster, traits, Collections.emptyList(), inputs, all);
  }

  /**
   * Creates an Intersect by parsing serialized output.
   */
  protected Intersect(RelInput input) {
    super(input);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
    double dRows = Double.MAX_VALUE;
    for (RelNode input : inputs) {
      Double rowCount = mq.getRowCount(input);
      if (rowCount == null) {
        // Assume this input does not reduce row count
        continue;
      }
      dRows = Math.min(dRows, rowCount);
    }
    dRows *= 0.25;
    return dRows;
  }

  @Override protected RelDataType deriveRowType() {
    final RelDataType leastRestrictiveRowType = deriveLeastRestrictiveRowType();
    List<RelDataTypeField> outputFields = leastRestrictiveRowType.getFieldList();

    List<List<RelDataTypeField>> inputs = getInputs()
        .stream()
        .map(i -> i.getRowType().getFieldList())
        .collect(Collectors.toList());

    // The leastRestrictiveRowType can potentially have columns marked as nullable that do not need
    // to be so. An output column is only nullable if it is nullable in ALL the inputs.
    final RelDataTypeFactory.Builder typeBuilder =
        new RelDataTypeFactory.Builder(getCluster().getTypeFactory());
    for (int fieldIndex = 0; fieldIndex < outputFields.size(); fieldIndex++) {
      boolean isNullable = outputFields.get(fieldIndex).getType().isNullable();
      for (List<RelDataTypeField> input : inputs) {
        isNullable &= input.get(fieldIndex).getType().isNullable();
      }

      typeBuilder
          .add(outputFields.get(fieldIndex))
          .nullable(isNullable);
    }

    return typeBuilder.build();
  }
}
