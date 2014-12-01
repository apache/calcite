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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlUtil;

import java.util.List;

/**
 * Relational expression that unnests its input's sole column into a
 * relation.
 *
 * <p>Like its inverse operation {@link Collect}, Uncollect is generally
 * invoked in a nested loop, driven by {@link Correlator} or similar.
 */
public class Uncollect extends SingleRel {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an Uncollect.
   *
   * <p>The row type of the child relational expression must contain precisely
   * one column, that column must be a multiset of records.
   *
   * @param cluster Cluster the relational expression belongs to
   * @param traitSet Traits
   * @param child   Child relational expression
   */
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    super(cluster, traitSet, child);
    assert deriveRowType() != null : "invalid child rowtype";
  }

  /**
   * Creates an Uncollect by parsing serialized output.
   */
  public Uncollect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Uncollect(getCluster(), traitSet, input);
  }

  protected RelDataType deriveRowType() {
    return deriveUncollectRowType(getInput());
  }

  /**
   * Returns the row type returned by applying the 'UNNEST' operation to a
   * relational expression. The relational expression must have precisely one
   * column, whose type must be a multiset of structs. The return type is the
   * type of that column.
   */
  public static RelDataType deriveUncollectRowType(RelNode rel) {
    RelDataType inputType = rel.getRowType();
    assert inputType.isStruct() : inputType + " is not a struct";
    final List<RelDataTypeField> fields = inputType.getFieldList();
    assert 1 == fields.size() : "expected 1 field";
    RelDataType ret = fields.get(0).getType().getComponentType();
    assert null != ret;
    if (!ret.isStruct()) {
      // Element type is not a record. It may be a scalar type, say
      // "INTEGER". Wrap it in a struct type.
      ret =
          rel.getCluster().getTypeFactory().builder()
              .add(SqlUtil.deriveAliasFromOrdinal(0), ret)
              .build();
    }
    return ret;
  }
}

// End Uncollect.java
