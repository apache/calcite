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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

/**
 * A relational expression that collapses multiple rows into one.
 *
 * <p>Rules:</p>
 *
 * <ul>
 * <li>{@code net.sf.farrago.fennel.rel.FarragoMultisetSplitterRule}
 * creates a Collect from a call to
 * {@link org.apache.calcite.sql.fun.SqlMultisetValueConstructor} or to
 * {@link org.apache.calcite.sql.fun.SqlMultisetQueryConstructor}.</li>
 * </ul>
 */
public class Collect extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final String fieldName;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Collect.
   *
   * @param cluster   Cluster
   * @param child     Child relational expression
   * @param fieldName Name of the sole output field
   */
  public Collect(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      String fieldName) {
    super(cluster, traitSet, child);
    this.fieldName = fieldName;
  }

  /**
   * Creates a Collect by parsing serialized output.
   */
  public Collect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getString("field"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Collect(getCluster(), traitSet, input, fieldName);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("field", fieldName);
  }

  /**
   * Returns the name of the sole output field.
   *
   * @return name of the sole output field
   */
  public String getFieldName() {
    return fieldName;
  }

  @Override protected RelDataType deriveRowType() {
    return deriveCollectRowType(this, fieldName);
  }

  /**
   * Derives the output type of a collect relational expression.
   *
   * @param rel       relational expression
   * @param fieldName name of sole output field
   * @return output type of a collect relational expression
   */
  public static RelDataType deriveCollectRowType(
      SingleRel rel,
      String fieldName) {
    RelDataType childType = rel.getInput().getRowType();
    assert childType.isStruct();
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    RelDataType ret =
        SqlTypeUtil.createMultisetType(
            typeFactory,
            childType,
            false);
    ret = typeFactory.builder().add(fieldName, ret).build();
    return typeFactory.createTypeWithNullability(ret, false);
  }
}

// End Collect.java
