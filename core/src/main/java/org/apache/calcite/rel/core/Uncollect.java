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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression that unnests its input's columns into a relation.
 *
 * <p>The input may have multiple columns, but each must be a multiset or
 * array. If {@code withOrdinality}, the output contains an extra
 * {@code ORDINALITY} column.
 *
 * <p>Like its inverse operation {@link Collect}, Uncollect is generally
 * invoked in a nested loop, driven by
 * {@link org.apache.calcite.rel.logical.LogicalCorrelate} or similar.
 */
public class Uncollect extends SingleRel {
  public final boolean withOrdinality;

  // To alias the items in Uncollect list,
  // i.e., "UNNEST(a, b, c) as T(d, e, f)"
  // outputs as row type Record(d, e, f) where the field "d" has element type of "a",
  // field "e" has element type of "b"(Presto dialect).

  // Without the aliases, the expression "UNNEST(a)" outputs row type
  // same with element type of "a".
  private final List<String> itemAliases;

  //~ Constructors -----------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    this(cluster, traitSet, child, false, Collections.emptyList());
  }

  /** Creates an Uncollect.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  @SuppressWarnings("method.invocation.invalid")
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, List<String> itemAliases) {
    super(cluster, traitSet, input);
    this.withOrdinality = withOrdinality;
    this.itemAliases = ImmutableList.copyOf(itemAliases);
    requireNonNull(deriveRowType(), "invalid child rowType");
  }

  /**
   * Creates an Uncollect by parsing serialized output.
   */
  public Uncollect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBoolean("withOrdinality", false), Collections.emptyList());
  }

  /**
   * Creates an Uncollect.
   *
   * <p>Each field of the input relational expression must be an array or
   * multiset.
   *
   * @param traitSet       Trait set
   * @param input          Input relational expression
   * @param withOrdinality Whether output should contain an ORDINALITY column
   * @param itemAliases    Aliases for the operand items
   */
  public static Uncollect create(
      RelTraitSet traitSet,
      RelNode input,
      boolean withOrdinality,
      List<String> itemAliases) {
    final RelOptCluster cluster = input.getCluster();
    return new Uncollect(cluster, traitSet, input, withOrdinality, itemAliases);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("withOrdinality", withOrdinality, withOrdinality);
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Uncollect(getCluster(), traitSet, input, withOrdinality, itemAliases);
  }

  @Override protected RelDataType deriveRowType() {
    return deriveUncollectRowType(input, withOrdinality, itemAliases);
  }

  /**
   * Returns the row type returned by applying the 'UNNEST' operation to a
   * relational expression.
   *
   * <p>Each column in the relational expression must be a multiset of
   * structs or an array. The return type is the combination of expanding
   * element types from each column, plus an ORDINALITY column if {@code
   * withOrdinality}. If {@code itemAliases} is not empty, the element types
   * would not expand, each column element outputs as a whole (the return
   * type has same column types as input type).
   */
  public static RelDataType deriveUncollectRowType(RelNode rel,
      boolean withOrdinality, List<String> itemAliases) {
    RelDataType inputType = rel.getRowType();
    assert inputType.isStruct() : inputType + " is not a struct";

    boolean requireAlias = !itemAliases.isEmpty();
    assert !requireAlias || itemAliases.size() == inputType.getFieldCount();

    final List<RelDataTypeField> fields = inputType.getFieldList();
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();

    if (fields.size() == 1
        && fields.get(0).getType().getSqlTypeName() == SqlTypeName.ANY) {
      // Component type is unknown to Uncollect, build a row type with input column name
      // and Any type.
      return builder
          .add(requireAlias ? itemAliases.get(0) : fields.get(0).getName(), SqlTypeName.ANY)
          .nullable(true)
          .build();
    }

    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      if (field.getType() instanceof MapSqlType) {
        // This code is similar to SqlUnnestOperator::inferReturnType.
        MapSqlType mapType = (MapSqlType) field.getType();
        builder.add(SqlUnnestOperator.MAP_KEY_COLUMN_NAME, mapType.getKeyType());
        builder.add(SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, mapType.getValueType());
      } else {
        RelDataType componentType = field.getType().getComponentType();
        if (null == componentType) {
          throw RESOURCE.unnestArgument().ex();
        }
        boolean isNullable = componentType.isNullable();
        if (requireAlias) {
          builder.add(itemAliases.get(i), componentType);
        } else if (componentType.isStruct()) {
          for (RelDataTypeField fieldInfo : componentType.getFieldList()) {
            RelDataType fieldType = fieldInfo.getType();
            if (isNullable) {
              fieldType = typeFactory.enforceTypeWithNullability(fieldType, true);
            }
            builder.add(fieldInfo.getName(), fieldType);
          }
        } else {
          // Element type is not a record, use the field name of the element directly
          builder.add(field.getName(), componentType);
        }
      }
    }

    if (withOrdinality) {
      builder.add(SqlUnnestOperator.ORDINALITY_COLUMN_NAME,
          SqlTypeName.INTEGER);
    }
    return builder.build();
  }

  /** Gets the aliases for the unnest items. */
  public List<String> getItemAliases() {
    return itemAliases;
  }
}
