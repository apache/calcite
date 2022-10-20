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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.Iterables;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A relational expression that collapses multiple rows into one.
 *
 * <p>Rules:</p>
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.SubQueryRemoveRule}
 * creates a Collect from a call to
 * {@link org.apache.calcite.sql.fun.SqlArrayQueryConstructor},
 * {@link org.apache.calcite.sql.fun.SqlMapQueryConstructor}, or
 * {@link org.apache.calcite.sql.fun.SqlMultisetQueryConstructor}.</li>
 * </ul>
 */
public class Collect extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Collect.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster
   * @param traitSet  Trait set
   * @param input     Input relational expression
   * @param rowType   Row type
   */
  protected Collect(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType rowType) {
    super(cluster, traitSet, input);
    this.rowType = requireNonNull(rowType, "rowType");
    final SqlTypeName collectionType = getCollectionType(rowType);
    switch (collectionType) {
    case ARRAY:
    case MAP:
    case MULTISET:
      break;
    default:
      throw new IllegalArgumentException("not a collection type "
          + collectionType);
    }
  }

  @Deprecated // to be removed before 2.0
  public Collect(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      String fieldName) {
    this(cluster, traitSet, input,
        deriveRowType(cluster.getTypeFactory(), SqlTypeName.MULTISET, fieldName,
            input.getRowType()));
  }

  /**
   * Creates a Collect by parsing serialized output.
   */
  public Collect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        deriveRowType(input.getCluster().getTypeFactory(), SqlTypeName.MULTISET,
            requireNonNull(input.getString("field"), "field"),
            input.getInput().getRowType()));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a Collect.
   *
   * @param input          Input relational expression
   * @param rowType        Row type
   */
  public static Collect create(RelNode input, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE);
    return new Collect(cluster, traitSet, input, rowType);
  }

  /**
   * Creates a Collect.
   *
   * @param input          Input relational expression
   * @param collectionType ARRAY, MAP or MULTISET
   * @param fieldName      Name of the sole output field
   */
  @Deprecated // to be removed before 2.0
  public static Collect create(RelNode input,
      SqlTypeName collectionType,
      String fieldName) {
    return create(input,
        deriveRowType(input.getCluster().getTypeFactory(), collectionType,
            fieldName, input.getRowType()));
  }

  /**
   * Creates a Collect.
   *
   * @param input          Input relational expression
   * @param sqlKind        SqlKind
   * @param fieldName      Name of the sole output field
   */
  public static Collect create(RelNode input,
      SqlKind sqlKind,
      String fieldName) {
    SqlTypeName collectionType = getCollectionType(sqlKind);
    RelDataType rowType;
    switch (sqlKind) {
    case ARRAY_QUERY_CONSTRUCTOR:
    case MULTISET_QUERY_CONSTRUCTOR:
      rowType = deriveRowType(input.getCluster().getTypeFactory(),
          collectionType, fieldName,
          SqlTypeUtil.deriveCollectionQueryComponentType(collectionType, input.getRowType()));
      break;
    default:
      rowType = deriveRowType(input.getCluster().getTypeFactory(), collectionType,
          fieldName, input.getRowType());
    }
    return create(input, rowType);
  }

  /** Returns the row type, guaranteed not null.
   * (The row type is never null after initialization, but
   * CheckerFramework can't deduce that references are safe.) */
  protected final RelDataType rowType() {
    return requireNonNull(rowType, "rowType");
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Collect(getCluster(), traitSet, input, rowType());
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("field", getFieldName());
  }

  /**
   * Returns the name of the sole output field.
   *
   * @return name of the sole output field
   */
  public String getFieldName() {
    return Iterables.getOnlyElement(rowType().getFieldList()).getName();
  }

  /** Returns the collection type (ARRAY, MAP, or MULTISET). */
  public SqlTypeName getCollectionType() {
    return getCollectionType(rowType());
  }

  private static SqlTypeName getCollectionType(RelDataType rowType) {
    return Iterables.getOnlyElement(rowType.getFieldList())
        .getType().getSqlTypeName();
  }

  private static SqlTypeName getCollectionType(SqlKind sqlKind) {
    switch (sqlKind) {
    case ARRAY_QUERY_CONSTRUCTOR:
    case ARRAY_VALUE_CONSTRUCTOR:
      return SqlTypeName.ARRAY;
    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
      return SqlTypeName.MULTISET;
    case MAP_QUERY_CONSTRUCTOR:
    case MAP_VALUE_CONSTRUCTOR:
      return SqlTypeName.MAP;
    default:
      throw new IllegalArgumentException("not a collection kind "
          + sqlKind);
    }
  }

  @Override protected RelDataType deriveRowType() {
    // this method should never be called; rowType is always set
    throw new UnsupportedOperationException();
  }

  /**
   * Derives the output row type of a Collect relational expression.
   *
   * @param rel       relational expression
   * @param fieldName name of sole output field
   * @return output row type of a Collect relational expression
   */
  @Deprecated // to be removed before 2.0
  public static RelDataType deriveCollectRowType(
      SingleRel rel,
      String fieldName) {
    RelDataType inputType = rel.getInput().getRowType();
    assert inputType.isStruct();
    return deriveRowType(rel.getCluster().getTypeFactory(),
        SqlTypeName.MULTISET, fieldName, inputType);
  }

  /**
   * Derives the output row type of a Collect relational expression.
   *
   * @param typeFactory    Type factory
   * @param collectionType MULTISET, ARRAY or MAP
   * @param fieldName      Name of sole output field
   * @param elementType    Element type
   * @return output row type of a Collect relational expression
   */
  public static RelDataType deriveRowType(RelDataTypeFactory typeFactory,
      SqlTypeName collectionType, String fieldName, RelDataType elementType) {
    final RelDataType type1;
    switch (collectionType) {
    case ARRAY:
      type1 = SqlTypeUtil.createArrayType(typeFactory, elementType, false);
      break;
    case MULTISET:
      type1 = SqlTypeUtil.createMultisetType(typeFactory, elementType, false);
      break;
    case MAP:
      type1 = SqlTypeUtil.createMapTypeFromRecord(typeFactory, elementType);
      break;
    default:
      throw new AssertionError(collectionType);
    }
    return typeFactory.createTypeWithNullability(
        typeFactory.builder().add(fieldName, type1).build(), false);
  }
}
