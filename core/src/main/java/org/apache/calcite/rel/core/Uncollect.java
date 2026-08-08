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
import org.apache.calcite.rel.RelShuttle;
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
 *
 * <p>{@code expandStructFields} controls the shape of the element columns:
 * if {@code true} a collection whose element type is a struct produces one
 * output column per struct field; if {@code false} it produces a single
 * column typed as the whole element (Trino semantics). Maps always expand
 * into a key and a value column, regardless of this flag.
 *
 * <p>{@code isOuter} controls what happens to an empty or {@code NULL}
 * collection: if {@code true} (LEFT JOIN semantics) one row is emitted with
 * every element column set to {@code NULL}; if {@code false} (INNER
 * semantics) no row is emitted. Every element column is therefore nullable
 * when {@code isOuter}.
 */
public class Uncollect extends SingleRel {
  public final boolean withOrdinality;

  /** If true, an empty or NULL collection yields a single row whose element
   * columns are all NULL, rather than no rows at all. */
  public final boolean isOuter;

  /** If true, a collection whose element type is a struct expands into one
   * output column per struct field; if false, it produces a single column
   * typed as the whole element. */
  public final boolean expandStructFields;

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
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, List<String> itemAliases) {
    // Non-empty item aliases historically implied that struct elements are not
    // expanded (Presto dialect), so this constructor derives
    // {@code expandStructFields} from their absence.
    this(cluster, traitSet, input, withOrdinality, itemAliases, itemAliases.isEmpty(),
        false);
  }

  /** Creates an Uncollect.
   *
   * @param input              Input relational expression
   * @param withOrdinality     Whether output should contain an ORDINALITY column
   * @param itemAliases        Aliases for the operand items
   * @param expandStructFields If true, a collection whose element type is a struct
   *                           produces one output column per struct field; if false,
   *                           a single column typed as the whole element
   * @param isOuter            If true, an empty or NULL collection yields one row of
   *                           NULLs (LEFT JOIN); if false, it yields no rows (INNER)
   */
  @SuppressWarnings("method.invocation.invalid")
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, List<String> itemAliases, boolean expandStructFields,
      boolean isOuter) {
    super(cluster, traitSet, input);
    this.withOrdinality = withOrdinality;
    this.itemAliases = ImmutableList.copyOf(itemAliases);
    this.expandStructFields = expandStructFields;
    this.isOuter = isOuter;
    requireNonNull(deriveRowType(), "invalid child rowType");
  }

  /**
   * Creates an Uncollect by parsing serialized output.
   */
  public Uncollect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBoolean("withOrdinality", false), Collections.emptyList(),
        input.getBoolean("expandStructFields", true),
        input.getBoolean("isOuter", false));
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

  /**
   * Creates an Uncollect.
   *
   * @param traitSet           Trait set
   * @param input              Input relational expression
   * @param withOrdinality     Whether output should contain an ORDINALITY column
   * @param itemAliases        Aliases for the operand items
   * @param expandStructFields If true, a collection whose element type is a struct
   *                           produces one output column per struct field; if false,
   *                           a single column typed as the whole element
   * @param isOuter            If true, an empty or NULL collection yields one row of
   *                           NULLs (LEFT JOIN); if false, it yields no rows (INNER)
   */
  public static Uncollect create(
      RelTraitSet traitSet,
      RelNode input,
      boolean withOrdinality,
      List<String> itemAliases,
      boolean expandStructFields,
      boolean isOuter) {
    final RelOptCluster cluster = input.getCluster();
    return new Uncollect(cluster, traitSet, input, withOrdinality, itemAliases,
        expandStructFields, isOuter);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("withOrdinality", withOrdinality, withOrdinality)
        .itemIf("expandStructFields", expandStructFields, !expandStructFields)
        .itemIf("isOuter", isOuter, isOuter);
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Uncollect(getCluster(), traitSet, input, withOrdinality, itemAliases,
        expandStructFields, isOuter);
  }

  /**
   * Returns the row type returned by applying the 'UNNEST' operation to a
   * relational expression.
   *
   * @deprecated Construct an {@link Uncollect} and call
   * {@link #getRowType()} instead.
   */
  @Deprecated // to be removed before 2.0
  public static RelDataType deriveUncollectRowType(RelNode rel,
      boolean withOrdinality, List<String> itemAliases) {
    return new Uncollect(rel.getCluster(), rel.getTraitSet(), rel,
        withOrdinality, itemAliases).getRowType();
  }

  /**
   * Returns the row type of the 'UNNEST' operation.
   *
   * <p>Each column in the input relational expression must be a multiset of
   * structs or an array. The return type is the combination of expanding
   * element types from each column, plus an ORDINALITY column if {@code
   * withOrdinality}.
   *
   * <p>{@code expandStructFields} controls the expansion of struct element
   * types: if {@code true}, one output column per struct field; if {@code
   * false}, a single column typed as the whole element. Maps always expand
   * into a key and a value column. {@code itemAliases}, when not empty,
   * names the non-expanded element columns.
   */
  @Override protected RelDataType deriveRowType() {
    RelDataType inputType = input.getRowType();
    assert inputType.isStruct() : inputType + " is not a struct";

    boolean requireAlias = !itemAliases.isEmpty();
    assert !requireAlias || itemAliases.size() == inputType.getFieldCount();

    final List<RelDataTypeField> fields = inputType.getFieldList();
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
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

    // With multiple collections, zip semantics pads shorter collections with
    // NULL, so all output columns from a multi-collection UNNEST are nullable.
    final boolean padNullable = fields.size() > 1;

    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      if (field.getType() instanceof MapSqlType) {
        // This code is similar to SqlUnnestOperator::inferReturnType.
        MapSqlType mapType = (MapSqlType) field.getType();
        RelDataType keyType = padNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getKeyType(), true)
            : mapType.getKeyType();
        RelDataType valueType = padNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getValueType(), true)
            : mapType.getValueType();
        builder.add(SqlUnnestOperator.MAP_KEY_COLUMN_NAME, keyType);
        builder.add(SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, valueType);
      } else {
        RelDataType componentType = field.getType().getComponentType();
        if (null == componentType) {
          throw RESOURCE.unnestArgument().ex();
        }
        boolean isNullable = componentType.isNullable() || padNullable;
        if (expandStructFields && componentType.isStruct()) {
          for (RelDataTypeField fieldInfo : componentType.getFieldList()) {
            RelDataType fieldType = fieldInfo.getType();
            if (isNullable) {
              fieldType = typeFactory.enforceTypeWithNullability(fieldType, true);
            }
            builder.add(fieldInfo.getName(), fieldType);
          }
        } else {
          // A single column typed as the whole element, named by the item
          // alias when present, otherwise by the collection field's name.
          RelDataType elementType = componentType.isStruct()
              ? typeFactory.builder().kind(componentType.getStructKind())
                  .addAll(componentType.getFieldList()).build()
              : componentType;
          // A NULL collection element becomes a NULL value in this column, so
          // the column is nullable whenever the element type is.
          RelDataType colType = isNullable
              ? typeFactory.enforceTypeWithNullability(elementType, true)
              : elementType;
          builder.add(requireAlias ? itemAliases.get(i) : field.getName(), colType);
        }
      }
    }

    if (withOrdinality) {
      builder.add(SqlUnnestOperator.ORDINALITY_COLUMN_NAME,
          SqlTypeName.INTEGER);
    }
    final RelDataType rowType = builder.build();
    if (!isOuter) {
      return rowType;
    }
    // Under isOuter an empty or NULL collection yields a row of NULLs, so
    // every output column is nullable, including the ordinality column.
    final RelDataTypeFactory.Builder outerBuilder = typeFactory.builder();
    for (RelDataTypeField field : rowType.getFieldList()) {
      outerBuilder.add(field.getName(),
          typeFactory.createTypeWithNullability(field.getType(), true));
    }
    return outerBuilder.build();
  }

  /** Gets the aliases for the unnest items. */
  public List<String> getItemAliases() {
    return itemAliases;
  }
}
