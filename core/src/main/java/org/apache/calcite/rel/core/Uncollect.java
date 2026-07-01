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
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
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
 * <p>By default, every input field is unnested and no input field is dropped
 * or passed through unchanged. {@code Uncollect} also supports a more
 * general mode, in which {@code passthroughFieldIndices} names input fields
 * that are copied unchanged to the output and {@code collectionFieldIndices}
 * names the (possibly smaller) set of input fields to unnest; any input
 * field in neither set is dropped.
 *
 * <p>{@code isOuter} controls the
 * behavior for empty or {@code NULL} collections: if {@code true} (LEFT JOIN
 * semantics) the input row is preserved with {@code NULL}-padded element
 * columns; if {@code false} (INNER semantics) the input row is dropped.
 */
public class Uncollect extends SingleRel {
  public final boolean withOrdinality;
  public final boolean isOuter;

  // To alias the items in Uncollect list,
  // i.e., "UNNEST(a, b, c) as T(d, e, f)"
  // outputs as row type Record(d, e, f) where the field "d" has element type of "a",
  // field "e" has element type of "b"(Presto dialect).

  // Without the aliases, the expression "UNNEST(a)" outputs row type
  // same with element type of "a".
  private final List<String> itemAliases;

  /** 0-based indices of the input fields to pass through unchanged. Empty by default. */
  private final ImmutableBitSet passthroughFieldIndices;
  /** 0-based indices of the input fields whose values are collections to unnest.
   * All input fields by default. */
  private final ImmutableBitSet collectionFieldIndices;

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
    this(cluster, traitSet, input, withOrdinality, itemAliases,
        ImmutableBitSet.of(), ImmutableBitSet.range(input.getRowType().getFieldCount()),
        false);
  }

  /** Creates an Uncollect, with explicit control over which input fields are
   * passed through unchanged and which are unnested.
   *
   * @param input                    Input relational expression
   * @param withOrdinality           Whether output should contain an ORDINALITY column
   * @param itemAliases              Aliases for the operand items; only meaningful when
   *                                 {@code passthroughFieldIndices} is empty and
   *                                 {@code collectionFieldIndices} covers every input field
   * @param passthroughFieldIndices 0-based indices of the input fields to pass through
   *                                unchanged
   * @param collectionFieldIndices  0-based indices of the input fields whose values are
   *                                collections to unnest
   * @param isOuter                 If true, preserves input rows with null/empty
   *                                collections (LEFT JOIN); if false, drops them (INNER)
   */
  @SuppressWarnings("method.invocation.invalid")
  public Uncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, List<String> itemAliases,
      ImmutableBitSet passthroughFieldIndices, ImmutableBitSet collectionFieldIndices,
      boolean isOuter) {
    super(cluster, traitSet, input);
    this.withOrdinality = withOrdinality;
    this.itemAliases = ImmutableList.copyOf(itemAliases);
    this.passthroughFieldIndices =
        requireNonNull(passthroughFieldIndices, "passthroughFieldIndices");
    this.collectionFieldIndices =
        requireNonNull(collectionFieldIndices, "collectionFieldIndices");
    if (collectionFieldIndices.isEmpty()) {
      throw new IllegalArgumentException("collectionFieldIndices must not be empty");
    }
    this.isOuter = isOuter;
    requireNonNull(deriveRowType(), "invalid child rowType");
  }

  /**
   * Creates an Uncollect by parsing serialized output.
   */
  public Uncollect(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBoolean("withOrdinality", false), Collections.emptyList(),
        fieldIndices(input, "passthrough", ImmutableBitSet.of()),
        fieldIndices(input, "collectionFields",
            ImmutableBitSet.range(input.getInput().getRowType().getFieldCount())),
        input.getBoolean("isOuter", false));
  }

  /** Maps the input-field names serialized under {@code tag} (see
   * {@link #explainTerms}) back to field indices; returns {@code defaultSet}
   * when the attribute is absent. */
  private static ImmutableBitSet fieldIndices(RelInput input, String tag,
      ImmutableBitSet defaultSet) {
    final List<String> names = input.getStringList(tag);
    if (names == null) {
      return defaultSet;
    }
    final RelDataType inputRowType = input.getInput().getRowType();
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (String name : names) {
      builder.set(
          requireNonNull(inputRowType.getField(name, true, false),
              () -> "field " + name).getIndex());
    }
    return builder.build();
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
   * Creates an Uncollect that unnests the collection-typed fields at
   * {@code collectionFieldIndices}, passing through the fields at
   * {@code passthroughFieldIndices} unchanged and dropping all others.
   *
   * @param traitSet                Trait set
   * @param input                   Input relational expression
   * @param withOrdinality          Whether output should contain an ORDINALITY column
   * @param passthroughFieldIndices 0-based indices of the input fields to pass through
   *                                unchanged
   * @param collectionFieldIndices  0-based indices of the input fields whose values are
   *                                collections to unnest
   * @param isOuter                 If true, preserves input rows with null/empty
   *                                collections (LEFT JOIN); if false, drops them (INNER)
   */
  public static Uncollect create(
      RelTraitSet traitSet,
      RelNode input,
      boolean withOrdinality,
      ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices,
      boolean isOuter) {
    final RelOptCluster cluster = input.getCluster();
    return new Uncollect(cluster, traitSet, input, withOrdinality, Collections.emptyList(),
        passthroughFieldIndices, collectionFieldIndices, isOuter);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    final List<RelDataTypeField> inputFields = getInput().getRowType().getFieldList();
    final List<String> passthroughNames = new ArrayList<>();
    for (int i : passthroughFieldIndices) {
      passthroughNames.add(inputFields.get(i).getName());
    }
    final boolean isPassthroughMode = !passthroughFieldIndices.isEmpty()
        || collectionFieldIndices.cardinality() != inputFields.size();
    final List<String> collFieldNames = new ArrayList<>();
    for (int i : collectionFieldIndices) {
      collFieldNames.add(inputFields.get(i).getName());
    }
    return super.explainTerms(pw)
        .itemIf("passthrough", passthroughNames, !passthroughNames.isEmpty())
        .itemIf("collectionFields", collFieldNames, isPassthroughMode)
        .itemIf("withOrdinality", withOrdinality, withOrdinality)
        .itemIf("isOuter", isOuter, isOuter);
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new Uncollect(getCluster(), traitSet, input, withOrdinality, itemAliases,
        passthroughFieldIndices, collectionFieldIndices, isOuter);
  }

  @Override protected RelDataType deriveRowType() {
    return deriveUncollectRowType(input, passthroughFieldIndices, collectionFieldIndices,
        withOrdinality, itemAliases, isOuter);
  }

  /**
   * Returns the row type returned by applying the 'UNNEST' operation to a
   * relational expression.
   *
   * <p>Pass-through fields (named by {@code passthroughFieldIndices}) are
   * copied to the output as-is, followed by the expansion of the
   * collection-typed fields (named by {@code collectionFieldIndices}): each
   * column in {@code collectionFieldIndices} must be a multiset of structs
   * or an array. The return type is the combination of expanding element
   * types from each such column, plus an ORDINALITY column if {@code
   * withOrdinality}. If {@code itemAliases} is not empty, the element types
   * would not expand, each collection field outputs as a whole (the return
   * type has same column types as input type). Any input field in neither
   * set is dropped.
   *
   * <p>Element columns are nullable when {@code outer} is {@code true}
   * (empty/null collections still emit a null-padded row) or when there is
   * more than one collection field (shorter collections are padded with
   * {@code NULL} to align with SQL {@code UNNEST(a, b)} zip semantics).
   */
  public static RelDataType deriveUncollectRowType(RelNode rel,
      ImmutableBitSet passthroughFieldIndices, ImmutableBitSet collectionFieldIndices,
      boolean withOrdinality, List<String> itemAliases, boolean outer) {
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

    // Element columns are nullable when:
    // - outer=true: empty/null collections emit a null-padded row, or
    // - multiple collections: zip pads shorter collections with NULL.
    final boolean elemNullable = outer || collectionFieldIndices.cardinality() > 1;

    // Pass-through fields first (in input-index order).
    for (int i = 0; i < fields.size(); i++) {
      if (passthroughFieldIndices.get(i)) {
        builder.add(fields.get(i));
      }
    }

    // Expanded collection fields second (in input-index order).
    for (int i = 0; i < fields.size(); i++) {
      if (!collectionFieldIndices.get(i)) {
        continue;
      }
      RelDataTypeField field = fields.get(i);
      if (field.getType() instanceof MapSqlType) {
        // This code is similar to SqlUnnestOperator::inferReturnType.
        MapSqlType mapType = (MapSqlType) field.getType();
        RelDataType keyType = elemNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getKeyType(), true)
            : mapType.getKeyType();
        RelDataType valueType = elemNullable
            ? typeFactory.enforceTypeWithNullability(mapType.getValueType(), true)
            : mapType.getValueType();
        builder.add(SqlUnnestOperator.MAP_KEY_COLUMN_NAME, keyType);
        builder.add(SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, valueType);
      } else {
        RelDataType componentType = field.getType().getComponentType();
        if (null == componentType) {
          throw RESOURCE.unnestArgument().ex();
        }
        boolean isNullable = componentType.isNullable() || elemNullable;
        if (requireAlias) {
          RelDataType colType = elemNullable
              ? typeFactory.enforceTypeWithNullability(componentType, true)
              : componentType;
          builder.add(itemAliases.get(i), colType);
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
          RelDataType colType = elemNullable
              ? typeFactory.enforceTypeWithNullability(componentType, true)
              : componentType;
          builder.add(field.getName(), colType);
        }
      }
    }

    if (withOrdinality) {
      final RelDataType ordType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      // For outer JOINS even the ordinality is nullable
      builder.add(SqlUnnestOperator.ORDINALITY_COLUMN_NAME,
          outer ? typeFactory.createTypeWithNullability(ordType, true) : ordType);
    }
    return builder.build();
  }

  /** Gets the aliases for the unnest items. */
  public List<String> getItemAliases() {
    return itemAliases;
  }

  /** Returns the 0-based indices of the input fields passed through unchanged. */
  public ImmutableBitSet getPassthroughFieldIndices() {
    return passthroughFieldIndices;
  }

  /** Returns the 0-based indices of the input fields to be unnested. */
  public ImmutableBitSet getCollectionFieldIndices() {
    return collectionFieldIndices;
  }
}
