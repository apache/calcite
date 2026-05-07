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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.calcite.rel.core.TableModify} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableModify extends TableModify
    implements EnumerableRel {
  public EnumerableTableModify(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation,
        updateColumnList, sourceExpressionList, flattened);
    assert child.getConvention() instanceof EnumerableConvention;
    assert getConvention() instanceof EnumerableConvention;
    final ModifiableTable modifiableTable =
        table.unwrap(ModifiableTable.class);
    if (modifiableTable == null) {
      throw new AssertionError(); // TODO: user error in validator
    }
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableTableModify(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result result =
        implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);

    // Enumerable produced by the input relational expression.
    final Expression sourceExp =
        builder.append("source", result.block);

    // Variable that will hold the table's mutable backing collection.
    final ParameterExpression collectionParameter =
        Expressions.parameter(Collection.class,
            builder.newName("collection"));

    // Expression that yields the ModifiableTable instance at runtime.
    final Expression expression = table.getExpression(ModifiableTable.class);
    requireNonNull(expression, "expression"); // TODO: user error in validator
    checkArgument(
        ModifiableTable.class.isAssignableFrom(
            Types.toClass(expression.getType())),
        "not assignable from type %s", expression.getType());

    // collection = table.getModifiableCollection()
    builder.add(
        Expressions.declare(
            Modifier.FINAL,
            collectionParameter,
            Expressions.call(
                expression,
                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method)));

    // Physical row representation of this TableModify node's output
    // (a single ROWCOUNT field).
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref == Prefer.ARRAY ? JavaRowFormat.ARRAY : JavaRowFormat.SCALAR);

    switch (getOperation()) {
    case INSERT:
      return implementInsert(implementor, result, builder, sourceExp, collectionParameter,
          physType);
    case UPDATE:
      return implementUpdate(implementor, builder, sourceExp, collectionParameter, physType);
    case DELETE:
      return implementDelete(implementor, result, builder, sourceExp, collectionParameter,
          physType);
    default:
      throw new AssertionError("unsupported operation: " + getOperation());
    }
  }

  /**
   * Generates code for an UPDATE statement.
   *
   * <p>Applies updates to matching rows in the backing collection and returns
   * the number of updated rows as a single-element enumerable.
   *
   * @param implementor         code-generation context
   * @param builder             block under construction
   * @param sourceExp           enumerable of source rows produced by the input
   * @param collectionParameter the modifiable backing collection of the table
   * @param physType            physical type of this node's output row
   */
  private Result implementUpdate(
      EnumerableRelImplementor implementor,
      BlockBuilder builder,
      Expression sourceExp,
      ParameterExpression collectionParameter,
      PhysType physType) {
    final List<String> updateCols = requireNonNull(getUpdateColumnList());
    final List<RelDataTypeField> tableFields = table.getRowType().getFieldList();
    final int tableFieldCount = tableFields.size();

    // Child row layout for UPDATE:
    // [originalField_0, ..., originalField_N-1, newValue_0, ..., newValue_M-1]
    // where N = tableFieldCount and M = updateCols.size().

    // Resolve each SET-column name to its 0-based position in the table row.
    final int[] updateColumnIndices = new int[updateCols.size()];
    for (int i = 0; i < updateCols.size(); i++) {
      final String colName = updateCols.get(i);
      int found = -1;
      for (int j = 0; j < tableFields.size(); j++) {
        if (tableFields.get(j).getName().equals(colName)) {
          found = j;
          break;
        }
      }
      if (found < 0) {
        throw new AssertionError("column '" + colName + "' not found in table");
      }
      updateColumnIndices[i] = found;
    }

    // Generate code that applies one-to-one update consumption:
    // each source row updates at most one matching sink row.
    final Expression updateCountExp =
        builder.append(
            "updateCount",
            Expressions.call(
                EnumerableTableModify.class,
                "applyUpdateOneToOne",
                // Source rows are produced by the child relational expression.
                sourceExp,
                // Sink is the table's mutable backing collection.
                Expressions.convert_(collectionParameter, List.class),
                // Number of original-row fields in each source payload.
                Expressions.constant(tableFieldCount),
                // Table column positions to overwrite from trailing source values.
                Expressions.constant(updateColumnIndices)));

    // Return the number of updated rows as the single output row.
    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                Expressions.convert_(
                    updateCountExp,
                    long.class))));

    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Applies UPDATE with one-to-one, first-match consumption semantics.
   *
   * <p>Each source row contributes one replacement row keyed by the original
   * row content. As sink rows are scanned in order, the first matching row for
   * each queued source update is replaced and consumed, so duplicate keys update
   * only as many rows as appear in the source.
   */
  public static long applyUpdateOneToOne(Enumerable<Object[]> source, List<Object[]> sink,
      int tableFieldCount, int[] updateColumnIndices) {
    final Map<List<Object>, Deque<Object[]>> updatesByKey = new HashMap<>();
    try (Enumerator<Object[]> e = source.enumerator()) {
      while (e.moveNext()) {
        final Object[] sourceRow = e.current();
        final List<Object> key = Arrays.asList(Arrays.copyOf(sourceRow, tableFieldCount));
        final Object[] newRow = applyUpdate(sourceRow, tableFieldCount, updateColumnIndices);
        updatesByKey.computeIfAbsent(key, k -> new ArrayDeque<>()).addLast(newRow);
      }
    }

    long updateCount = 0;
    final ListIterator<Object[]> it = sink.listIterator();
    while (it.hasNext()) {
      final Object[] current = it.next();
      final List<Object> key = Arrays.asList(current);
      final Deque<Object[]> pending = updatesByKey.get(key);
      if (pending == null || pending.isEmpty()) {
        continue;
      }
      it.set(pending.removeFirst());
      updateCount++;
      if (pending.isEmpty()) {
        updatesByKey.remove(key);
      }
    }
    return updateCount;
  }

  /**
   * Generates code for a DELETE statement.
   *
   * <p>The source produces every row that matches the WHERE clause. Those rows
   * are removed from the backing collection and the number of deleted rows is
   * returned as a single-element enumerable.
   *
   * @param implementor         code-generation context
   * @param result              compiled result of the input relational expression
   * @param builder             block under construction
   * @param sourceExp           enumerable of rows to delete — every row matched
   *                            by the WHERE clause, as produced by the input
   *                            relational expression
   * @param collectionParameter the modifiable backing collection of the table
   * @param physType            physical type of this node's output row
   */
  private Result implementDelete(
      EnumerableRelImplementor implementor,
      Result result,
      BlockBuilder builder,
      Expression sourceExp,
      ParameterExpression collectionParameter,
      PhysType physType) {

    // Snapshot the collection size before the delete so we can compute the
    // number of rows removed as (sizeBefore - sizeAfter).
    final Expression countParameter =
        builder.append(
            "count",
            Expressions.call(collectionParameter, "size"),
            false);

    // Build source delete keys as Object[] values in table field order.
    final JavaTypeFactory typeFactory = (JavaTypeFactory) getCluster().getTypeFactory();
    final JavaRowFormat tableFormat = EnumerableTableScan.deduceFormat(table);
    final PhysType tablePhysType = PhysTypeImpl.of(typeFactory, table.getRowType(), tableFormat);
    final PhysType sourcePhysType = result.physType;
    final int fieldCount = table.getRowType().getFieldCount();

    final ParameterExpression sourceRow =
        Expressions.parameter(sourcePhysType.getJavaRowType(), "sourceRow");
    final List<Expression> sourceValues = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      sourceValues.add(
          sourcePhysType.fieldReference(sourceRow, i,
              tablePhysType.getJavaFieldType(i)));
    }
    final Expression deleteKeysExp =
        builder.append(
            "deleteKeys",
            Expressions.call(
                sourceExp,
                BuiltInMethod.SELECT.method,
                Expressions.lambda(
                    Function1.class,
                    Expressions.newArrayInit(Object.class, sourceValues),
                    sourceRow)));

    // Build sink key extractor by reading table fields from each sink row.
    final ParameterExpression sinkRow = Expressions.parameter(Object.class, "sinkRow");
    final Expression typedSinkRow =
        Expressions.convert_(sinkRow, tablePhysType.getJavaRowType());
    final List<Expression> sinkValues = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      sinkValues.add(tablePhysType.fieldReference(typedSinkRow, i, Object.class));
    }
    final Expression sinkKeySelector =
        Expressions.lambda(
            Function1.class,
            Expressions.newArrayInit(Object.class, sinkValues),
            sinkRow);

    // Remove one sink row per matching source row, matched by field values.
    builder.add(
        Expressions.statement(
            Expressions.call(
                EnumerableTableModify.class,
                "applyDeleteRowsByKey",
                deleteKeysExp,
                collectionParameter,
                sinkKeySelector)));

    // Snapshot the size again and return (sizeBefore - sizeAfter) as the delete count.
    final Expression deletedCountParameter =
        builder.append(
            "deletedCount",
            Expressions.call(collectionParameter, "size"),
            false);

    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                Expressions.convert_(
                    Expressions.subtract(countParameter, deletedCountParameter),
                    long.class))));

    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Normalizes the source expression to match the table's row type by adding
   * field-by-field cast projections when needed.
   *
   * @param builder       the block builder for code generation
   * @param sourceExp     the source expression to normalize
   * @param result        the compiled result of the input relational expression
   * @param operationName the name to use in builder.append (e.g., "insertRows", "deleteRows")
   * @return the normalized expression, either the original sourceExp if types match
   * or a new expression with field casts applied
   */
  private Expression normalizeSourceExpression(BlockBuilder builder, Expression sourceExp,
      Result result, String operationName) {
    if (!getInput().getRowType().equals(getRowType())) {
      // The source row type doesn't match the table's row type (e.g. types
      // differ in nullability or precision), so wrap the source in a projection
      // that casts each field to the exact Java type the table expects.
      final JavaTypeFactory typeFactory = (JavaTypeFactory) getCluster().getTypeFactory();
      final JavaRowFormat format = EnumerableTableScan.deduceFormat(table);
      PhysType tablePhysType = PhysTypeImpl.of(typeFactory, table.getRowType(), format);

      // One cast expression per field: sourceField -> tableFieldType.
      List<Expression> expressionList = new ArrayList<>();
      final PhysType sourcePhysType = result.physType;
      final ParameterExpression o_ = Expressions.parameter(sourcePhysType.getJavaRowType(), "o");
      final int fieldCount = sourcePhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        expressionList.add(
            sourcePhysType.fieldReference(o_, i, tablePhysType.getJavaFieldType(i)));
      }

      // normalizedExp = sourceExp.select(o -> new TableRow(cast(o.f0), cast(o.f1), ...))
      return builder.append(
          operationName,
          Expressions.call(
              sourceExp,
              BuiltInMethod.SELECT.method,
              Expressions.lambda(tablePhysType.record(expressionList), o_)));
    } else {
      return sourceExp;
    }
  }

  /**
   * Generates code for an INSERT statement.
   *
   * <p>All rows produced by the source are added to the backing collection and
   * the number of inserted rows is returned as a single-element enumerable.
   *
   * @param implementor         code-generation context
   * @param result              compiled result of the input relational expression
   * @param builder             block under construction
   * @param sourceExp           enumerable of rows to insert — the output of the input
   *                            relational expression (VALUES, SELECT, etc.) with any
   *                            upstream filtering or projection already applied
   * @param collectionParameter the modifiable backing collection of the table
   * @param physType            physical type of this node's output row
   */
  private Result implementInsert(
      EnumerableRelImplementor implementor,
      Result result,
      BlockBuilder builder,
      Expression sourceExp,
      ParameterExpression collectionParameter,
      PhysType physType) {

    // Snapshot the collection size before the insert so we can compute the
    // number of rows added as (sizeAfter - sizeBefore).
    final Expression countParameter =
        builder.append(
            "count",
            Expressions.call(collectionParameter, "size"),
            false);

    // Normalize the source values to match the table's row type
    final Expression insertExp =
        normalizeSourceExpression(builder, sourceExp, result, "insertRows");

    // Stream all rows from insertExp into the backing collection.
    builder.add(
        Expressions.statement(
            Expressions.call(
                insertExp, BuiltInMethod.INTO.method, collectionParameter)));

    // Snapshot the size again and return (sizeAfter - sizeBefore) as the insert count.
    final Expression updatedCountParameter =
        builder.append(
            "updatedCount",
            Expressions.call(collectionParameter, "size"),
            false);

    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                Expressions.convert_(
                    Expressions.subtract(updatedCountParameter, countParameter),
                    long.class))));

    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Builds the replacement row for an UPDATE source row.
   *
   * @param row                 source row produced by the child expression
   * @param tableFieldCount     number of fields in the original table row
   * @param updateColumnIndices 0-based indices of the columns being updated
   * @return the replacement row
   */
  public static Object[] applyUpdate(Object[] row, int tableFieldCount, int[] updateColumnIndices) {
    // Source row layout: [originalField_0, ..., originalField_N-1, newValue_0, ..., newValue_M-1]
    // where N = tableFieldCount and M = updateColumnIndices.length.
    // Copy the first N fields and overwrite the positions named in the SET clause.
    final Object[] newRow = new Object[tableFieldCount];
    System.arraycopy(row, 0, newRow, 0, tableFieldCount);
    for (int i = 0; i < updateColumnIndices.length; i++) {
      newRow[updateColumnIndices[i]] = row[tableFieldCount + i];
    }
    return newRow;
  }

  /**
   * Removes one sink row per source row, matching by field values in table order. Accepts a
   * lambda that extracts the key from a sink row.
   */
  public static void applyDeleteRowsByKey(Enumerable<Object[]> sourceKeys,
      Collection<Object> sinkRows, Function1<Object, Object[]> sinkKeySelector) {

    // Build a map of source keys to the number of sink rows that must be removed for each.
    final Map<List<Object>, Integer> pendingByKey = new HashMap<>();
    try (Enumerator<Object[]> e = sourceKeys.enumerator()) {
      while (e.moveNext()) {
        final List<Object> key = keyOf(e.current());
        pendingByKey.put(key, pendingByKey.getOrDefault(key, 0) + 1);
      }
    }

    // Iterate over sink rows and remove matching rows based on key.
    for (java.util.Iterator<Object> it = sinkRows.iterator(); it.hasNext();) {
      final List<Object> key = keyOf(sinkKeySelector.apply(it.next()));
      final Integer pending = pendingByKey.get(key);
      if (pending == null || pending == 0) {
        continue;
      }

      it.remove();

      if (pending == 1) {
        pendingByKey.remove(key);
      } else {
        pendingByKey.put(key, pending - 1);
      }
    }
  }

  /**
   * Returns an equatable key for a row.
   *
   * @param rowValues row values
   * @return key for row
   */
  private static List<Object> keyOf(Object[] rowValues) {
    return Arrays.asList(Arrays.copyOf(rowValues, rowValues.length));
  }

}
