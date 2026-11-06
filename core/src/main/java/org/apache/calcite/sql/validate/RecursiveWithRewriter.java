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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCycleClause;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSearchClause;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.sql.validate.SqlValidatorUtil.EXPR_SUGGESTER;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/** Lowers SEARCH and CYCLE to a recursive UNION with generated columns.
 *
 * <p>Each row has its own path. The row closing a cycle is emitted, but is
 * filtered out before evaluating the next recursive step. Fresh syntax nodes
 * are used because the original query has already been validated. */
final class RecursiveWithRewriter {
  private static final SqlParserPos POS = SqlParserPos.ZERO;
  private static final String INPUT = "$RECURSIVE_INPUT";
  private static final String DEPTH = "$DEPTH";

  private final SqlValidatorImpl validator;
  private final SqlWithItem item;
  private final @Nullable SqlCycleClause cycle;
  private final @Nullable SqlSearchClause search;
  private final SqlNameMatcher matcher;

  private RecursiveWithRewriter(SqlValidatorImpl validator, SqlWithItem item) {
    this.validator = validator;
    this.item = item;
    this.cycle = item.cycleClause;
    this.search = item.searchClause;
    this.matcher = validator.getCatalogReader().nameMatcher();
  }

  static SqlWithItem rewrite(SqlValidatorImpl validator, SqlValidatorScope scope,
      SqlWithItem item) {
    return new RecursiveWithRewriter(validator, item).rewrite(scope);
  }

  /** Replaces SEARCH and CYCLE with explicit generated columns in both UNION operands.
   *
   * <p>For {@code CYCLE k1, k2 SET mark TO cycleValue DEFAULT defaultValue
   * USING path}, the transformation is schematically:
   *
   * <pre>{@code
   * seed output:
   *   original columns,
   *   defaultValue AS mark,
   *   ARRAY[ROW(k1, k2)] AS path
   * recursive branch:
   *   SELECT original output columns,
   *          CASE WHEN contains(previousPath, ROW(k1, k2))
   *               THEN cycleValue ELSE defaultValue END AS mark,
   *          append(previousPath, ROW(k1, k2)) AS path
   *   FROM (
   *     SELECT original projection, previous.path AS previousPath
   *     FROM original sources
   *     WHERE (original condition)
   *       AND previous.mark <> cycleValue
   *   ) AS step
   * }</pre>
   *
   * <p>Here {@code previous} is the recursive CTE reference in the original
   * FROM clause; the original condition is TRUE if there was no WHERE clause.
   * The added WHERE condition stops expanding rows already marked as cycles.
   * The recursive output keys are taken after the user's projection. A row
   * closing a cycle is therefore returned with its extended path, but cannot
   * feed the next recursive step.
   *
   * <p>For {@code SEARCH DEPTH FIRST BY k1, k2 SET searchOrder}, the
   * transformation is schematically:
   *
   * <pre>{@code
   * seed output:
   *   original columns,
   *   ARRAY[ROW(k1, k2)] AS searchOrder
   * recursive branch:
   *   SELECT original output columns,
   *          append(previousOrder, ROW(k1, k2)) AS searchOrder
   *   FROM (
   *     SELECT original projection, previous.searchOrder AS previousOrder
   *     FROM original sources
   *     WHERE original condition
   *   ) AS step
   * }</pre>
   *
   * <p>The generated array contains the key tuples along the path, including
   * the current row. Lexicographic ordering of these arrays places a parent
   * before its descendants. For distinct sibling keys, a node's descendants
   * precede the next sibling.
   * The keys within each tuple follow the order of the BY list. Even a single
   * BY column is wrapped in ROW, so the generated column is always an array
   * of rows. The internal {@link SqlInternalOperators#RECURSIVE_PATH_APPEND}
   * operator implements append.
   *
   * <p>For {@code SEARCH BREADTH FIRST BY k1, k2 SET searchOrder}, the
   * transformation is schematically:
   *
   * <pre>{@code
   * seed output:
   *   original columns,
   *   ROW(0, k1, k2) AS searchOrder
   * recursive branch:
   *   SELECT original output columns,
   *          ROW(previousOrder.$DEPTH + 1, k1, k2) AS searchOrder
   *   FROM (
   *     SELECT original projection, previous.searchOrder AS previousOrder
   *     FROM original sources
   *     WHERE original condition
   *   ) AS step
   * }</pre>
   *
   * <p>Here {@code $DEPTH} denotes the first field of the generated row. It is
   * an INTEGER starting at zero for every seed row and incremented at each
   * recursive step. The remaining fields contain only the current row's keys,
   * not the ancestors' keys. Ordering by this row therefore sorts by depth,
   * then by the BY columns within each level, independently of parent order.
   * Key field names are uniquified to avoid collisions with {@code $DEPTH}.
   *
   * <p>In both SEARCH transformations, the previous ordering value is carried
   * through the inner SELECT alongside the user's original projection. The
   * outer SELECT computes the new ordering value using the projected output
   * keys, just as for CYCLE. The seed is also wrapped so that its output keys
   * can be referenced when constructing the initial ordering value.
   * SEARCH only computes a sort key: it neither adds an ORDER BY nor changes
   * the physical evaluation order of recursion, and it does not detect cycles.
   * The consuming query must use {@code ORDER BY searchOrder} to request the
   * corresponding output order. Rows with equal ordering values have no
   * guaranteed relative order.
   *
   * <p>The original UNION [ALL] is retained. If both clauses are present, they
   * are lowered together so that the user's projection and recursive reference
   * are only expanded once. The search column precedes the cycle mark and path
   * columns, and the CYCLE filter still stops expansion of completed paths.
   * The user's projection, including SELECT *, is expanded before generated
   * columns are added; those columns are not visible in the user's recursive
   * term. The returned WITH item declares all generated columns and no longer
   * has SEARCH or CYCLE clauses. */
  private SqlWithItem rewrite(SqlValidatorScope scope) {
    final Reference reference = validateRecursiveQuery();
    final RelDataType rowType = validator.getNamespaceOrThrow(item).getRowType();
    final List<RelDataTypeField> cycleKeys = cycle == null
        ? ImmutableList.of() : validateKeys(rowType, cycle.columns, false);
    final List<RelDataTypeField> searchKeys = search == null
        ? ImmutableList.of() : validateKeys(rowType, search.columns, true);
    final List<String> names = rowType.getFieldNames();
    validateGeneratedNames(names);
    final RelDataType markType = cycle == null ? null : validateMarkType(scope);

    final SqlNode query = prepareQuery(reference);
    final SqlCall union = (SqlCall) queryBody(query);
    union.setOperand(0, rewriteSeed(union.operand(0), names, cycleKeys, searchKeys, markType));
    union.setOperand(1,
        rewriteStep(union.operand(1), reference, names, cycleKeys, searchKeys, markType));

    final List<SqlNode> outputNames = identifiers(names);
    for (SqlIdentifier generated : generatedColumns()) {
      outputNames.add(copy(generated));
    }
    return new SqlWithItem(item.getParserPosition(), item.name,
        new SqlNodeList(outputNames, POS), query, item.recursive);
  }

  /** Checks the recursive query's shape without changing its syntax tree. */
  private Reference validateRecursiveQuery() {
    final SqlNode body = queryBody(item.query);
    if (body.getKind() != SqlKind.UNION
        || !(((SqlCall) body).operand(1) instanceof SqlSelect)) {
      throw invalidRecursiveQuery();
    }
    final SqlSelect step = ((SqlCall) body).operand(1);
    final List<SqlNode> references = new ArrayList<>();
    findDirectReferences(step.getFrom(), references);
    if (references.size() != 1 || recursiveReferenceCount() != 1) {
      throw invalidRecursiveQuery();
    }
    if (validator.isAggregate(step) || step.isDistinct() || !step.getWindowList().isEmpty()
        || SqlUtil.containsCall(step, call -> call.getKind() == SqlKind.OVER)) {
      throw search != null
          ? validator.newValidationError(search, RESOURCE.searchRecursiveSelectNotSupported())
          : validator.newValidationError(requireNonNull(cycle, "cycle"),
              RESOURCE.cycleRecursiveSelectNotSupported());
    }
    return reference(references.get(0));
  }

  private RuntimeException invalidRecursiveQuery() {
    return search != null
        ? validator.newValidationError(search, RESOURCE.searchRequiresRecursiveSelect())
        : validator.newValidationError(requireNonNull(cycle, "cycle"),
            RESOURCE.cycleRequiresRecursiveSelect());
  }

  /** Finds direct references on the non-null-generating sides of joins. */
  private void findDirectReferences(@Nullable SqlNode from, List<SqlNode> references) {
    if (isSelfReference(from)) {
      references.add(from);
    } else if (from instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) from;
      if (!join.getJoinType().generatesNullsOnLeft()) {
        findDirectReferences(join.getLeft(), references);
      }
      if (!join.getJoinType().generatesNullsOnRight()) {
        findDirectReferences(join.getRight(), references);
      }
    } else if (from != null && from.getKind() == SqlKind.AS
        && isSelfReference(((SqlCall) from).operand(0))) {
      references.add(from);
    }
  }

  @EnsuresNonNullIf(expression = "#1", result = true)
  private boolean isSelfReference(@Nullable SqlNode node) {
    return node instanceof SqlIdentifier && ((SqlIdentifier) node).isSimple()
        && matcher.matches(((SqlIdentifier) node).getSimple(), item.name.getSimple());
  }

  /** Counts resolved references as well, to catch references hidden in subqueries. */
  private int recursiveReferenceCount() {
    final List<SqlIdentifier> references = new ArrayList<>();
    item.query.accept(new SqlBasicVisitor<Void>() {
      @Override public Void visit(SqlIdentifier id) {
        final SqlValidatorNamespace ns = validator.getNamespace(id);
        if (ns != null) {
          final SqlNode node = ns.resolve().getNode();
          if (node instanceof SqlWithItemTableRef
              && ((SqlWithItemTableRef) node).getWithItem() == item) {
            references.add(id);
          }
        }
        return null;
      }
    });
    return references.size();
  }

  private List<RelDataTypeField> validateKeys(RelDataType rowType,
      SqlNodeList columns, boolean searchKeys) {
    final List<RelDataTypeField> keys = new ArrayList<>();
    for (SqlNode node : columns) {
      final SqlIdentifier key = (SqlIdentifier) node;
      final RelDataTypeField field = matcher.field(rowType, key.getSimple());
      if (field == null) {
        throw validator.newValidationError(key,
            searchKeys ? RESOURCE.searchColumnNotFound(key.getSimple(), item.name.getSimple())
                : RESOURCE.cycleColumnNotFound(key.getSimple(), item.name.getSimple()));
      }
      if (keys.contains(field)) {
        throw validator.newValidationError(key,
            RESOURCE.duplicateNameInColumnList(key.getSimple()));
      }
      keys.add(field);
    }
    return keys;
  }

  private void validateGeneratedNames(List<String> names) {
    final Set<String> used = matcher.createSet();
    used.addAll(names);
    for (SqlIdentifier generated : generatedColumns()) {
      if (!used.add(generated.getSimple())) {
        throw validator.newValidationError(generated,
            search != null && generated == search.orderColumn
                ? RESOURCE.searchColumnConflict(generated.getSimple())
                : RESOURCE.cycleColumnConflict(generated.getSimple()));
      }
    }
  }

  private List<SqlIdentifier> generatedColumns() {
    final List<SqlIdentifier> columns = new ArrayList<>();
    if (search != null) {
      columns.add(search.orderColumn);
    }
    if (cycle != null) {
      columns.add(cycle.markColumn);
      columns.add(cycle.pathColumn);
    }
    return columns;
  }

  private RelDataType validateMarkType(SqlValidatorScope scope) {
    final SqlCycleClause cycle = requireNonNull(this.cycle);
    if (!(cycle.markValue instanceof SqlLiteral)
        || !(cycle.defaultValue instanceof SqlLiteral)) {
      throw validator.newValidationError(cycle, RESOURCE.cycleInvalidMarkValues());
    }
    final Object mark = ((SqlLiteral) cycle.markValue).getValue();
    final Object defaultMark = ((SqlLiteral) cycle.defaultValue).getValue();
    final boolean valid;
    if (mark instanceof Boolean && defaultMark instanceof Boolean) {
      valid = !mark.equals(defaultMark);
    } else if (mark instanceof NlsString && defaultMark instanceof NlsString) {
      final String markString = ((NlsString) mark).getValue();
      final String defaultString = ((NlsString) defaultMark).getValue();
      valid = markString.length() == 1 && defaultString.length() == 1
          && !markString.equals(defaultString);
    } else {
      valid = false;
    }
    if (!valid) {
      throw validator.newValidationError(cycle, RESOURCE.cycleInvalidMarkValues());
    }
    final RelDataType type =
        validator.getTypeFactory().leastRestrictive(
            ImmutableList.of(validator.deriveType(scope, cycle.markValue),
                validator.deriveType(scope, cycle.defaultValue)));
    if (type == null) {
      throw validator.newValidationError(cycle, RESOURCE.cycleInvalidMarkValues());
    }
    return type;
  }

  /** Chooses generated column names, accounting for an explicit alias column list. */
  private Reference reference(SqlNode source) {
    final SqlCall as = source instanceof SqlCall ? (SqlCall) source : null;
    final String alias = requireNonNull(SqlValidatorUtil.alias(source), "alias");
    final Set<String> aliases = matcher.createSet();
    if (as != null && as.operandCount() > 2) {
      aliases.addAll(
          SqlIdentifier.simpleNames(as.getOperandList().subList(2, as.operandCount())));
    }
    final List<SqlIdentifier> columns = new ArrayList<>();
    for (SqlIdentifier generated : generatedColumns()) {
      columns.add(
          qualified(alias,
          SqlValidatorUtil.uniquify(generated.getSimple(), aliases, EXPR_SUGGESTER)));
    }
    return new Reference(source, columns);
  }

  private SqlIdentifier referenceColumn(Reference reference, SqlIdentifier column) {
    return reference.columns.get(generatedColumns().indexOf(column));
  }

  /** Copies the query and adjusts joins and aliases before adding generated columns. */
  private SqlNode prepareQuery(Reference reference) {
    final SqlSelect step = ((SqlCall) queryBody(item.query)).operand(1);
    final SqlNodeList stepSelectList = step.getSelectList();
    final SelectScope stepScope = requireNonNull(validator.getRawSelectScope(step));
    // Validation saves the expanded projection even if identifier expansion is disabled.
    final SqlNodeList expandedSelectList =
        new SqlNodeList(requireNonNull(stepScope.getExpandedSelectList()),
            stepSelectList.getParserPosition());
    final SqlShuttle shuttle = new DeepCopier() {
      @Override public SqlNode visit(SqlNodeList nodeList) {
        return requireNonNull(
            super.visit(nodeList == stepSelectList ? expandedSelectList : nodeList));
      }

      @Override public SqlNode visit(SqlCall call) {
        final SqlCall result = (SqlCall) super.visit(call);
        if (call instanceof SqlJoin && ((SqlJoin) call).isNatural()) {
          makeJoinExplicit((SqlJoin) call, (SqlJoin) result);
        }
        if (call == reference.source && call.operandCount() > 2) {
          final List<SqlNode> operands = new ArrayList<>(result.getOperandList());
          for (SqlIdentifier column : reference.columns) {
            operands.add(new SqlIdentifier(Util.last(column.names), POS));
          }
          return result.getOperator().createCall(result.getParserPosition(), operands);
        }
        return result;
      }
    };
    return requireNonNull(item.query.accept(shuttle));
  }

  private void makeJoinExplicit(SqlJoin original, SqlJoin copy) {
    final List<String> names = requireNonNull(validator.usingNames(original));
    copy.setOperand(1, SqlLiteral.createBoolean(false, POS));
    copy.setOperand(4,
        (names.isEmpty() ? JoinConditionType.ON : JoinConditionType.USING).symbol(POS));
    copy.setOperand(5, names.isEmpty() ? SqlLiteral.createBoolean(true, POS)
        : new SqlNodeList(identifiers(names), POS));
  }

  private SqlSelect rewriteSeed(SqlNode seed, List<String> names,
      List<RelDataTypeField> cycleKeys, List<RelDataTypeField> searchKeys,
      @Nullable RelDataType markType) {
    final List<SqlNode> columns = columns(names);
    if (search != null) {
      columns.add(searchOrder(searchKeys, true));
    }
    if (cycle != null) {
      columns.add(cast(copy(cycle.defaultValue), requireNonNull(markType, "markType")));
      columns.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(POS, key(cycleKeys)));
    }
    return select(columns, alias(seed, names));
  }

  private SqlSelect rewriteStep(SqlSelect step, Reference reference, List<String> names,
      List<RelDataTypeField> cycleKeys, List<RelDataTypeField> searchKeys,
      @Nullable RelDataType markType) {
    final List<String> inputNames = new ArrayList<>(names);
    final List<SqlNode> columns = columns(names);
    final List<SqlNode> stepColumns = new ArrayList<>(step.getSelectList());
    if (search != null) {
      inputNames.add(search.orderColumn.getSimple());
      stepColumns.add(referenceColumn(reference, search.orderColumn));
      columns.add(searchOrder(searchKeys, false));
    }
    if (cycle != null) {
      excludeCompletedPaths(step, reference, requireNonNull(markType, "markType"));
      inputNames.add(cycle.pathColumn.getSimple());
      stepColumns.add(referenceColumn(reference, cycle.pathColumn));
      columns.add(cycleMark(cycleKeys, requireNonNull(markType, "markType")));
      columns.add(
          SqlInternalOperators.RECURSIVE_PATH_APPEND.createCall(POS,
              previousPath(), key(cycleKeys)));
    }
    step.setSelectList(new SqlNodeList(stepColumns, POS));
    return select(columns, alias(step, inputNames));
  }

  /** Filters closed paths before evaluating the user's projection. */
  private void excludeCompletedPaths(SqlSelect step, Reference reference, RelDataType markType) {
    final SqlCycleClause cycle = requireNonNull(this.cycle);
    final SqlNode mark = cast(copy(cycle.markValue), markType);
    final SqlNode live =
        SqlStdOperatorTable.NOT_EQUALS.createCall(POS,
            referenceColumn(reference, cycle.markColumn), mark);
    step.setWhere(step.getWhere() == null ? live
        : SqlStdOperatorTable.AND.createCall(POS, step.getWhere(), live));
  }

  private SqlNode cycleMark(List<RelDataTypeField> keys, RelDataType markType) {
    final SqlCycleClause cycle = requireNonNull(this.cycle);
    final SqlNode contains =
        SqlInternalOperators.CYCLE_PATH_CONTAINS.createCall(POS, previousPath(), key(keys));
    return new SqlCase(POS, null, SqlNodeList.of(contains),
        SqlNodeList.of(cast(copy(cycle.markValue), markType)),
        cast(copy(cycle.defaultValue), markType));
  }

  private SqlIdentifier previousPath() {
    return qualified(INPUT, requireNonNull(cycle, "cycle").pathColumn.getSimple());
  }

  private SqlNode searchOrder(List<RelDataTypeField> keys, boolean seed) {
    final SqlSearchClause search = requireNonNull(this.search);
    final SqlIdentifier previous = qualified(INPUT, search.orderColumn.getSimple());
    if (!search.breadthFirst.booleanValue()) {
      return seed
          ? SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(POS, key(keys))
          : SqlInternalOperators.RECURSIVE_PATH_APPEND.createCall(POS, previous, key(keys));
    }
    // The private depth field cannot collide with a user's BY column name.
    final List<String> fieldNames = new ArrayList<>();
    final Set<String> usedNames = matcher.createSet();
    final List<SqlNode> values = new ArrayList<>();
    fieldNames.add(DEPTH);
    usedNames.add(DEPTH);
    values.add(seed ? SqlLiteral.createExactNumeric("0", POS)
        : SqlStdOperatorTable.PLUS.createCall(POS, previous.plus(DEPTH, POS),
            SqlLiteral.createExactNumeric("1", POS)));
    for (RelDataTypeField field : keys) {
      fieldNames.add(SqlValidatorUtil.uniquify(field.getName(), usedNames, EXPR_SUGGESTER));
      values.add(cast(qualified(INPUT, field.getName()), field.getType()));
    }
    return new SqlRowOperator("ROW", fieldNames).createCall(POS, values);
  }

  private static SqlNode queryBody(SqlNode query) {
    while (query instanceof SqlWith) {
      query = ((SqlWith) query).body;
    }
    return query;
  }

  private static SqlNode key(List<RelDataTypeField> keys) {
    return new SqlRowOperator("ROW", Util.transform(keys, RelDataTypeField::getName))
        .createCall(POS,
        Util.transform(keys, field -> cast(qualified(INPUT, field.getName()), field.getType())));
  }

  private static SqlNode cast(SqlNode value, RelDataType type) {
    final SqlNode typeSpec = SqlTypeUtil.isInterval(type)
        ? SqlNode.clone(type.getIntervalQualifier())
        : SqlTypeUtil.convertTypeToSpec(type);
    return SqlStdOperatorTable.CAST.createCall(POS, value, typeSpec);
  }

  private static SqlIdentifier qualified(String table, String column) {
    return new SqlIdentifier(ImmutableList.of(table, column), POS);
  }

  private static List<SqlNode> columns(List<String> names) {
    return new ArrayList<>(Util.transform(names, name -> qualified(INPUT, name)));
  }

  private static List<SqlNode> identifiers(List<String> names) {
    return new ArrayList<>(Util.transform(names, name -> new SqlIdentifier(name, POS)));
  }

  private static SqlNode alias(SqlNode query, List<String> names) {
    final List<SqlNode> operands = new ArrayList<>();
    operands.add(query);
    operands.add(new SqlIdentifier(INPUT, POS));
    operands.addAll(identifiers(names));
    return SqlStdOperatorTable.AS.createCall(POS, operands);
  }

  private static SqlSelect select(List<SqlNode> columns, SqlNode from) {
    return new SqlSelect(POS, null, new SqlNodeList(columns, POS), from,
        null, null, null, null, null, null, null, null, null);
  }

  private static SqlNode copy(SqlNode node) {
    return requireNonNull(node.accept(new DeepCopier()));
  }

  /** Original recursive table reference and its qualified generated columns. */
  private static class Reference {
    final SqlNode source;
    final List<SqlIdentifier> columns;

    Reference(SqlNode source, List<SqlIdentifier> columns) {
      this.source = source;
      this.columns = columns;
    }
  }

  /** Copies syntax nodes without reusing the validator's cached call types. */
  private static class DeepCopier extends SqlShuttle {
    @Override public SqlNode visit(SqlIdentifier id) {
      return SqlNode.clone(id);
    }

    @Override public SqlNode visit(SqlLiteral literal) {
      return SqlNode.clone(literal);
    }

    @Override public SqlNode visit(SqlCall call) {
      final CallCopyingArgHandler handler = new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, handler);
      return handler.result();
    }
  }
}
