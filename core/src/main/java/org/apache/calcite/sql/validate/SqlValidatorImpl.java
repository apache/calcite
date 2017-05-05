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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Default implementation of {@link SqlValidator}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {
  //~ Static fields/initializers ---------------------------------------------

  public static final Logger TRACER = CalciteTrace.PARSER_LOGGER;

  /**
   * Alias generated for the source table when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

  /**
   * Alias generated for the target table when rewriting UPDATE to MERGE if no
   * alias was specified by the user.
   */
  public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

  /**
   * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_ANON_PREFIX = "SYS$ANON";

  //~ Instance fields --------------------------------------------------------

  private final SqlOperatorTable opTab;
  final SqlValidatorCatalogReader catalogReader;

  /**
   * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
   * objects at these positions
   */
  protected final Map<String, IdInfo> idPositions = new HashMap<>();

  /**
   * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
   * scope created from them}.
   */
  protected final Map<SqlNode, SqlValidatorScope> scopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> whereScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its GROUP BY clause.
   */
  private final Map<SqlSelect, SqlValidatorScope> groupByScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> selectScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
   */
  private final Map<SqlSelect, SqlValidatorScope> orderScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node that is the argument to a CURSOR
   * constructor to the scope of the result of that select node
   */
  private final Map<SqlSelect, SqlValidatorScope> cursorScopes =
      new IdentityHashMap<>();

  /**
   * The name-resolution scope of a LATERAL TABLE clause.
   */
  private TableScope tableScope = null;

  /**
   * Maps a {@link SqlNode node} to the
   * {@link SqlValidatorNamespace namespace} which describes what columns they
   * contain.
   */
  protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
      new IdentityHashMap<>();

  /**
   * Set of select expressions used as cursor definitions. In standard SQL,
   * only the top-level SELECT is a cursor; Calcite extends this with
   * cursors as inputs to table functions.
   */
  private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

  /**
   * Stack of objects that maintain information about function calls. A stack
   * is needed to handle nested function calls. The function call currently
   * being validated is at the top of the stack.
   */
  protected final Deque<FunctionParamInfo> functionCallStack =
      new ArrayDeque<>();

  private int nextGeneratedId;
  protected final RelDataTypeFactory typeFactory;
  protected final RelDataType unknownType;
  private final RelDataType booleanType;

  /**
   * Map of derived RelDataType for each node. This is an IdentityHashMap
   * since in some cases (such as null literals) we need to discriminate by
   * instance.
   */
  private final Map<SqlNode, RelDataType> nodeToTypeMap =
      new IdentityHashMap<>();
  private final AggFinder aggFinder;
  private final AggFinder aggOrOverFinder;
  private final AggFinder aggOrOverOrGroupFinder;
  private final AggFinder groupFinder;
  private final AggFinder overFinder;
  private final SqlConformance conformance;
  private final Map<SqlNode, SqlNode> originalExprs = new HashMap<>();

  private SqlNode top;

  // REVIEW jvs 30-June-2006: subclasses may override shouldExpandIdentifiers
  // in a way that ignores this; we should probably get rid of the protected
  // method and always use this variable (or better, move preferences like
  // this to a separate "parameter" class)
  protected boolean expandIdentifiers;

  protected boolean expandColumnReferences;

  private boolean rewriteCalls;

  private NullCollation nullCollation = NullCollation.HIGH;

  // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
  // if it's OK to expand the signature of that method.
  private boolean validatingSqlMerge;

  private boolean inWindow;                        // Allow nested aggregates

  private final SqlValidatorImpl.ValidationErrorFunction validationErrorFunction =
      new SqlValidatorImpl.ValidationErrorFunction();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param conformance   Compatibility mode
   */
  protected SqlValidatorImpl(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    this.opTab = Preconditions.checkNotNull(opTab);
    this.catalogReader = Preconditions.checkNotNull(catalogReader);
    this.typeFactory = Preconditions.checkNotNull(typeFactory);
    this.conformance = Preconditions.checkNotNull(conformance);

    // NOTE jvs 23-Dec-2003:  This is used as the type for dynamic
    // parameters and null literals until a real type is imposed for them.
    unknownType = typeFactory.createSqlType(SqlTypeName.NULL);
    booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    rewriteCalls = true;
    expandColumnReferences = true;
    aggFinder = new AggFinder(opTab, false, true, false, null);
    aggOrOverFinder = new AggFinder(opTab, true, true, false, null);
    overFinder = new AggFinder(opTab, true, false, false, aggOrOverFinder);
    groupFinder = new AggFinder(opTab, false, false, true, null);
    aggOrOverOrGroupFinder = new AggFinder(opTab, true, true, true, null);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlConformance getConformance() {
    return conformance;
  }

  public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  public SqlOperatorTable getOperatorTable() {
    return opTab;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RelDataType getUnknownType() {
    return unknownType;
  }

  public SqlNodeList expandStar(
      SqlNodeList selectList,
      SqlSelect select,
      boolean includeSystemVars) {
    final List<SqlNode> list = new ArrayList<>();
    final List<Map.Entry<String, RelDataType>> types = new ArrayList<>();
    for (int i = 0; i < selectList.size(); i++) {
      final SqlNode selectItem = selectList.get(i);
      expandSelectItem(
          selectItem,
          select,
          unknownType,
          list,
          catalogReader.nameMatcher().isCaseSensitive()
              ? new LinkedHashSet<String>()
              : new TreeSet<>(String.CASE_INSENSITIVE_ORDER),
          types,
          includeSystemVars);
    }
    getRawSelectScope(select).setExpandedSelectList(list);
    return new SqlNodeList(list, SqlParserPos.ZERO);
  }

  // implement SqlValidator
  public void declareCursor(SqlSelect select, SqlValidatorScope parentScope) {
    cursorSet.add(select);

    // add the cursor to a map that maps the cursor to its select based on
    // the position of the cursor relative to other cursors in that call
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
    int numCursors = cursorMap.size();
    cursorMap.put(numCursors, select);

    // create a namespace associated with the result of the select
    // that is the argument to the cursor constructor; register it
    // with a scope corresponding to the cursor
    SelectScope cursorScope = new SelectScope(parentScope, null, select);
    cursorScopes.put(select, cursorScope);
    final SelectNamespace selectNs = createSelectNamespace(select, select);
    String alias = deriveAlias(select, nextGeneratedId++);
    registerNamespace(cursorScope, alias, selectNs, false);
  }

  // implement SqlValidator
  public void pushFunctionCall() {
    FunctionParamInfo funcInfo = new FunctionParamInfo();
    functionCallStack.push(funcInfo);
  }

  // implement SqlValidator
  public void popFunctionCall() {
    functionCallStack.pop();
  }

  // implement SqlValidator
  public String getParentCursor(String columnListParamName) {
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<String, String> parentCursorMap =
        funcParamInfo.columnListParamToParentCursorMap;
    return parentCursorMap.get(columnListParamName);
  }

  /**
   * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
   * true; otherwise writes the unexpanded item.
   *
   * @param selectItem        Select-list item
   * @param select            Containing select clause
   * @param selectItems       List that expanded items are written to
   * @param aliases           Set of aliases
   * @param types             List of data types in alias order
   * @param includeSystemVars If true include system vars in lists
   * @return Whether the node was expanded
   */
  private boolean expandSelectItem(
      final SqlNode selectItem,
      SqlSelect select,
      RelDataType targetType,
      List<SqlNode> selectItems,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> types,
      final boolean includeSystemVars) {
    final SelectScope scope = (SelectScope) getWhereScope(select);
    if (expandStar(selectItems, aliases, types, includeSystemVars, scope,
        selectItem)) {
      return true;
    }

    // Expand the select item: fully-qualify columns, and convert
    // parentheses-free functions such as LOCALTIME into explicit function
    // calls.
    SqlNode expanded = expand(selectItem, scope);
    final String alias =
        deriveAlias(
            selectItem,
            aliases.size());

    // If expansion has altered the natural alias, supply an explicit 'AS'.
    final SqlValidatorScope selectScope = getSelectScope(select);
    if (expanded != selectItem) {
      String newAlias =
          deriveAlias(
              expanded,
              aliases.size());
      if (!newAlias.equals(alias)) {
        expanded =
            SqlStdOperatorTable.AS.createCall(
                selectItem.getParserPosition(),
                expanded,
                new SqlIdentifier(alias, SqlParserPos.ZERO));
        deriveTypeImpl(selectScope, expanded);
      }
    }

    selectItems.add(expanded);
    aliases.add(alias);

    inferUnknownTypes(targetType, scope, expanded);
    final RelDataType type = deriveType(selectScope, expanded);
    setValidatedNodeType(expanded, type);
    types.add(Pair.of(alias, type));
    return false;
  }

  private boolean expandStar(List<SqlNode> selectItems, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> types, boolean includeSystemVars,
      SelectScope scope, SqlNode node) {
    if (!(node instanceof SqlIdentifier)) {
      return false;
    }
    final SqlIdentifier identifier = (SqlIdentifier) node;
    if (!identifier.isStar()) {
      return false;
    }
    final SqlParserPos startPosition = identifier.getParserPosition();
    switch (identifier.names.size()) {
    case 1:
      for (ScopeChild child : scope.children) {
        final int before = types.size();
        if (child.namespace.getRowType().isDynamicStruct()) {
          // don't expand star if the underneath table is dynamic.
          // Treat this star as a special field in validation/conversion and
          // wait until execution time to expand this star.
          final SqlNode exp =
              new SqlIdentifier(
                  ImmutableList.of(child.name,
                      DynamicRecordType.DYNAMIC_STAR_PREFIX),
                  startPosition);
          addToSelectList(
               selectItems,
               aliases,
               types,
               exp,
               scope,
               includeSystemVars);
        } else {
          final SqlNode from = child.namespace.getNode();
          final SqlValidatorNamespace fromNs = getNamespace(from, scope);
          assert fromNs != null;
          final RelDataType rowType = fromNs.getRowType();
          for (RelDataTypeField field : rowType.getFieldList()) {
            String columnName = field.getName();

            // TODO: do real implicit collation here
            final SqlIdentifier exp =
                new SqlIdentifier(
                    ImmutableList.of(child.name, columnName),
                    startPosition);
            addOrExpandField(
                selectItems,
                aliases,
                types,
                includeSystemVars,
                scope,
                exp,
                field);
          }
        }
        if (child.nullable) {
          for (int i = before; i < types.size(); i++) {
            final Map.Entry<String, RelDataType> entry = types.get(i);
            final RelDataType type = entry.getValue();
            if (!type.isNullable()) {
              types.set(i,
                  Pair.of(entry.getKey(),
                      typeFactory.createTypeWithNullability(type, true)));
            }
          }
        }
      }
      return true;

    default:
      final SqlIdentifier prefixId = identifier.skipLast(1);
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      final SqlNameMatcher nameMatcher =
          scope.validator.catalogReader.nameMatcher();
      scope.resolve(prefixId.names, nameMatcher, true, resolved);
      if (resolved.count() == 0) {
        // e.g. "select s.t.* from e"
        // or "select r.* from e"
        throw newValidationError(prefixId,
            RESOURCE.unknownIdentifier(prefixId.toString()));
      }
      final RelDataType rowType = resolved.only().rowType();
      if (rowType.isDynamicStruct()) {
        // don't expand star if the underneath table is dynamic.
        addToSelectList(
            selectItems,
            aliases,
            types,
            prefixId.plus(DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition),
            scope,
            includeSystemVars);
      } else if (rowType.isStruct()) {
        for (RelDataTypeField field : rowType.getFieldList()) {
          String columnName = field.getName();

          // TODO: do real implicit collation here
          addOrExpandField(
              selectItems,
              aliases,
              types,
              includeSystemVars,
              scope,
              prefixId.plus(columnName, startPosition),
              field);
        }
      } else {
        throw newValidationError(prefixId, RESOURCE.starRequiresRecordType());
      }
      return true;
    }
  }

  private boolean addOrExpandField(List<SqlNode> selectItems, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> types, boolean includeSystemVars,
      SelectScope scope, SqlIdentifier id, RelDataTypeField field) {
    switch (field.getType().getStructKind()) {
    case PEEK_FIELDS:
    case PEEK_FIELDS_DEFAULT:
      final SqlNode starExp = id.plusStar();
      expandStar(
          selectItems,
          aliases,
          types,
          includeSystemVars,
          scope,
          starExp);
      return true;

    default:
      addToSelectList(
          selectItems,
          aliases,
          types,
          id,
          scope,
          includeSystemVars);
    }

    return false;
  }

  public SqlNode validate(SqlNode topNode) {
    SqlValidatorScope scope = new EmptyScope(this);
    scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
    final SqlNode topNode2 = validateScopedExpression(topNode, scope);
    final RelDataType type = getValidatedNodeType(topNode2);
    Util.discard(type);
    return topNode2;
  }

  public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos) {
    SqlValidatorScope scope = new EmptyScope(this);
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    cursorSet.add(outermostNode);
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(
          scope,
          null,
          outermostNode,
          outermostNode,
          null,
          false);
    }
    final SqlValidatorNamespace ns = getNamespace(outermostNode);
    if (ns == null) {
      throw new AssertionError("Not a query: " + outermostNode);
    }
    Collection<SqlMoniker> hintList = Sets.newTreeSet(SqlMoniker.COMPARATOR);
    lookupSelectHints(ns, pos, hintList);
    return ImmutableList.copyOf(hintList);
  }

  public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos) {
    final String posString = pos.toString();
    IdInfo info = idPositions.get(posString);
    if (info != null) {
      final SqlQualified qualified = info.scope.fullyQualify(info.id);
      return new SqlIdentifierMoniker(qualified.identifier);
    } else {
      return null;
    }
  }

  /**
   * Looks up completion hints for a syntactically correct select SQL that has
   * been parsed into an expression tree.
   *
   * @param select   the Select node of the parsed expression tree
   * @param pos      indicates the position in the sql statement we want to get
   *                 completion hints for
   * @param hintList list of {@link SqlMoniker} (sql identifiers) that can
   *                 fill in at the indicated position
   */
  void lookupSelectHints(
      SqlSelect select,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    IdInfo info = idPositions.get(pos.toString());
    if ((info == null) || (info.scope == null)) {
      SqlNode fromNode = select.getFrom();
      final SqlValidatorScope fromScope = getFromScope(select);
      lookupFromHints(fromNode, fromScope, pos, hintList);
    } else {
      lookupNameCompletionHints(info.scope, info.id.names,
          info.id.getParserPosition(), hintList);
    }
  }

  private void lookupSelectHints(
      SqlValidatorNamespace ns,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    final SqlNode node = ns.getNode();
    if (node instanceof SqlSelect) {
      lookupSelectHints((SqlSelect) node, pos, hintList);
    }
  }

  private void lookupFromHints(
      SqlNode node,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns.isWrapperFor(IdentifierNamespace.class)) {
      IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
      final SqlIdentifier id = idNs.getId();
      for (int i = 0; i < id.names.size(); i++) {
        if (pos.toString().equals(
            id.getComponent(i).getParserPosition().toString())) {
          final List<SqlMoniker> objNames = new ArrayList<>();
          SqlValidatorUtil.getSchemaObjectMonikers(
              getCatalogReader(),
              id.names.subList(0, i + 1),
              objNames);
          for (SqlMoniker objName : objNames) {
            if (objName.getType() != SqlMonikerType.FUNCTION) {
              hintList.add(objName);
            }
          }
          return;
        }
      }
    }
    switch (node.getKind()) {
    case JOIN:
      lookupJoinHints((SqlJoin) node, scope, pos, hintList);
      break;
    default:
      lookupSelectHints(ns, pos, hintList);
      break;
    }
  }

  private void lookupJoinHints(
      SqlJoin join,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    lookupFromHints(left, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    lookupFromHints(right, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    switch (conditionType) {
    case ON:
      condition.findValidOptions(this, joinScope, pos, hintList);
      return;
    default:

      // No suggestions.
      // Not supporting hints for other types such as 'Using' yet.
      return;
    }
  }

  /**
   * Populates a list of all the valid alternatives for an identifier.
   *
   * @param scope    Validation scope
   * @param names    Components of the identifier
   * @param pos      position
   * @param hintList a list of valid options
   */
  public final void lookupNameCompletionHints(
      SqlValidatorScope scope,
      List<String> names,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    // Remove the last part of name - it is a dummy
    List<String> subNames = Util.skipLast(names);

    if (subNames.size() > 0) {
      // If there's a prefix, resolve it to a namespace.
      SqlValidatorNamespace ns = null;
      for (String name : subNames) {
        if (ns == null) {
          final SqlValidatorScope.ResolvedImpl resolved =
              new SqlValidatorScope.ResolvedImpl();
          final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
          scope.resolve(ImmutableList.of(name), nameMatcher, false, resolved);
          if (resolved.count() == 1) {
            ns = resolved.only().namespace;
          }
        } else {
          ns = ns.lookupChild(name);
        }
        if (ns == null) {
          break;
        }
      }
      if (ns != null) {
        RelDataType rowType = ns.getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
          hintList.add(
              new SqlMonikerImpl(
                  field.getName(),
                  SqlMonikerType.COLUMN));
        }
      }

      // builtin function names are valid completion hints when the
      // identifier has only 1 name part
      findAllValidFunctionNames(names, this, hintList, pos);
    } else {
      // No prefix; use the children of the current scope (that is,
      // the aliases in the FROM clause)
      scope.findAliases(hintList);

      // If there's only one alias, add all child columns
      SelectScope selectScope =
          SqlValidatorUtil.getEnclosingSelectScope(scope);
      if ((selectScope != null)
          && (selectScope.getChildren().size() == 1)) {
        RelDataType rowType =
            selectScope.getChildren().get(0).getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
          hintList.add(
              new SqlMonikerImpl(
                  field.getName(),
                  SqlMonikerType.COLUMN));
        }
      }
    }

    findAllValidUdfNames(names, this, hintList);
  }

  private static void findAllValidUdfNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result) {
    final List<SqlMoniker> objNames = new ArrayList<>();
    SqlValidatorUtil.getSchemaObjectMonikers(
        validator.getCatalogReader(),
        names,
        objNames);
    for (SqlMoniker objName : objNames) {
      if (objName.getType() == SqlMonikerType.FUNCTION) {
        result.add(objName);
      }
    }
  }

  private static void findAllValidFunctionNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result,
      SqlParserPos pos) {
    // a function name can only be 1 part
    if (names.size() > 1) {
      return;
    }
    for (SqlOperator op : validator.getOperatorTable().getOperatorList()) {
      SqlIdentifier curOpId =
          new SqlIdentifier(
              op.getName(),
              pos);

      final SqlCall call =
          SqlUtil.makeCall(
              validator.getOperatorTable(),
              curOpId);
      if (call != null) {
        result.add(
            new SqlMonikerImpl(
                op.getName(),
                SqlMonikerType.FUNCTION));
      } else {
        if ((op.getSyntax() == SqlSyntax.FUNCTION)
            || (op.getSyntax() == SqlSyntax.PREFIX)) {
          if (op.getOperandTypeChecker() != null) {
            String sig = op.getAllowedSignatures();
            sig = sig.replaceAll("'", "");
            result.add(
                new SqlMonikerImpl(
                    sig,
                    SqlMonikerType.FUNCTION));
            continue;
          }
          result.add(
              new SqlMonikerImpl(
                  op.getName(),
                  SqlMonikerType.FUNCTION));
        }
      }
    }
  }

  public SqlNode validateParameterizedExpression(
      SqlNode topNode,
      final Map<String, RelDataType> nameToTypeMap) {
    SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
    return validateScopedExpression(topNode, scope);
  }

  private SqlNode validateScopedExpression(
      SqlNode topNode,
      SqlValidatorScope scope) {
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    cursorSet.add(outermostNode);
    top = outermostNode;
    TRACER.trace("After unconditional rewrite: " + outermostNode.toString());
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(scope, null, outermostNode, outermostNode, null, false);
    }
    outermostNode.validate(this, scope);
    if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
      // force type derivation so that we can provide it to the
      // caller later without needing the scope
      deriveType(scope, outermostNode);
    }
    TRACER.trace("After validation: " + outermostNode.toString());
    return outermostNode;
  }

  public void validateQuery(SqlNode node, SqlValidatorScope scope,
      RelDataType targetRowType) {
    final SqlValidatorNamespace ns = getNamespace(node, scope);
    if (node.getKind() == SqlKind.TABLESAMPLE) {
      List<SqlNode> operands = ((SqlCall) node).getOperandList();
      SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands.get(1));
      if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
        validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition());
      } else if (sampleSpec
          instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
        validateFeature(RESOURCE.sQLFeatureExt_T613_Substitution(),
            node.getParserPosition());
      }
    }

    validateNamespace(ns, targetRowType);
    if (node == top) {
      validateModality(node);
    }
    validateAccess(
        node,
        ns.getTable(),
        SqlAccessEnum.SELECT);
  }

  /**
   * Validates a namespace.
   *
   * @param namespace Namespace
   * @param targetRowType Desired row type, must not be null, may be the data
   *                      type 'unknown'.
   */
  protected void validateNamespace(final SqlValidatorNamespace namespace,
      RelDataType targetRowType) {
    namespace.validate(targetRowType);
    if (namespace.getNode() != null) {
      setValidatedNodeType(namespace.getNode(), namespace.getType());
    }
  }

  @VisibleForTesting
  public SqlValidatorScope getEmptyScope() {
    return new EmptyScope(this);
  }

  public SqlValidatorScope getCursorScope(SqlSelect select) {
    return cursorScopes.get(select);
  }

  public SqlValidatorScope getWhereScope(SqlSelect select) {
    return whereScopes.get(select);
  }

  public SqlValidatorScope getSelectScope(SqlSelect select) {
    return selectScopes.get(select);
  }

  public SelectScope getRawSelectScope(SqlSelect select) {
    SqlValidatorScope scope = getSelectScope(select);
    if (scope instanceof AggregatingSelectScope) {
      scope = ((AggregatingSelectScope) scope).getParent();
    }
    return (SelectScope) scope;
  }

  public SqlValidatorScope getHavingScope(SqlSelect select) {
    // Yes, it's the same as getSelectScope
    return selectScopes.get(select);
  }

  public SqlValidatorScope getGroupScope(SqlSelect select) {
    // Yes, it's the same as getWhereScope
    return groupByScopes.get(select);
  }

  public SqlValidatorScope getFromScope(SqlSelect select) {
    return scopes.get(select);
  }

  public SqlValidatorScope getOrderScope(SqlSelect select) {
    return orderScopes.get(select);
  }

  public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
    return scopes.get(node);
  }

  public SqlValidatorScope getJoinScope(SqlNode node) {
    return scopes.get(stripAs(node));
  }

  public SqlValidatorScope getOverScope(SqlNode node) {
    return scopes.get(node);
  }

  private SqlValidatorNamespace getNamespace(SqlNode node,
      SqlValidatorScope scope) {
    if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
      final SqlIdentifier id = (SqlIdentifier) node;
      final DelegatingScope idScope = (DelegatingScope) ((DelegatingScope) scope).getParent();
      return getNamespace(id, idScope);
    } else if (node instanceof SqlCall) {
      // Handle extended identifiers.
      final SqlCall sqlCall = (SqlCall) node;
      final SqlKind sqlKind = sqlCall.getOperator().getKind();
      if (sqlKind.equals(SqlKind.EXTEND)) {
        final SqlIdentifier id = (SqlIdentifier) sqlCall.getOperandList().get(0);
        final DelegatingScope idScope = (DelegatingScope) scope;
        return getNamespace(id, idScope);
      } else {
        final SqlNode nested = sqlCall.getOperandList().get(0);
        if (sqlKind.equals(SqlKind.AS)
            && nested.getKind().equals(SqlKind.EXTEND)) {
          return getNamespace(nested, scope);
        }
      }
    }
    return getNamespace(node);
  }

  private SqlValidatorNamespace getNamespace(SqlIdentifier id, DelegatingScope scope) {
    if (id.isSimple()) {
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      scope.resolve(id.names, nameMatcher, false, resolved);
      if (resolved.count() == 1) {
        return resolved.only().namespace;
      }
    }
    return getNamespace(id);
  }

  public SqlValidatorNamespace getNamespace(SqlNode node) {
    switch (node.getKind()) {
    case AS:

      // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
      final SqlValidatorNamespace ns = namespaces.get(node);
      if (ns != null) {
        return ns;
      }
      // fall through
    case OVER:
    case COLLECTION_TABLE:
    case ORDER_BY:
    case TABLESAMPLE:
      return getNamespace(((SqlCall) node).operand(0));
    default:
      return namespaces.get(node);
    }
  }

  /**
   * Performs expression rewrites which are always used unconditionally. These
   * rewrites massage the expression tree into a standard form so that the
   * rest of the validation logic can be simpler.
   *
   * @param node      expression to be rewritten
   * @param underFrom whether node appears directly under a FROM clause
   * @return rewritten expression
   */
  protected SqlNode performUnconditionalRewrites(
      SqlNode node,
      boolean underFrom) {
    if (node == null) {
      return node;
    }

    SqlNode newOperand;

    // first transform operands and invoke generic call rewrite
    if (node instanceof SqlCall) {
      if (node instanceof SqlMerge) {
        validatingSqlMerge = true;
      }
      SqlCall call = (SqlCall) node;
      final SqlKind kind = call.getKind();
      final List<SqlNode> operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        SqlNode operand = operands.get(i);
        boolean childUnderFrom;
        if (kind == SqlKind.SELECT) {
          childUnderFrom = i == SqlSelect.FROM_OPERAND;
        } else if (kind == SqlKind.AS && (i == 0)) {
          // for an aliased expression, it is under FROM if
          // the AS expression is under FROM
          childUnderFrom = underFrom;
        } else {
          childUnderFrom = false;
        }
        newOperand =
            performUnconditionalRewrites(operand, childUnderFrom);
        if (newOperand != null && newOperand != operand) {
          call.setOperand(i, newOperand);
        }
      }

      if (call.getOperator() instanceof SqlUnresolvedFunction) {
        assert call instanceof SqlBasicCall;
        final SqlUnresolvedFunction function =
            (SqlUnresolvedFunction) call.getOperator();
        // This function hasn't been resolved yet.  Perform
        // a half-hearted resolution now in case it's a
        // builtin function requiring special casing.  If it's
        // not, we'll handle it later during overload resolution.
        final List<SqlOperator> overloads = new ArrayList<>();
        opTab.lookupOperatorOverloads(function.getNameAsId(),
            function.getFunctionType(), SqlSyntax.FUNCTION, overloads);
        if (overloads.size() == 1) {
          ((SqlBasicCall) call).setOperator(overloads.get(0));
        }
      }
      if (rewriteCalls) {
        node = call.getOperator().rewriteCall(this, call);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode operand = list.get(i);
        newOperand =
            performUnconditionalRewrites(
                operand,
                false);
        if (newOperand != null) {
          list.getList().set(i, newOperand);
        }
      }
    }

    // now transform node itself
    final SqlKind kind = node.getKind();
    switch (kind) {
    case VALUES:
      // CHECKSTYLE: IGNORE 1
      if (underFrom || true) {
        // leave FROM (VALUES(...)) [ AS alias ] clauses alone,
        // otherwise they grow cancerously if this rewrite is invoked
        // over and over
        return node;
      } else {
        final SqlNodeList selectList =
            new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
        return new SqlSelect(node.getParserPosition(), null, selectList, node,
            null, null, null, null, null, null, null);
      }

    case ORDER_BY: {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      if (orderBy.query instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) orderBy.query;

        // Don't clobber existing ORDER BY.  It may be needed for
        // an order-sensitive function like RANK.
        if (select.getOrderList() == null) {
          // push ORDER BY into existing select
          select.setOrderBy(orderBy.orderList);
          select.setOffset(orderBy.offset);
          select.setFetch(orderBy.fetch);
          return select;
        }
      }
      if (orderBy.query instanceof SqlWith
          && ((SqlWith) orderBy.query).body instanceof SqlSelect) {
        SqlWith with = (SqlWith) orderBy.query;
        SqlSelect select = (SqlSelect) with.body;

        // Don't clobber existing ORDER BY.  It may be needed for
        // an order-sensitive function like RANK.
        if (select.getOrderList() == null) {
          // push ORDER BY into existing select
          select.setOrderBy(orderBy.orderList);
          select.setOffset(orderBy.offset);
          select.setFetch(orderBy.fetch);
          return with;
        }
      }
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
      final SqlNodeList orderList;
      if (getInnerSelect(node) != null && isAggregate(getInnerSelect(node))) {
        orderList =
            orderBy.orderList.clone(orderBy.orderList.getParserPosition());
        // We assume that ORDER BY item does not have ASC etc.
        // We assume that ORDER BY item is present in SELECT list.
        for (int i = 0; i < orderList.size(); i++) {
          SqlNode sqlNode = orderList.get(i);
          SqlNodeList selectList2 = getInnerSelect(node).getSelectList();
          for (Ord<SqlNode> sel : Ord.zip(selectList2)) {
            if (stripAs(sel.e).equalsDeep(sqlNode, Litmus.IGNORE)) {
              orderList.set(i,
                  SqlLiteral.createExactNumeric(Integer.toString(sel.i + 1),
                      SqlParserPos.ZERO));
            }
          }
        }
      } else {
        orderList = orderBy.orderList;
      }
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, orderBy.query,
          null, null, null, null, orderList, orderBy.offset,
          orderBy.fetch);
    }

    case EXPLICIT_TABLE: {
      // (TABLE t) is equivalent to (SELECT * FROM t)
      SqlCall call = (SqlCall) node;
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, call.operand(0),
          null, null, null, null, null, null, null);
    }

    case DELETE: {
      SqlDelete call = (SqlDelete) node;
      SqlSelect select = createSourceSelectForDelete(call);
      call.setSourceSelect(select);
      break;
    }

    case UPDATE: {
      SqlUpdate call = (SqlUpdate) node;
      SqlSelect select = createSourceSelectForUpdate(call);
      call.setSourceSelect(select);

      // See if we're supposed to rewrite UPDATE to MERGE
      // (unless this is the UPDATE clause of a MERGE,
      // in which case leave it alone).
      if (!validatingSqlMerge) {
        SqlNode selfJoinSrcExpr =
            getSelfJoinExprForUpdate(
                call.getTargetTable(),
                UPDATE_SRC_ALIAS);
        if (selfJoinSrcExpr != null) {
          node = rewriteUpdateToMerge(call, selfJoinSrcExpr);
        }
      }
      break;
    }

    case MERGE: {
      SqlMerge call = (SqlMerge) node;
      rewriteMerge(call);
      break;
    }
    }
    return node;
  }

  private SqlSelect getInnerSelect(SqlNode node) {
    for (;;) {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      } else if (node instanceof SqlOrderBy) {
        node = ((SqlOrderBy) node).query;
      } else if (node instanceof SqlWith) {
        node = ((SqlWith) node).body;
      } else {
        return null;
      }
    }
  }

  private void rewriteMerge(SqlMerge call) {
    SqlNodeList selectList;
    SqlUpdate updateStmt = call.getUpdateCall();
    if (updateStmt != null) {
      // if we have an update statement, just clone the select list
      // from the update statement's source since it's the same as
      // what we want for the select list of the merge source -- '*'
      // followed by the update set expressions
      selectList =
          (SqlNodeList) updateStmt.getSourceSelect().getSelectList()
              .clone();
    } else {
      // otherwise, just use select *
      selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    }
    SqlNode targetTable = call.getTargetTable();
    if (call.getAlias() != null) {
      targetTable =
          SqlValidatorUtil.addAlias(
              targetTable,
              call.getAlias().getSimple());
    }

    // Provided there is an insert substatement, the source select for
    // the merge is a left outer join between the source in the USING
    // clause and the target table; otherwise, the join is just an
    // inner join.  Need to clone the source table reference in order
    // for validation to work
    SqlNode sourceTableRef = call.getSourceTableRef();
    SqlInsert insertCall = call.getInsertCall();
    JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
    SqlNode leftJoinTerm = (SqlNode) sourceTableRef.clone();
    SqlNode outerJoin =
        new SqlJoin(SqlParserPos.ZERO,
            leftJoinTerm,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            joinType.symbol(SqlParserPos.ZERO),
            targetTable,
            JoinConditionType.ON.symbol(SqlParserPos.ZERO),
            call.getCondition());
    SqlSelect select =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, outerJoin, null,
            null, null, null, null, null, null);
    call.setSourceSelect(select);

    // Source for the insert call is a select of the source table
    // reference with the select list being the value expressions;
    // note that the values clause has already been converted to a
    // select on the values row constructor; so we need to extract
    // that via the from clause on the select
    if (insertCall != null) {
      SqlCall valuesCall = (SqlCall) insertCall.getSource();
      SqlCall rowCall = valuesCall.operand(0);
      selectList =
          new SqlNodeList(
              rowCall.getOperandList(),
              SqlParserPos.ZERO);
      SqlNode insertSource = (SqlNode) sourceTableRef.clone();
      select =
          new SqlSelect(SqlParserPos.ZERO, null, selectList, insertSource, null,
              null, null, null, null, null, null);
      insertCall.setSource(select);
    }
  }

  private SqlNode rewriteUpdateToMerge(
      SqlUpdate updateCall,
      SqlNode selfJoinSrcExpr) {
    // Make sure target has an alias.
    if (updateCall.getAlias() == null) {
      updateCall.setAlias(
          new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO));
    }
    SqlNode selfJoinTgtExpr =
        getSelfJoinExprForUpdate(
            updateCall.getTargetTable(),
            updateCall.getAlias().getSimple());
    assert selfJoinTgtExpr != null;

    // Create join condition between source and target exprs,
    // creating a conjunction with the user-level WHERE
    // clause if one was supplied
    SqlNode condition = updateCall.getCondition();
    SqlNode selfJoinCond =
        SqlStdOperatorTable.EQUALS.createCall(
            SqlParserPos.ZERO,
            selfJoinSrcExpr,
            selfJoinTgtExpr);
    if (condition == null) {
      condition = selfJoinCond;
    } else {
      condition =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO,
              selfJoinCond,
              condition);
    }
    SqlNode target =
        updateCall.getTargetTable().clone(SqlParserPos.ZERO);

    // For the source, we need to anonymize the fields, so
    // that for a statement like UPDATE T SET I = I + 1,
    // there's no ambiguity for the "I" in "I + 1";
    // this is OK because the source and target have
    // identical values due to the self-join.
    // Note that we anonymize the source rather than the
    // target because downstream, the optimizer rules
    // don't want to see any projection on top of the target.
    IdentifierNamespace ns =
        new IdentifierNamespace(this, target, null, null);
    RelDataType rowType = ns.getRowType();
    SqlNode source = updateCall.getTargetTable().clone(SqlParserPos.ZERO);
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    int i = 1;
    for (RelDataTypeField field : rowType.getFieldList()) {
      SqlIdentifier col =
          new SqlIdentifier(
              field.getName(),
              SqlParserPos.ZERO);
      selectList.add(
          SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i));
      ++i;
    }
    source =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, source, null, null,
            null, null, null, null, null);
    source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
    SqlMerge mergeCall =
        new SqlMerge(updateCall.getParserPosition(), target, condition, source,
            updateCall, null, null, updateCall.getAlias());
    rewriteMerge(mergeCall);
    return mergeCall;
  }

  /**
   * Allows a subclass to provide information about how to convert an UPDATE
   * into a MERGE via self-join. If this method returns null, then no such
   * conversion takes place. Otherwise, this method should return a suitable
   * unique identifier expression for the given table.
   *
   * @param table identifier for table being updated
   * @param alias alias to use for qualifying columns in expression, or null
   *              for unqualified references; if this is equal to
   *              {@value #UPDATE_SRC_ALIAS}, then column references have been
   *              anonymized to "SYS$ANONx", where x is the 1-based column
   *              number.
   * @return expression for unique identifier, or null to prevent conversion
   */
  protected SqlNode getSelfJoinExprForUpdate(
      SqlNode table,
      String alias) {
    return null;
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into an UPDATE
   * statement to be updated.
   *
   * @param call Call to the UPDATE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    int ordinal = 0;
    for (SqlNode exp : call.getSourceExpressionList()) {
      // Force unique aliases to avoid a duplicate for Y with
      // SET X=Y
      String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
      selectList.add(SqlValidatorUtil.addAlias(exp, alias));
      ++ordinal;
    }
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null);
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into a DELETE
   * statement to be deleted.
   *
   * @param call Call to the DELETE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null);
  }

  /**
   * Returns null if there is no common type. E.g. if the rows have a
   * different number of columns.
   */
  RelDataType getTableConstructorRowType(
      SqlCall values,
      SqlValidatorScope scope) {
    final List<SqlNode> rows = values.getOperandList();
    assert rows.size() >= 1;
    final List<RelDataType> rowTypes = new ArrayList<>();
    for (final SqlNode row : rows) {
      assert row.getKind() == SqlKind.ROW;
      SqlCall rowConstructor = (SqlCall) row;

      // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
      // rows, need to infer aliases from there.
      final List<String> aliasList = new ArrayList<>();
      final List<RelDataType> typeList = new ArrayList<>();
      for (Ord<SqlNode> column : Ord.zip(rowConstructor.getOperandList())) {
        final String alias = deriveAlias(column.e, column.i);
        aliasList.add(alias);
        final RelDataType type = deriveType(scope, column.e);
        typeList.add(type);
      }
      rowTypes.add(typeFactory.createStructType(typeList, aliasList));
    }
    if (rows.size() == 1) {
      // TODO jvs 10-Oct-2005:  get rid of this workaround once
      // leastRestrictive can handle all cases
      return rowTypes.get(0);
    }
    return typeFactory.leastRestrictive(rowTypes);
  }

  public RelDataType getValidatedNodeType(SqlNode node) {
    RelDataType type = getValidatedNodeTypeIfKnown(node);
    if (type == null) {
      throw Util.needToImplement(node);
    } else {
      return type;
    }
  }

  public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    final RelDataType type = nodeToTypeMap.get(node);
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns != null) {
      return ns.getType();
    }
    final SqlNode original = originalExprs.get(node);
    if (original != null && original != node) {
      return getValidatedNodeType(original);
    }
    return null;
  }

  /**
   * Saves the type of a {@link SqlNode}, now that it has been validated.
   *
   * <p>Unlike the base class method, this method is not deprecated.
   * It is available from within Calcite, but is not part of the public API.
   *
   * @param node A SQL parse tree node, never null
   * @param type Its type; must not be null
   */
  @SuppressWarnings("deprecation")
  public final void setValidatedNodeType(SqlNode node, RelDataType type) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(node);
    if (type.equals(unknownType)) {
      // don't set anything until we know what it is, and don't overwrite
      // a known type with the unknown type
      return;
    }
    nodeToTypeMap.put(node, type);
  }

  public void removeValidatedNodeType(SqlNode node) {
    nodeToTypeMap.remove(node);
  }

  public RelDataType deriveType(
      SqlValidatorScope scope,
      SqlNode expr) {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(expr);

    // if we already know the type, no need to re-derive
    RelDataType type = nodeToTypeMap.get(expr);
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(expr);
    if (ns != null) {
      return ns.getType();
    }
    type = deriveTypeImpl(scope, expr);
    Preconditions.checkArgument(
        type != null,
        "SqlValidator.deriveTypeInternal returned null");
    setValidatedNodeType(expr, type);
    return type;
  }

  /**
   * Derives the type of a node, never null.
   */
  RelDataType deriveTypeImpl(
      SqlValidatorScope scope,
      SqlNode operand) {
    DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
    final RelDataType type = operand.accept(v);
    // After Guava 17, use Verify.verifyNotNull for Preconditions.checkNotNull
    Bug.upgrade("guava-17");
    return Preconditions.checkNotNull(scope.nullifyType(operand, type));
  }

  public RelDataType deriveConstructorType(
      SqlValidatorScope scope,
      SqlCall call,
      SqlFunction unresolvedConstructor,
      SqlFunction resolvedConstructor,
      List<RelDataType> argTypes) {
    SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
    assert sqlIdentifier != null;
    RelDataType type = catalogReader.getNamedType(sqlIdentifier);
    if (type == null) {
      // TODO jvs 12-Feb-2005:  proper type name formatting
      throw newValidationError(sqlIdentifier,
          RESOURCE.unknownDatatypeName(sqlIdentifier.toString()));
    }

    if (resolvedConstructor == null) {
      if (call.operandCount() > 0) {
        // This is not a default constructor invocation, and
        // no user-defined constructor could be found
        throw handleUnresolvedFunction(call, unresolvedConstructor, argTypes,
            null);
      }
    } else {
      SqlCall testCall =
          resolvedConstructor.createCall(
              call.getParserPosition(),
              call.getOperandList());
      RelDataType returnType =
          resolvedConstructor.validateOperands(
              this,
              scope,
              testCall);
      assert type == returnType;
    }

    if (shouldExpandIdentifiers()) {
      if (resolvedConstructor != null) {
        ((SqlBasicCall) call).setOperator(resolvedConstructor);
      } else {
        // fake a fully-qualified call to the default constructor
        ((SqlBasicCall) call).setOperator(
            new SqlFunction(
                type.getSqlIdentifier(),
                ReturnTypes.explicit(type),
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR));
      }
    }
    return type;
  }

  public CalciteException handleUnresolvedFunction(SqlCall call,
      SqlFunction unresolvedFunction, List<RelDataType> argTypes,
      List<String> argNames) {
    // For builtins, we can give a better error message
    final List<SqlOperator> overloads = new ArrayList<>();
    opTab.lookupOperatorOverloads(unresolvedFunction.getNameAsId(), null,
        SqlSyntax.FUNCTION, overloads);
    if (overloads.size() == 1) {
      SqlFunction fun = (SqlFunction) overloads.get(0);
      if ((fun.getSqlIdentifier() == null)
          && (fun.getSyntax() != SqlSyntax.FUNCTION_ID)) {
        final int expectedArgCount =
            fun.getOperandCountRange().getMin();
        throw newValidationError(call,
            RESOURCE.invalidArgCount(call.getOperator().getName(),
                expectedArgCount));
      }
    }

    AssignableOperandTypeChecker typeChecking =
        new AssignableOperandTypeChecker(argTypes, argNames);
    String signature =
        typeChecking.getAllowedSignatures(
            unresolvedFunction,
            unresolvedFunction.getName());
    throw newValidationError(call,
        RESOURCE.validatorUnknownFunction(signature));
  }

  protected void inferUnknownTypes(
      RelDataType inferredType,
      SqlValidatorScope scope,
      SqlNode node) {
    final SqlValidatorScope newScope = scopes.get(node);
    if (newScope != null) {
      scope = newScope;
    }
    boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
    if ((node instanceof SqlDynamicParam) || isNullLiteral) {
      if (inferredType.equals(unknownType)) {
        if (isNullLiteral) {
          throw newValidationError(node, RESOURCE.nullIllegal());
        } else {
          throw newValidationError(node, RESOURCE.dynamicParamIllegal());
        }
      }

      // REVIEW:  should dynamic parameter types always be nullable?
      RelDataType newInferredType =
          typeFactory.createTypeWithNullability(inferredType, true);
      if (SqlTypeUtil.inCharFamily(inferredType)) {
        newInferredType =
            typeFactory.createTypeWithCharsetAndCollation(
                newInferredType,
                inferredType.getCharset(),
                inferredType.getCollation());
      }
      setValidatedNodeType(node, newInferredType);
    } else if (node instanceof SqlNodeList) {
      SqlNodeList nodeList = (SqlNodeList) node;
      if (inferredType.isStruct()) {
        if (inferredType.getFieldCount() != nodeList.size()) {
          // this can happen when we're validating an INSERT
          // where the source and target degrees are different;
          // bust out, and the error will be detected higher up
          return;
        }
      }
      int i = 0;
      for (SqlNode child : nodeList) {
        RelDataType type;
        if (inferredType.isStruct()) {
          type = inferredType.getFieldList().get(i).getType();
          ++i;
        } else {
          type = inferredType;
        }
        inferUnknownTypes(type, scope, child);
      }
    } else if (node instanceof SqlCase) {
      final SqlCase caseCall = (SqlCase) node;

      final RelDataType whenType =
          caseCall.getValueOperand() == null ? booleanType : unknownType;
      for (SqlNode sqlNode : caseCall.getWhenOperands().getList()) {
        inferUnknownTypes(whenType, scope, sqlNode);
      }
      RelDataType returnType = deriveType(scope, node);
      for (SqlNode sqlNode : caseCall.getThenOperands().getList()) {
        inferUnknownTypes(returnType, scope, sqlNode);
      }

      if (!SqlUtil.isNullLiteral(caseCall.getElseOperand(), false)) {
        inferUnknownTypes(
            returnType,
            scope,
            caseCall.getElseOperand());
      } else {
        setValidatedNodeType(caseCall.getElseOperand(), returnType);
      }
    } else if (node instanceof SqlCall) {
      final SqlCall call = (SqlCall) node;
      final SqlOperandTypeInference operandTypeInference =
          call.getOperator().getOperandTypeInference();
      final SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
      final List<SqlNode> operands = callBinding.operands();
      final RelDataType[] operandTypes = new RelDataType[operands.size()];
      if (operandTypeInference == null) {
        // TODO:  eventually should assert(operandTypeInference != null)
        // instead; for now just eat it
        Arrays.fill(operandTypes, unknownType);
      } else {
        operandTypeInference.inferOperandTypes(
            callBinding,
            inferredType,
            operandTypes);
      }
      for (int i = 0; i < operands.size(); ++i) {
        inferUnknownTypes(operandTypes[i], scope, operands.get(i));
      }
    }
  }

  /**
   * Adds an expression to a select list, ensuring that its alias does not
   * clash with any existing expressions on the list.
   */
  protected void addToSelectList(
      List<SqlNode> list,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fieldList,
      SqlNode exp,
      SqlValidatorScope scope,
      final boolean includeSystemVars) {
    String alias = SqlValidatorUtil.getAlias(exp, -1);
    String uniqueAlias =
        SqlValidatorUtil.uniquify(
            alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER);
    if (!alias.equals(uniqueAlias)) {
      exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
    }
    fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
    list.add(exp);
  }

  public String deriveAlias(
      SqlNode node,
      int ordinal) {
    return SqlValidatorUtil.getAlias(node, ordinal);
  }

  // implement SqlValidator
  public void setIdentifierExpansion(boolean expandIdentifiers) {
    this.expandIdentifiers = expandIdentifiers;
  }

  // implement SqlValidator
  public void setColumnReferenceExpansion(
      boolean expandColumnReferences) {
    this.expandColumnReferences = expandColumnReferences;
  }

  // implement SqlValidator
  public boolean getColumnReferenceExpansion() {
    return expandColumnReferences;
  }

  public void setDefaultNullCollation(NullCollation nullCollation) {
    this.nullCollation = Preconditions.checkNotNull(nullCollation);
  }

  public NullCollation getDefaultNullCollation() {
    return nullCollation;
  }

  // implement SqlValidator
  public void setCallRewrite(boolean rewriteCalls) {
    this.rewriteCalls = rewriteCalls;
  }

  public boolean shouldExpandIdentifiers() {
    return expandIdentifiers;
  }

  protected boolean shouldAllowIntermediateOrderBy() {
    return true;
  }

  private void registerMatchRecognize(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlMatchRecognize call,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {

    final MatchRecognizeNamespace matchRecognizeNamespace =
        createMatchRecognizeNameSpace(call, enclosingNode);
    registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable);

    final MatchRecognizeScope matchRecognizeScope =
        new MatchRecognizeScope(parentScope, call);
    scopes.put(call, matchRecognizeScope);

    // parse input query
    SqlNode expr = call.getTableRef();
    SqlNode newExpr = registerFrom(usingScope, matchRecognizeScope, expr,
        expr, null, null, forceNullable);
    if (expr != newExpr) {
      call.setOperand(0, newExpr);
    }
  }

  protected MatchRecognizeNamespace createMatchRecognizeNameSpace(
      SqlMatchRecognize call,
      SqlNode enclosingNode) {
    return new MatchRecognizeNamespace(this, call, enclosingNode);
  }

  /**
   * Registers a new namespace, and adds it as a child of its parent scope.
   * Derived class can override this method to tinker with namespaces as they
   * are created.
   *
   * @param usingScope    Parent scope (which will want to look for things in
   *                      this namespace)
   * @param alias         Alias by which parent will refer to this namespace
   * @param ns            Namespace
   * @param forceNullable Whether to force the type of namespace to be nullable
   */
  protected void registerNamespace(
      SqlValidatorScope usingScope,
      String alias,
      SqlValidatorNamespace ns,
      boolean forceNullable) {
    namespaces.put(ns.getNode(), ns);
    if (usingScope != null) {
      usingScope.addChild(ns, alias, forceNullable);
    }
  }

  /**
   * Registers scopes and namespaces implied a relational expression in the
   * FROM clause.
   *
   * <p>{@code parentScope} and {@code usingScope} are often the same. They
   * differ when the namespace are not visible within the parent. (Example
   * needed.)
   *
   * <p>Likewise, {@code enclosingNode} and {@code node} are often the same.
   * {@code enclosingNode} is the topmost node within the FROM clause, from
   * which any decorations like an alias (<code>AS alias</code>) or a table
   * sample clause are stripped away to get {@code node}. Both are recorded in
   * the namespace.
   *
   * @param parentScope   Parent scope which this scope turns to in order to
   *                      resolve objects
   * @param usingScope    Scope whose child list this scope should add itself to
   * @param node          Node which namespace is based on
   * @param enclosingNode Outermost node for namespace, including decorations
   *                      such as alias and sample clause
   * @param alias         Alias
   * @param extendList    Definitions of extended columns
   * @param forceNullable Whether to force the type of namespace to be
   *                      nullable because it is in an outer join
   * @return registered node, usually the same as {@code node}
   */
  private SqlNode registerFrom(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      final SqlNode node,
      SqlNode enclosingNode,
      String alias,
      SqlNodeList extendList,
      boolean forceNullable) {
    final SqlKind kind = node.getKind();

    SqlNode expr;
    SqlNode newExpr;

    // Add an alias if necessary.
    SqlNode newNode = node;
    if (alias == null) {
      switch (kind) {
      case IDENTIFIER:
      case OVER:
        alias = deriveAlias(node, -1);
        if (alias == null) {
          alias = deriveAlias(node, nextGeneratedId++);
        }
        if (shouldExpandIdentifiers()) {
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;

      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case VALUES:
      case UNNEST:
      case OTHER_FUNCTION:
      case COLLECTION_TABLE:
      case MATCH_RECOGNIZE:

        // give this anonymous construct a name since later
        // query processing stages rely on it
        alias = deriveAlias(node, nextGeneratedId++);
        if (shouldExpandIdentifiers()) {
          // Since we're expanding identifiers, we should make the
          // aliases explicit too, otherwise the expanded query
          // will not be consistent if we convert back to SQL, e.g.
          // "select EXPR$1.EXPR$2 from values (1)".
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;
      }
    }

    SqlCall call;
    SqlNode operand;
    SqlNode newOperand;

    switch (kind) {
    case AS:
      call = (SqlCall) node;
      if (alias == null) {
        alias = call.operand(1).toString();
      }
      SqlValidatorScope usingScope2 = usingScope;
      if (call.operandCount() > 2) {
        usingScope2 = null;
      }
      expr = call.operand(0);
      newExpr =
          registerFrom(
              parentScope,
              usingScope2,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }

      // If alias has a column list, introduce a namespace to translate
      // column names.
      if (call.operandCount() > 2) {
        registerNamespace(
            usingScope,
            alias,
            new AliasNamespace(this, call, enclosingNode),
            false);
      }
      return node;
    case MATCH_RECOGNIZE:
      registerMatchRecognize(parentScope, usingScope,
        (SqlMatchRecognize) node, enclosingNode, alias, forceNullable);
      return node;
    case TABLESAMPLE:
      call = (SqlCall) node;
      expr = call.operand(0);
      newExpr =
          registerFrom(
              parentScope,
              usingScope,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }
      return node;

    case JOIN:
      final SqlJoin join = (SqlJoin) node;
      final JoinScope joinScope =
          new JoinScope(parentScope, usingScope, join);
      scopes.put(join, joinScope);
      final SqlNode left = join.getLeft();
      final SqlNode right = join.getRight();
      final boolean rightIsLateral = isLateral(right);
      boolean forceLeftNullable = forceNullable;
      boolean forceRightNullable = forceNullable;
      switch (join.getJoinType()) {
      case LEFT:
        forceRightNullable = true;
        break;
      case RIGHT:
        forceLeftNullable = true;
        break;
      case FULL:
        forceLeftNullable = true;
        forceRightNullable = true;
        break;
      }
      final SqlNode newLeft =
          registerFrom(
              parentScope,
              joinScope,
              left,
              left,
              null,
              null,
              forceLeftNullable);
      if (newLeft != left) {
        join.setLeft(newLeft);
      }
      final SqlValidatorScope rightParentScope;
      if (rightIsLateral) {
        rightParentScope = joinScope;
      } else {
        rightParentScope = parentScope;
      }
      final SqlNode newRight =
          registerFrom(
              rightParentScope,
              joinScope,
              right,
              right,
              null,
              null,
              forceRightNullable);
      if (newRight != right) {
        join.setRight(newRight);
      }
      registerSubQueries(joinScope, join.getCondition());
      final JoinNamespace joinNamespace = new JoinNamespace(this, join);
      registerNamespace(null, null, joinNamespace, forceNullable);
      return join;

    case IDENTIFIER:
      final SqlIdentifier id = (SqlIdentifier) node;
      final IdentifierNamespace newNs =
          new IdentifierNamespace(
              this, id, extendList, enclosingNode,
              parentScope);
      registerNamespace(usingScope, alias, newNs, forceNullable);
      if (tableScope == null) {
        tableScope = new TableScope(parentScope, node);
      }
      tableScope.addChild(newNs, alias, forceNullable);
      if (extendList != null && extendList.size() != 0) {
        return enclosingNode;
      }
      return newNode;

    case LATERAL:
      if (tableScope != null) {
        tableScope.meetLateral();
      }
      return registerFrom(
          parentScope,
          usingScope,
          ((SqlCall) node).operand(0),
          enclosingNode,
          alias,
          extendList,
          forceNullable);

    case COLLECTION_TABLE:
      call = (SqlCall) node;
      operand = call.operand(0);
      newOperand =
          registerFrom(
              tableScope == null ? parentScope : tableScope,
              usingScope,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }
      scopes.put(node, parentScope);
      return newNode;

    case SELECT:
    case UNION:
    case INTERSECT:
    case EXCEPT:
    case VALUES:
    case WITH:
    case UNNEST:
    case OTHER_FUNCTION:
      if (alias == null) {
        alias = deriveAlias(node, nextGeneratedId++);
      }
      registerQuery(
          parentScope,
          usingScope,
          node,
          enclosingNode,
          alias,
          forceNullable);
      return newNode;

    case OVER:
      if (!shouldAllowOverRelation()) {
        throw Util.unexpected(kind);
      }
      call = (SqlCall) node;
      final OverScope overScope = new OverScope(usingScope, call);
      scopes.put(call, overScope);
      operand = call.operand(0);
      newOperand =
          registerFrom(
              parentScope,
              overScope,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }

      for (ScopeChild child : overScope.children) {
        registerNamespace(usingScope, child.name, child.namespace,
            forceNullable);
      }

      return newNode;

    case EXTEND:
      final SqlCall extend = (SqlCall) node;
      return registerFrom(parentScope,
          usingScope,
          extend.getOperandList().get(0),
          extend,
          alias,
          (SqlNodeList) extend.getOperandList().get(1),
          forceNullable);

    default:
      throw Util.unexpected(kind);
    }
  }

  private static boolean isLateral(SqlNode node) {
    switch (node.getKind()) {
    case LATERAL:
    case UNNEST:
      // Per SQL std, UNNEST is implicitly LATERAL.
      return true;
    case AS:
      return isLateral(((SqlCall) node).operand(0));
    default:
      return false;
    }
  }

  protected boolean shouldAllowOverRelation() {
    return false;
  }

  /**
   * Creates a namespace for a <code>SELECT</code> node. Derived class may
   * override this factory method.
   *
   * @param select        Select node
   * @param enclosingNode Enclosing node
   * @return Select namespace
   */
  protected SelectNamespace createSelectNamespace(
      SqlSelect select,
      SqlNode enclosingNode) {
    return new SelectNamespace(this, select, enclosingNode);
  }

  /**
   * Creates a namespace for a set operation (<code>UNION</code>, <code>
   * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override
   * this factory method.
   *
   * @param call          Call to set operation
   * @param enclosingNode Enclosing node
   * @return Set operation namespace
   */
  protected SetopNamespace createSetopNamespace(
      SqlCall call,
      SqlNode enclosingNode) {
    return new SetopNamespace(this, call, enclosingNode);
  }

  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   */
  private void registerQuery(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {
    Preconditions.checkArgument(usingScope == null || alias != null);
    registerQuery(
        parentScope,
        usingScope,
        node,
        enclosingNode,
        alias,
        forceNullable,
        true);
  }

  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   * @param checkUpdate if true, validate that the update feature is supported
   *                    if validating the update statement
   */
  private void registerQuery(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(enclosingNode);
    Preconditions.checkArgument(usingScope == null || alias != null);

    SqlCall call;
    List<SqlNode> operands;
    switch (node.getKind()) {
    case SELECT:
      final SqlSelect select = (SqlSelect) node;
      final SelectNamespace selectNs =
          createSelectNamespace(select, enclosingNode);
      registerNamespace(usingScope, alias, selectNs, forceNullable);
      final SqlValidatorScope windowParentScope =
          (usingScope != null) ? usingScope : parentScope;
      SelectScope selectScope =
          new SelectScope(parentScope, windowParentScope, select);
      scopes.put(select, selectScope);

      // Start by registering the WHERE clause
      whereScopes.put(select, selectScope);
      registerOperandSubQueries(
          selectScope,
          select,
          SqlSelect.WHERE_OPERAND);

      // Register FROM with the inherited scope 'parentScope', not
      // 'selectScope', otherwise tables in the FROM clause would be
      // able to see each other.
      final SqlNode from = select.getFrom();
      if (from != null) {
        final SqlNode newFrom =
            registerFrom(
                parentScope,
                selectScope,
                from,
                from,
                null,
                null,
                false);
        if (newFrom != from) {
          select.setFrom(newFrom);
        }
      }

      // If this is an aggregating query, the SELECT list and HAVING
      // clause use a different scope, where you can only reference
      // columns which are in the GROUP BY clause.
      SqlValidatorScope aggScope = selectScope;
      if (isAggregate(select)) {
        aggScope =
            new AggregatingSelectScope(selectScope, select, false);
        selectScopes.put(select, aggScope);
      } else {
        selectScopes.put(select, selectScope);
      }
      if (select.getGroup() != null) {
        GroupByScope groupByScope =
            new GroupByScope(selectScope, select.getGroup(), select);
        groupByScopes.put(select, groupByScope);
        registerSubQueries(groupByScope, select.getGroup());
      }
      registerOperandSubQueries(
          aggScope,
          select,
          SqlSelect.HAVING_OPERAND);
      registerSubQueries(aggScope, select.getSelectList());
      final SqlNodeList orderList = select.getOrderList();
      if (orderList != null) {
        // If the query is 'SELECT DISTINCT', restrict the columns
        // available to the ORDER BY clause.
        if (select.isDistinct()) {
          aggScope =
              new AggregatingSelectScope(selectScope, select, true);
        }
        OrderByScope orderScope =
            new OrderByScope(aggScope, orderList, select);
        orderScopes.put(select, orderScope);
        registerSubQueries(orderScope, orderList);

        if (!isAggregate(select)) {
          // Since this is not an aggregating query,
          // there cannot be any aggregates in the ORDER BY clause.
          SqlNode agg = aggFinder.findAgg(orderList);
          if (agg != null) {
            throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
          }
        }
      }
      break;

    case INTERSECT:
      validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case EXCEPT:
      validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case UNION:
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case WITH:
      registerWith(parentScope, usingScope, (SqlWith) node, enclosingNode,
          alias, forceNullable, checkUpdate);
      break;

    case VALUES:
      call = (SqlCall) node;
      scopes.put(call, parentScope);
      final TableConstructorNamespace tableConstructorNamespace =
          new TableConstructorNamespace(
              this,
              call,
              parentScope,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          tableConstructorNamespace,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); ++i) {
        assert operands.get(i).getKind() == SqlKind.ROW;

        // FIXME jvs 9-Feb-2005:  Correlation should
        // be illegal in these sub-queries.  Same goes for
        // any non-lateral SELECT in the FROM list.
        registerOperandSubQueries(parentScope, call, i);
      }
      break;

    case INSERT:
      SqlInsert insertCall = (SqlInsert) node;
      InsertNamespace insertNs =
          new InsertNamespace(
              this,
              insertCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, insertNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          insertCall.getSource(),
          enclosingNode,
          null,
          false);
      break;

    case DELETE:
      SqlDelete deleteCall = (SqlDelete) node;
      DeleteNamespace deleteNs =
          new DeleteNamespace(
              this,
              deleteCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, deleteNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          deleteCall.getSourceSelect(),
          enclosingNode,
          null,
          false);
      break;

    case UPDATE:
      if (checkUpdate) {
        validateFeature(RESOURCE.sQLFeature_E101_03(),
            node.getParserPosition());
      }
      SqlUpdate updateCall = (SqlUpdate) node;
      UpdateNamespace updateNs =
          new UpdateNamespace(
              this,
              updateCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, updateNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          updateCall.getSourceSelect(),
          enclosingNode,
          null,
          false);
      break;

    case MERGE:
      validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
      SqlMerge mergeCall = (SqlMerge) node;
      MergeNamespace mergeNs =
          new MergeNamespace(
              this,
              mergeCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, mergeNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          mergeCall.getSourceSelect(),
          enclosingNode,
          null,
          false);

      // update call can reference either the source table reference
      // or the target table, so set its parent scope to the merge's
      // source select; when validating the update, skip the feature
      // validation check
      if (mergeCall.getUpdateCall() != null) {
        registerQuery(
            whereScopes.get(mergeCall.getSourceSelect()),
            null,
            mergeCall.getUpdateCall(),
            enclosingNode,
            null,
            false,
            false);
      }
      if (mergeCall.getInsertCall() != null) {
        registerQuery(
            parentScope,
            null,
            mergeCall.getInsertCall(),
            enclosingNode,
            null,
            false);
      }
      break;

    case UNNEST:
      call = (SqlCall) node;
      final UnnestNamespace unnestNs =
          new UnnestNamespace(this, call, parentScope, enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          unnestNs,
          forceNullable);
      registerOperandSubQueries(parentScope, call, 0);
      scopes.put(node, parentScope);
      break;

    case OTHER_FUNCTION:
      call = (SqlCall) node;
      ProcedureNamespace procNs =
          new ProcedureNamespace(
              this,
              parentScope,
              call,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          procNs,
          forceNullable);
      registerSubQueries(parentScope, call);
      break;

    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
      validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      call = (SqlCall) node;
      CollectScope cs = new CollectScope(parentScope, usingScope, call);
      final CollectNamespace tableConstructorNs =
          new CollectNamespace(call, cs, enclosingNode);
      final String alias2 = deriveAlias(node, nextGeneratedId++);
      registerNamespace(
          usingScope,
          alias2,
          tableConstructorNs,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        registerOperandSubQueries(parentScope, call, i);
      }
      break;

    default:
      throw Util.unexpected(node.getKind());
    }
  }

  private void registerSetop(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {
    SqlCall call = (SqlCall) node;
    final SetopNamespace setopNamespace =
        createSetopNamespace(call, enclosingNode);
    registerNamespace(usingScope, alias, setopNamespace, forceNullable);

    // A setop is in the same scope as its parent.
    scopes.put(call, parentScope);
    for (SqlNode operand : call.getOperandList()) {
      registerQuery(
          parentScope,
          null,
          operand,
          operand,
          null,
          false);
    }
  }

  private void registerWith(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlWith with,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    final WithNamespace withNamespace =
        new WithNamespace(this, with, enclosingNode);
    registerNamespace(usingScope, alias, withNamespace, forceNullable);

    SqlValidatorScope scope = parentScope;
    for (SqlNode withItem_ : with.withList) {
      final SqlWithItem withItem = (SqlWithItem) withItem_;
      final WithScope withScope = new WithScope(scope, withItem);
      scopes.put(withItem, withScope);

      registerQuery(scope, null, withItem.query, with,
          withItem.name.getSimple(), false);
      registerNamespace(null, alias,
          new WithItemNamespace(this, withItem, enclosingNode),
          false);
      scope = withScope;
    }

    registerQuery(scope, null, with.body, enclosingNode, alias, forceNullable,
        checkUpdate);
  }

  public boolean isAggregate(SqlSelect select) {
    if (getAggregate(select) != null) {
      return true;
    }
    // Also when nested window aggregates are present
    for (SqlNode node : select.getSelectList()) {
      if (node instanceof SqlCall) {
        SqlCall call = (SqlCall) overFinder.findAgg(node);
        if (call != null
            && call.getOperator().getKind() == SqlKind.OVER
            && call.getOperandList().size() != 0) {
          if (call.operand(0) instanceof SqlCall
              && isNestedAggregateWindow((SqlCall) call.operand(0))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  protected boolean isNestedAggregateWindow(SqlCall windowFunction) {
    AggFinder nestedAggFinder =
        new AggFinder(opTab, false, false, false, aggFinder);
    return nestedAggFinder.findAgg(windowFunction) != null;
  }

  /** Returns the parse tree node (GROUP BY, HAVING, or an aggregate function
   * call) that causes {@code select} to be an aggregate query, or null if it is
   * not an aggregate query.
   *
   * <p>The node is useful context for error messages. */
  protected SqlNode getAggregate(SqlSelect select) {
    SqlNode node = select.getGroup();
    if (node != null) {
      return node;
    }
    node = select.getHaving();
    if (node != null) {
      return node;
    }
    return getAgg(select);
  }

  private SqlNode getAgg(SqlSelect select) {
    final SelectScope selectScope = getRawSelectScope(select);
    if (selectScope != null) {
      final List<SqlNode> selectList = selectScope.getExpandedSelectList();
      if (selectList != null) {
        return aggFinder.findAgg(selectList);
      }
    }
    return aggFinder.findAgg(select.getSelectList());
  }

  @SuppressWarnings("deprecation")
  public boolean isAggregate(SqlNode selectNode) {
    return aggFinder.findAgg(selectNode) != null;
  }

  private void validateNodeFeature(SqlNode node) {
    switch (node.getKind()) {
    case MULTISET_VALUE_CONSTRUCTOR:
      validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      break;
    }
  }

  private void registerSubQueries(
      SqlValidatorScope parentScope,
      SqlNode node) {
    if (node == null) {
      return;
    }
    if (node.getKind().belongsTo(SqlKind.QUERY)
        || node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR
        || node.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
      registerQuery(parentScope, null, node, node, null, false);
    } else if (node instanceof SqlCall) {
      validateNodeFeature(node);
      SqlCall call = (SqlCall) node;
      for (int i = 0; i < call.operandCount(); i++) {
        registerOperandSubQueries(parentScope, call, i);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode listNode = list.get(i);
        if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
          listNode =
              SqlStdOperatorTable.SCALAR_QUERY.createCall(
                  listNode.getParserPosition(),
                  listNode);
          list.set(i, listNode);
        }
        registerSubQueries(parentScope, listNode);
      }
    } else {
      // atomic node -- can be ignored
    }
  }

  /**
   * Registers any sub-queries inside a given call operand, and converts the
   * operand to a scalar sub-query if the operator requires it.
   *
   * @param parentScope    Parent scope
   * @param call           Call
   * @param operandOrdinal Ordinal of operand within call
   * @see SqlOperator#argumentMustBeScalar(int)
   */
  private void registerOperandSubQueries(
      SqlValidatorScope parentScope,
      SqlCall call,
      int operandOrdinal) {
    SqlNode operand = call.operand(operandOrdinal);
    if (operand == null) {
      return;
    }
    if (operand.getKind().belongsTo(SqlKind.QUERY)
        && call.getOperator().argumentMustBeScalar(operandOrdinal)) {
      operand =
          SqlStdOperatorTable.SCALAR_QUERY.createCall(
              operand.getParserPosition(),
              operand);
      call.setOperand(operandOrdinal, operand);
    }
    registerSubQueries(parentScope, operand);
  }

  public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
    final SqlQualified fqId = scope.fullyQualify(id);
    if (expandColumnReferences) {
      // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
      // own ideas about qualification.
      id.assignNamesFrom(fqId.identifier);
    } else {
      Util.discard(fqId);
    }
  }

  public void validateLiteral(SqlLiteral literal) {
    switch (literal.getTypeName()) {
    case DECIMAL:
      // Decimal and long have the same precision (as 64-bit integers), so
      // the unscaled value of a decimal must fit into a long.

      // REVIEW jvs 4-Aug-2004:  This should probably be calling over to
      // the available calculator implementations to see what they
      // support.  For now use ESP instead.
      //
      // jhyde 2006/12/21: I think the limits should be baked into the
      // type system, not dependent on the calculator implementation.
      BigDecimal bd = (BigDecimal) literal.getValue();
      BigInteger unscaled = bd.unscaledValue();
      long longValue = unscaled.longValue();
      if (!BigInteger.valueOf(longValue).equals(unscaled)) {
        // overflow
        throw newValidationError(literal,
            RESOURCE.numberLiteralOutOfRange(bd.toString()));
      }
      break;

    case DOUBLE:
      validateLiteralAsDouble(literal);
      break;

    case BINARY:
      final BitString bitString = (BitString) literal.getValue();
      if ((bitString.getBitCount() % 8) != 0) {
        throw newValidationError(literal, RESOURCE.binaryLiteralOdd());
      }
      break;

    case DATE:
    case TIME:
    case TIMESTAMP:
      Calendar calendar = (Calendar) literal.getValue();
      final int year = calendar.get(Calendar.YEAR);
      final int era = calendar.get(Calendar.ERA);
      if (year < 1 || era == GregorianCalendar.BC || year > 9999) {
        throw newValidationError(literal,
            RESOURCE.dateLiteralOutOfRange(literal.toString()));
      }
      break;

    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      if (literal instanceof SqlIntervalLiteral) {
        SqlIntervalLiteral.IntervalValue interval =
            (SqlIntervalLiteral.IntervalValue)
                literal.getValue();
        SqlIntervalQualifier intervalQualifier =
            interval.getIntervalQualifier();

        // ensure qualifier is good before attempting to validate literal
        validateIntervalQualifier(intervalQualifier);
        String intervalStr = interval.getIntervalLiteral();
        // throws CalciteContextException if string is invalid
        int[] values = intervalQualifier.evaluateIntervalLiteral(intervalStr,
            literal.getParserPosition(), typeFactory.getTypeSystem());
        Util.discard(values);
      }
      break;
    default:
      // default is to do nothing
    }
  }

  private void validateLiteralAsDouble(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    double d = bd.doubleValue();
    if (Double.isInfinite(d) || Double.isNaN(d)) {
      // overflow
      throw newValidationError(literal,
          RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd)));
    }

    // REVIEW jvs 4-Aug-2004:  what about underflow?
  }

  public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
    assert qualifier != null;
    boolean startPrecisionOutOfRange = false;
    boolean fractionalSecondPrecisionOutOfRange = false;
    final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

    final int startPrecision = qualifier.getStartPrecision(typeSystem);
    final int fracPrecision =
        qualifier.getFractionalSecondPrecision(typeSystem);
    final int maxPrecision = typeSystem.getMaxPrecision(qualifier.typeName());
    final int minPrecision = qualifier.typeName().getMinPrecision();
    final int minScale = qualifier.typeName().getMinScale();
    final int maxScale = typeSystem.getMaxScale(qualifier.typeName());
    if (qualifier.isYearMonth()) {
      if (startPrecision < minPrecision || startPrecision > maxPrecision) {
        startPrecisionOutOfRange = true;
      } else {
        if (fracPrecision < minScale || fracPrecision > maxScale) {
          fractionalSecondPrecisionOutOfRange = true;
        }
      }
    } else {
      if (startPrecision < minPrecision || startPrecision > maxPrecision) {
        startPrecisionOutOfRange = true;
      } else {
        if (fracPrecision < minScale || fracPrecision > maxScale) {
          fractionalSecondPrecisionOutOfRange = true;
        }
      }
    }

    if (startPrecisionOutOfRange) {
      throw newValidationError(qualifier,
          RESOURCE.intervalStartPrecisionOutOfRange(startPrecision,
              "INTERVAL " + qualifier));
    } else if (fractionalSecondPrecisionOutOfRange) {
      throw newValidationError(qualifier,
          RESOURCE.intervalFractionalSecondPrecisionOutOfRange(
              fracPrecision,
              "INTERVAL " + qualifier));
    }
  }

  /**
   * Validates the FROM clause of a query, or (recursively) a child node of
   * the FROM clause: AS, OVER, JOIN, VALUES, or sub-query.
   *
   * @param node          Node in FROM clause, typically a table or derived
   *                      table
   * @param targetRowType Desired row type of this expression, or
   *                      {@link #unknownType} if not fussy. Must not be null.
   * @param scope         Scope
   */
  protected void validateFrom(
      SqlNode node,
      RelDataType targetRowType,
      SqlValidatorScope scope) {
    Preconditions.checkNotNull(targetRowType);
    switch (node.getKind()) {
    case AS:
      validateFrom(
          ((SqlCall) node).operand(0),
          targetRowType,
          scope);
      break;
    case VALUES:
      validateValues((SqlCall) node, targetRowType, scope);
      break;
    case JOIN:
      validateJoin((SqlJoin) node, scope);
      break;
    case OVER:
      validateOver((SqlCall) node, scope);
      break;
    default:
      validateQuery(node, scope, targetRowType);
      break;
    }

    // Validate the namespace representation of the node, just in case the
    // validation did not occur implicitly.
    getNamespace(node, scope).validate(targetRowType);
  }

  protected void validateOver(SqlCall call, SqlValidatorScope scope) {
    throw new AssertionError("OVER unexpected in this context");
  }

  protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    boolean natural = join.isNatural();
    final JoinType joinType = join.getJoinType();
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    validateFrom(left, unknownType, joinScope);
    validateFrom(right, unknownType, joinScope);

    // Validate condition.
    switch (conditionType) {
    case NONE:
      Preconditions.checkArgument(condition == null);
      break;
    case ON:
      Preconditions.checkArgument(condition != null);
      SqlNode expandedCondition = expand(condition, joinScope);
      join.setOperand(5, expandedCondition);
      condition = join.getCondition();
      validateWhereOrOn(joinScope, condition, "ON");
      break;
    case USING:
      SqlNodeList list = (SqlNodeList) condition;

      // Parser ensures that using clause is not empty.
      Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
      for (int i = 0; i < list.size(); i++) {
        SqlIdentifier id = (SqlIdentifier) list.get(i);
        final RelDataType leftColType = validateUsingCol(id, left);
        final RelDataType rightColType = validateUsingCol(id, right);
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
          throw newValidationError(id,
              RESOURCE.naturalOrUsingColumnNotCompatible(id.getSimple(),
                  leftColType.toString(), rightColType.toString()));
        }
      }
      break;
    default:
      throw Util.unexpected(conditionType);
    }

    // Validate NATURAL.
    if (natural) {
      if (condition != null) {
        throw newValidationError(condition,
            RESOURCE.naturalDisallowsOnOrUsing());
      }

      // Join on fields that occur exactly once on each side. Ignore
      // fields that occur more than once on either side.
      final RelDataType leftRowType = getNamespace(left).getRowType();
      final RelDataType rightRowType = getNamespace(right).getRowType();
      List<String> naturalColumnNames =
          SqlValidatorUtil.deriveNaturalJoinColumnList(
              leftRowType,
              rightRowType);

      // Check compatibility of the chosen columns.
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      for (String name : naturalColumnNames) {
        final RelDataType leftColType =
            nameMatcher.field(leftRowType, name).getType();
        final RelDataType rightColType =
            nameMatcher.field(rightRowType, name).getType();
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
          throw newValidationError(join,
              RESOURCE.naturalOrUsingColumnNotCompatible(name,
                  leftColType.toString(), rightColType.toString()));
        }
      }
    }

    // Which join types require/allow a ON/USING condition, or allow
    // a NATURAL keyword?
    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
    case FULL:
      if ((condition == null) && !natural) {
        throw newValidationError(join, RESOURCE.joinRequiresCondition());
      }
      break;
    case COMMA:
    case CROSS:
      if (condition != null) {
        throw newValidationError(join.getConditionTypeNode(),
            RESOURCE.crossJoinDisallowsCondition());
      }
      if (natural) {
        throw newValidationError(join.getConditionTypeNode(),
            RESOURCE.crossJoinDisallowsCondition());
      }
      break;
    default:
      throw Util.unexpected(joinType);
    }
  }

  /**
   * Throws an error if there is an aggregate or windowed aggregate in the
   * given clause.
   *
   * @param aggFinder Finder for the particular kind(s) of aggregate function
   * @param node      Parse tree
   * @param clause    Name of clause: "WHERE", "GROUP BY", "ON"
   */
  private void validateNoAggs(AggFinder aggFinder, SqlNode node,
                              String clause) {
    final SqlCall agg = aggFinder.findAgg(node);
    if (agg == null) {
      return;
    }
    final SqlOperator op = agg.getOperator();
    if (op == SqlStdOperatorTable.OVER) {
      throw newValidationError(agg,
          RESOURCE.windowedAggregateIllegalInClause(clause));
    } else if (op.isGroup() || op.isGroupAuxiliary()) {
      throw newValidationError(agg,
          RESOURCE.groupFunctionMustAppearInGroupByClause(op.getName()));
    } else {
      throw newValidationError(agg,
          RESOURCE.aggregateIllegalInClause(clause));
    }
  }

  private RelDataType validateUsingCol(SqlIdentifier id, SqlNode leftOrRight) {
    if (id.names.size() == 1) {
      String name = id.names.get(0);
      final SqlValidatorNamespace namespace = getNamespace(leftOrRight);
      final RelDataType rowType = namespace.getRowType();
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      final RelDataTypeField field = nameMatcher.field(rowType, name);
      if (field != null) {
        if (Collections.frequency(rowType.getFieldNames(), name) > 1) {
          throw newValidationError(id,
              RESOURCE.columnInUsingNotUnique(id.toString()));
        }
        return field.getType();
      }
    }
    throw newValidationError(id, RESOURCE.columnNotFound(id.toString()));
  }

  /**
   * Validates a SELECT statement.
   *
   * @param select        Select statement
   * @param targetRowType Desired row type, must not be null, may be the data
   *                      type 'unknown'.
   */
  protected void validateSelect(
      SqlSelect select,
      RelDataType targetRowType) {
    assert targetRowType != null;

    // Namespace is either a select namespace or a wrapper around one.
    final SelectNamespace ns =
        getNamespace(select).unwrap(SelectNamespace.class);

    // Its rowtype is null, meaning it hasn't been validated yet.
    // This is important, because we need to take the targetRowType into
    // account.
    assert ns.rowType == null;

    if (select.isDistinct()) {
      validateFeature(RESOURCE.sQLFeature_E051_01(),
          select.getModifierNode(SqlSelectKeyword.DISTINCT)
              .getParserPosition());
    }

    final SqlNodeList selectItems = select.getSelectList();
    RelDataType fromType = unknownType;
    if (selectItems.size() == 1) {
      final SqlNode selectItem = selectItems.get(0);
      if (selectItem instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) selectItem;
        if (id.isStar() && (id.names.size() == 1)) {
          // Special case: for INSERT ... VALUES(?,?), the SQL
          // standard says we're supposed to propagate the target
          // types down.  So iff the select list is an unqualified
          // star (as it will be after an INSERT ... VALUES has been
          // expanded), then propagate.
          fromType = targetRowType;
        }
      }
    }

    // Make sure that items in FROM clause have distinct aliases.
    final SelectScope fromScope = (SelectScope) getFromScope(select);
    List<String> names = fromScope.getChildNames();
    if (!catalogReader.nameMatcher().isCaseSensitive()) {
      names = Lists.transform(names,
          new Function<String, String>() {
            public String apply(String s) {
              return s.toUpperCase(Locale.ROOT);
            }
          });
    }
    final int duplicateAliasOrdinal = Util.firstDuplicate(names);
    if (duplicateAliasOrdinal >= 0) {
      final ScopeChild child =
          fromScope.children.get(duplicateAliasOrdinal);
      throw newValidationError(child.namespace.getEnclosingNode(),
          RESOURCE.fromAliasDuplicate(child.name));
    }

    if (select.getFrom() == null) {
      if (conformance.isFromRequired()) {
        throw newValidationError(select, RESOURCE.selectMissingFrom());
      }
    } else {
      validateFrom(select.getFrom(), fromType, fromScope);
    }

    validateWhereClause(select);
    validateGroupClause(select);
    validateHavingClause(select);
    validateWindowClause(select);

    // Validate the SELECT clause late, because a select item might
    // depend on the GROUP BY list, or the window function might reference
    // window name in the WINDOW clause etc.
    final RelDataType rowType =
        validateSelectList(selectItems, select, targetRowType);
    ns.setType(rowType);

    // Validate ORDER BY after we have set ns.rowType because in some
    // dialects you can refer to columns of the select list, e.g.
    // "SELECT empno AS x FROM emp ORDER BY x"
    validateOrderList(select);
  }

  /** Validates that a query can deliver the modality it promises. Only called
   * on the top-most SELECT or set operator in the tree. */
  private void validateModality(SqlNode query) {
    final SqlModality modality = deduceModality(query);
    if (query instanceof SqlSelect) {
      final SqlSelect select = (SqlSelect) query;
      validateModality(select, modality, true);
    } else if (query.getKind() == SqlKind.VALUES) {
      switch (modality) {
      case STREAM:
        throw newValidationError(query, Static.RESOURCE.cannotStreamValues());
      }
    } else {
      assert query.isA(SqlKind.SET_QUERY);
      final SqlCall call = (SqlCall) query;
      for (SqlNode operand : call.getOperandList()) {
        if (deduceModality(operand) != modality) {
          throw newValidationError(operand,
              Static.RESOURCE.streamSetOpInconsistentInputs());
        }
        validateModality(operand);
      }
    }
  }

  /** Return the intended modality of a SELECT or set-op. */
  private SqlModality deduceModality(SqlNode query) {
    if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      return select.getModifierNode(SqlSelectKeyword.STREAM) != null
          ? SqlModality.STREAM
          : SqlModality.RELATION;
    } else if (query.getKind() == SqlKind.VALUES) {
      return SqlModality.RELATION;
    } else {
      assert query.isA(SqlKind.SET_QUERY);
      final SqlCall call = (SqlCall) query;
      return deduceModality(call.getOperandList().get(0));
    }
  }

  public boolean validateModality(SqlSelect select, SqlModality modality,
      boolean fail) {
    final SelectScope scope = getRawSelectScope(select);

    switch (modality) {
    case STREAM:
      if (scope.children.size() == 1) {
        for (ScopeChild child : scope.children) {
          if (!child.namespace.supportsModality(modality)) {
            if (fail) {
              throw newValidationError(child.namespace.getNode(),
                  Static.RESOURCE.cannotConvertToStream(child.name));
            } else {
              return false;
            }
          }
        }
      } else {
        int supportsModalityCount = 0;
        for (ScopeChild child : scope.children) {
          if (child.namespace.supportsModality(modality)) {
            ++supportsModalityCount;
          }
        }

        if (supportsModalityCount == 0) {
          if (fail) {
            String inputs = Joiner.on(", ").join(scope.getChildNames());
            throw newValidationError(select,
                Static.RESOURCE.cannotStreamResultsForNonStreamingInputs(inputs));
          } else {
            return false;
          }
        }
      }
      break;
    default:
      for (ScopeChild child : scope.children) {
        if (!child.namespace.supportsModality(modality)) {
          if (fail) {
            throw newValidationError(child.namespace.getNode(),
                Static.RESOURCE.cannotConvertToRelation(child.name));
          } else {
            return false;
          }
        }
      }
    }

    // Make sure that aggregation is possible.
    final SqlNode aggregateNode = getAggregate(select);
    if (aggregateNode != null) {
      switch (modality) {
      case STREAM:
        SqlNodeList groupList = select.getGroup();
        if (groupList == null
            || !SqlValidatorUtil.containsMonotonic(scope, groupList)) {
          if (fail) {
            throw newValidationError(aggregateNode,
                Static.RESOURCE.streamMustGroupByMonotonic());
          } else {
            return false;
          }
        }
      }
    }

    // Make sure that ORDER BY is possible.
    final SqlNodeList orderList  = select.getOrderList();
    if (orderList != null && orderList.size() > 0) {
      switch (modality) {
      case STREAM:
        if (!hasSortedPrefix(scope, orderList)) {
          if (fail) {
            throw newValidationError(orderList.get(0),
                Static.RESOURCE.streamMustOrderByMonotonic());
          } else {
            return false;
          }
        }
      }
    }
    return true;
  }

  /** Returns whether the prefix is sorted. */
  private boolean hasSortedPrefix(SelectScope scope, SqlNodeList orderList) {
    return isSortCompatible(scope, orderList.get(0), false);
  }

  private boolean isSortCompatible(SelectScope scope, SqlNode node,
      boolean descending) {
    switch (node.getKind()) {
    case DESCENDING:
      return isSortCompatible(scope, ((SqlCall) node).getOperandList().get(0),
          true);
    }
    final SqlMonotonicity monotonicity = scope.getMonotonicity(node);
    switch (monotonicity) {
    case INCREASING:
    case STRICTLY_INCREASING:
      return !descending;
    case DECREASING:
    case STRICTLY_DECREASING:
      return descending;
    default:
      return false;
    }
  }

  protected void validateWindowClause(SqlSelect select) {
    final SqlNodeList windowList = select.getWindowList();
    if ((windowList == null) || (windowList.size() == 0)) {
      return;
    }

    final SelectScope windowScope = (SelectScope) getFromScope(select);
    assert windowScope != null;

    // 1. ensure window names are simple
    // 2. ensure they are unique within this scope
    for (SqlNode node : windowList) {
      final SqlWindow child = (SqlWindow) node;
      SqlIdentifier declName = child.getDeclName();
      if (!declName.isSimple()) {
        throw newValidationError(declName, RESOURCE.windowNameMustBeSimple());
      }

      if (windowScope.existingWindowName(declName.toString())) {
        throw newValidationError(declName, RESOURCE.duplicateWindowName());
      } else {
        windowScope.addWindowName(declName.toString());
      }
    }

    // 7.10 rule 2
    // Check for pairs of windows which are equivalent.
    for (int i = 0; i < windowList.size(); i++) {
      SqlNode window1 = windowList.get(i);
      for (int j = i + 1; j < windowList.size(); j++) {
        SqlNode window2 = windowList.get(j);
        if (window1.equalsDeep(window2, Litmus.IGNORE)) {
          throw newValidationError(window2, RESOURCE.dupWindowSpec());
        }
      }
    }

    // Hand off to validate window spec components
    windowList.validate(this, windowScope);
  }

  public void validateWith(SqlWith with, SqlValidatorScope scope) {
    final SqlValidatorNamespace namespace = getNamespace(with);
    validateNamespace(namespace, unknownType);
  }

  public void validateWithItem(SqlWithItem withItem) {
    if (withItem.columnList != null) {
      final RelDataType rowType = getValidatedNodeType(withItem.query);
      final int fieldCount = rowType.getFieldCount();
      if (withItem.columnList.size() != fieldCount) {
        throw newValidationError(withItem.columnList,
            RESOURCE.columnCountMismatch());
      }
      SqlValidatorUtil.checkIdentifierListForDuplicates(
          withItem.columnList.getList(), validationErrorFunction);
    } else {
      // Luckily, field names have not been make unique yet.
      final List<String> fieldNames =
          getValidatedNodeType(withItem.query).getFieldNames();
      final int i = Util.firstDuplicate(fieldNames);
      if (i >= 0) {
        throw newValidationError(withItem.query,
            RESOURCE.duplicateColumnAndNoColumnList(fieldNames.get(i)));
      }
    }
  }

  public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {
    // Resolve identifier as a table.
    final SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();
    scope.resolveTable(id.names, catalogReader.nameMatcher(),
        SqlValidatorScope.Path.EMPTY, resolved);
    if (resolved.count() != 1) {
      throw newValidationError(id, RESOURCE.tableNameNotFound(id.toString()));
    }
    // We've found a table. But is it a sequence?
    final SqlValidatorNamespace ns = resolved.only().namespace;
    if (ns instanceof TableNamespace) {
      final Table table = ((RelOptTable) ns.getTable()).unwrap(Table.class);
      switch (table.getJdbcTableType()) {
      case SEQUENCE:
      case TEMPORARY_SEQUENCE:
        return;
      }
    }
    throw newValidationError(id, RESOURCE.notASequence(id.toString()));
  }

  public SqlValidatorScope getWithScope(SqlNode withItem) {
    assert withItem.getKind() == SqlKind.WITH_ITEM;
    return scopes.get(withItem);
  }

  /**
   * Validates the ORDER BY clause of a SELECT statement.
   *
   * @param select Select statement
   */
  protected void validateOrderList(SqlSelect select) {
    // ORDER BY is validated in a scope where aliases in the SELECT clause
    // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
    // is valid.
    SqlNodeList orderList = select.getOrderList();
    if (orderList == null) {
      return;
    }
    if (!shouldAllowIntermediateOrderBy()) {
      if (!cursorSet.contains(select)) {
        throw newValidationError(select, RESOURCE.invalidOrderByPos());
      }
    }
    final SqlValidatorScope orderScope = getOrderScope(select);
    Preconditions.checkNotNull(orderScope != null);

    List<SqlNode> expandList = new ArrayList<>();
    for (SqlNode orderItem : orderList) {
      SqlNode expandedOrderItem = expand(orderItem, orderScope);
      expandList.add(expandedOrderItem);
    }

    SqlNodeList expandedOrderList = new SqlNodeList(
        expandList,
        orderList.getParserPosition());
    select.setOrderBy(expandedOrderList);

    for (SqlNode orderItem : expandedOrderList) {
      validateOrderItem(select, orderItem);
    }
  }

  /**
   * Validates an item in the GROUP BY clause of a SELECT statement.
   *
   * @param select Select statement
   * @param groupByItem GROUP BY clause item
   */
  private void validateGroupByItem(SqlSelect select, SqlNode groupByItem) {
    final SqlValidatorScope groupByScope = getGroupScope(select);
    groupByScope.validateExpr(groupByItem);
  }

  /**
   * Validates an item in the ORDER BY clause of a SELECT statement.
   *
   * @param select Select statement
   * @param orderItem ORDER BY clause item
   */
  private void validateOrderItem(SqlSelect select, SqlNode orderItem) {
    switch (orderItem.getKind()) {
    case DESCENDING:
      validateFeature(RESOURCE.sQLConformance_OrderByDesc(),
          orderItem.getParserPosition());
      validateOrderItem(select,
          ((SqlCall) orderItem).operand(0));
      return;
    }

    final SqlValidatorScope orderScope = getOrderScope(select);
    validateExpr(orderItem, orderScope);
  }

  public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
    final SqlNode newSqlNode =
        new OrderExpressionExpander(select, orderExpr).go();
    if (newSqlNode != orderExpr) {
      final SqlValidatorScope scope = getOrderScope(select);
      inferUnknownTypes(unknownType, scope, newSqlNode);
      final RelDataType type = deriveType(scope, newSqlNode);
      setValidatedNodeType(newSqlNode, type);
    }
    return newSqlNode;
  }

  /**
   * Validates the GROUP BY clause of a SELECT statement. This method is
   * called even if no GROUP BY clause is present.
   */
  protected void validateGroupClause(SqlSelect select) {
    SqlNodeList groupList = select.getGroup();
    if (groupList == null) {
      return;
    }
    validateNoAggs(aggOrOverFinder, groupList, "GROUP BY");
    final SqlValidatorScope groupScope = getGroupScope(select);
    inferUnknownTypes(unknownType, groupScope, groupList);

    // expand the expression in group list.
    List<SqlNode> expandedList = new ArrayList<>();
    for (SqlNode groupItem : groupList) {
      SqlNode expandedItem = expandGroupByOrHavingExpr(groupItem, groupScope, select, false);
      expandedList.add(expandedItem);
    }
    groupList = new SqlNodeList(expandedList, groupList.getParserPosition());
    select.setGroupBy(groupList);
    for (SqlNode groupItem : expandedList) {
      validateGroupByItem(select, groupItem);
    }

    // Nodes in the GROUP BY clause are expressions except if they are calls
    // to the GROUPING SETS, ROLLUP or CUBE operators; this operators are not
    // expressions, because they do not have a type.
    for (SqlNode node : groupList) {
      switch (node.getKind()) {
      case GROUPING_SETS:
      case ROLLUP:
      case CUBE:
        node.validate(this, groupScope);
        break;
      default:
        node.validateExpr(this, groupScope);
      }
    }

    // Derive the type of each GROUP BY item. We don't need the type, but
    // it resolves functions, and that is necessary for deducing
    // monotonicity.
    final SqlValidatorScope selectScope = getSelectScope(select);
    AggregatingSelectScope aggregatingScope = null;
    if (selectScope instanceof AggregatingSelectScope) {
      aggregatingScope = (AggregatingSelectScope) selectScope;
    }
    for (SqlNode groupItem : groupList) {
      if (groupItem instanceof SqlNodeList
          && ((SqlNodeList) groupItem).size() == 0) {
        continue;
      }
      validateGroupItem(groupScope, aggregatingScope, groupItem);
    }

    SqlNode agg = aggFinder.findAgg(groupList);
    if (agg != null) {
      throw newValidationError(agg, RESOURCE.aggregateIllegalInGroupBy());
    }
  }

  private void validateGroupItem(SqlValidatorScope groupScope,
      AggregatingSelectScope aggregatingScope,
      SqlNode groupItem) {
    switch (groupItem.getKind()) {
    case GROUPING_SETS:
    case ROLLUP:
    case CUBE:
      validateGroupingSets(groupScope, aggregatingScope, (SqlCall) groupItem);
      break;
    default:
      if (groupItem instanceof SqlNodeList) {
        break;
      }
      final RelDataType type = deriveType(groupScope, groupItem);
      setValidatedNodeType(groupItem, type);
    }
  }

  private void validateGroupingSets(SqlValidatorScope groupScope,
      AggregatingSelectScope aggregatingScope, SqlCall groupItem) {
    for (SqlNode node : groupItem.getOperandList()) {
      validateGroupItem(groupScope, aggregatingScope, node);
    }
  }

  protected void validateWhereClause(SqlSelect select) {
    // validate WHERE clause
    final SqlNode where = select.getWhere();
    if (where == null) {
      return;
    }
    final SqlValidatorScope whereScope = getWhereScope(select);
    final SqlNode expandedWhere = expand(where, whereScope);
    select.setWhere(expandedWhere);
    validateWhereOrOn(whereScope, expandedWhere, "WHERE");
  }

  protected void validateWhereOrOn(
      SqlValidatorScope scope,
      SqlNode condition,
      String keyword) {
    validateNoAggs(aggOrOverOrGroupFinder, condition, keyword);
    inferUnknownTypes(
        booleanType,
        scope,
        condition);
    condition.validate(this, scope);
    final RelDataType type = deriveType(scope, condition);
    if (!SqlTypeUtil.inBooleanFamily(type)) {
      throw newValidationError(condition, RESOURCE.condMustBeBoolean(keyword));
    }
  }

  protected void validateHavingClause(SqlSelect select) {
    // HAVING is validated in the scope after groups have been created.
    // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
    // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
    // clause is illegal.
    SqlNode having = select.getHaving();
    if (having == null) {
      return;
    }
    final AggregatingScope havingScope =
        (AggregatingScope) getSelectScope(select);
    if (getConformance().isHavingAlias()) {
      SqlNode newExpr = expandGroupByOrHavingExpr(having, havingScope, select, true);
      if (having != newExpr) {
        having = newExpr;
        select.setHaving(newExpr);
      }
    }
    havingScope.checkAggregateExpr(having, true);
    inferUnknownTypes(
        booleanType,
        havingScope,
        having);
    having.validate(this, havingScope);
    final RelDataType type = deriveType(havingScope, having);
    if (!SqlTypeUtil.inBooleanFamily(type)) {
      throw newValidationError(having, RESOURCE.havingMustBeBoolean());
    }
  }

  protected RelDataType validateSelectList(
      final SqlNodeList selectItems,
      SqlSelect select,
      RelDataType targetRowType) {
    // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
    // are ignored.

    // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
    final SqlValidatorScope selectScope = getSelectScope(select);
    final List<SqlNode> expandedSelectItems = new ArrayList<>();
    final Set<String> aliases = Sets.newHashSet();
    final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();

    for (int i = 0; i < selectItems.size(); i++) {
      SqlNode selectItem = selectItems.get(i);
      if (selectItem instanceof SqlSelect) {
        handleScalarSubQuery(
            select,
            (SqlSelect) selectItem,
            expandedSelectItems,
            aliases,
            fieldList);
      } else {
        expandSelectItem(
            selectItem,
            select,
            targetRowType.isStruct()
                && targetRowType.getFieldCount() >= i
                ? targetRowType.getFieldList().get(i).getType()
                : unknownType,
            expandedSelectItems,
            aliases,
            fieldList,
            false);
      }
    }

    // Create the new select list with expanded items.  Pass through
    // the original parser position so that any overall failures can
    // still reference the original input text.
    SqlNodeList newSelectList =
        new SqlNodeList(
            expandedSelectItems,
            selectItems.getParserPosition());
    if (shouldExpandIdentifiers()) {
      select.setSelectList(newSelectList);
    }
    getRawSelectScope(select).setExpandedSelectList(expandedSelectItems);

    // TODO: when SELECT appears as a value sub-query, should be using
    // something other than unknownType for targetRowType
    inferUnknownTypes(targetRowType, selectScope, newSelectList);

    for (SqlNode selectItem : expandedSelectItems) {
      validateNoAggs(groupFinder, selectItem, "SELECT");
      validateExpr(selectItem, selectScope);
    }

    assert fieldList.size() >= aliases.size();
    return typeFactory.createStructType(fieldList);
  }

  /**
   * Validates an expression.
   *
   * @param expr  Expression
   * @param scope Scope in which expression occurs
   */
  private void validateExpr(SqlNode expr, SqlValidatorScope scope) {
    if (expr instanceof SqlCall) {
      final SqlOperator op = ((SqlCall) expr).getOperator();
      if (op.isAggregator() && op.requiresOver()) {
        throw newValidationError(expr,
            RESOURCE.absentOverClause());
      }
    }

    // Call on the expression to validate itself.
    expr.validateExpr(this, scope);

    // Perform any validation specific to the scope. For example, an
    // aggregating scope requires that expressions are valid aggregations.
    scope.validateExpr(expr);
  }

  /**
   * Processes SubQuery found in Select list. Checks that is actually Scalar
   * sub-query and makes proper entries in each of the 3 lists used to create
   * the final rowType entry.
   *
   * @param parentSelect        base SqlSelect item
   * @param selectItem          child SqlSelect from select list
   * @param expandedSelectItems Select items after processing
   * @param aliasList           built from user or system values
   * @param fieldList           Built up entries for each select list entry
   */
  private void handleScalarSubQuery(
      SqlSelect parentSelect,
      SqlSelect selectItem,
      List<SqlNode> expandedSelectItems,
      Set<String> aliasList,
      List<Map.Entry<String, RelDataType>> fieldList) {
    // A scalar sub-query only has one output column.
    if (1 != selectItem.getSelectList().size()) {
      throw newValidationError(selectItem,
          RESOURCE.onlyScalarSubQueryAllowed());
    }

    // No expansion in this routine just append to list.
    expandedSelectItems.add(selectItem);

    // Get or generate alias and add to list.
    final String alias =
        deriveAlias(
            selectItem,
            aliasList.size());
    aliasList.add(alias);

    final SelectScope scope = (SelectScope) getWhereScope(parentSelect);
    final RelDataType type = deriveType(scope, selectItem);
    setValidatedNodeType(selectItem, type);

    // we do not want to pass on the RelRecordType returned
    // by the sub query.  Just the type of the single expression
    // in the sub-query select list.
    assert type instanceof RelRecordType;
    RelRecordType rec = (RelRecordType) type;

    RelDataType nodeType = rec.getFieldList().get(0).getType();
    nodeType = typeFactory.createTypeWithNullability(nodeType, true);
    fieldList.add(Pair.of(alias, nodeType));
  }

  /**
   * Derives a row-type for INSERT and UPDATE operations.
   *
   * @param table            Target table for INSERT/UPDATE
   * @param targetColumnList List of target columns, or null if not specified
   * @param append           Whether to append fields to those in <code>
   *                         baseRowType</code>
   * @return Rowtype
   */
  protected RelDataType createTargetRowType(
      SqlValidatorTable table,
      SqlNodeList targetColumnList,
      boolean append) {
    RelDataType baseRowType = table.getRowType();
    if (targetColumnList == null) {
      return baseRowType;
    }
    List<RelDataTypeField> targetFields = baseRowType.getFieldList();
    final List<Map.Entry<String, RelDataType>> types = new ArrayList<>();
    if (append) {
      for (RelDataTypeField targetField : targetFields) {
        types.add(
            Pair.of(SqlUtil.deriveAliasFromOrdinal(types.size()),
                targetField.getType()));
      }
    }
    final Set<Integer> assignedFields = new HashSet<>();
    final RelOptTable relOptTable = table instanceof RelOptTable
        ? ((RelOptTable) table) : null;
    for (SqlNode node : targetColumnList) {
      SqlIdentifier id = (SqlIdentifier) node;
      RelDataTypeField targetField =
          SqlValidatorUtil.getTargetField(
              baseRowType, typeFactory, id, catalogReader, relOptTable);
      if (targetField == null) {
        throw newValidationError(id,
            RESOURCE.unknownTargetColumn(id.toString()));
      }
      if (!assignedFields.add(targetField.getIndex())) {
        throw newValidationError(id,
            RESOURCE.duplicateTargetColumn(targetField.getName()));
      }
      types.add(targetField);
    }
    return typeFactory.createStructType(types);
  }

  public void validateInsert(SqlInsert insert) {
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    validateNamespace(targetNamespace, unknownType);
    final RelOptTable relOptTable = SqlValidatorUtil.getRelOptTable(
        targetNamespace, catalogReader.unwrap(Prepare.CatalogReader.class), null, null);
    final SqlValidatorTable table = relOptTable == null
        ? targetNamespace.getTable()
        : relOptTable.unwrap(SqlValidatorTable.class);

    // INSERT has an optional column name list.  If present then
    // reduce the rowtype to the columns specified.  If not present
    // then the entire target rowtype is used.
    final RelDataType targetRowType =
        createTargetRowType(
            table,
            insert.getTargetColumnList(),
            false);

    final SqlNode source = insert.getSource();
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, targetRowType);
    } else {
      final SqlValidatorScope scope = scopes.get(source);
      validateQuery(source, scope, targetRowType);
    }

    // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
    // discarding the type inferred by inferUnknownTypes (which was invoked
    // from validateSelect above).  It would be better if that information
    // were used here so that we never saw any untyped nulls during
    // checkTypeAssignment.
    final RelDataType sourceRowType = getNamespace(source).getRowType();
    final RelDataType logicalTargetRowType =
        getLogicalTargetRowType(targetRowType, insert);
    setValidatedNodeType(insert, logicalTargetRowType);
    final RelDataType logicalSourceRowType =
        getLogicalSourceRowType(sourceRowType, insert);

    checkFieldCount(insert, table, logicalSourceRowType, logicalTargetRowType);

    checkTypeAssignment(logicalSourceRowType, logicalTargetRowType, insert);

    checkConstraint(table, source, logicalTargetRowType);

    validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT);
  }

  /**
   * Validates insert values against the constraint of a modifiable view.
   *
   * @param validatorTable Table that may wrap a ModifiableViewTable
   * @param source        The values being inserted
   * @param targetRowType The target type for the view
   */
  private void checkConstraint(
      SqlValidatorTable validatorTable,
      SqlNode source,
      RelDataType targetRowType) {
    final ModifiableViewTable modifiableViewTable =
        validatorTable.unwrap(ModifiableViewTable.class);
    if (modifiableViewTable != null && source instanceof SqlCall) {
      final Table table = modifiableViewTable.unwrap(Table.class);
      final RelDataType tableRowType = table.getRowType(typeFactory);
      final List<RelDataTypeField> tableFields = tableRowType.getFieldList();

      // Get the mapping from column indexes of the underlying table
      // to the target columns and view constraints.
      final Map<Integer, RelDataTypeField> tableIndexToTargetField =
          SqlValidatorUtil.getIndexToFieldMap(tableFields, targetRowType);
      final Map<Integer, RexNode> projectMap =
          RelOptUtil.getColumnConstraints(modifiableViewTable, targetRowType, typeFactory);

      // Determine columns (indexed to the underlying table) that need
      // to be validated against the view constraint.
      final ImmutableBitSet targetColumns =
          ImmutableBitSet.of(tableIndexToTargetField.keySet());
      final ImmutableBitSet constrainedColumns =
          ImmutableBitSet.of(projectMap.keySet());
      final ImmutableBitSet constrainedTargetColumns =
          targetColumns.intersect(constrainedColumns);

      // Validate insert values against the view constraint.
      final List<SqlNode> values = ((SqlCall) source).getOperandList();
      for (final int colIndex : constrainedTargetColumns.asList()) {
        final String colName = tableFields.get(colIndex).getName();
        final RelDataTypeField targetField = tableIndexToTargetField.get(colIndex);
        for (SqlNode row : values) {
          final SqlCall call = (SqlCall) row;
          final SqlNode sourceValue = call.operand(targetField.getIndex());
          final ValidationError validationError =
              new ValidationError(sourceValue,
                  RESOURCE.viewConstraintNotSatisfied(colName,
                      Util.last(validatorTable.getQualifiedName())));
          RelOptUtil.validateValueAgainstConstraint(sourceValue,
              projectMap.get(colIndex), validationError);
        }
      }
    }
  }

  /**
   * Validates updates against the constraint of a modifiable view.
   *
   * @param validatorTable A {@link SqlValidatorTable} that may wrap a
   *                       ModifiableViewTable
   * @param update         The UPDATE parse tree node
   * @param targetRowType  The target type
   */
  private void checkConstraint(
      SqlValidatorTable validatorTable,
      SqlUpdate update,
      RelDataType targetRowType) {
    final ModifiableViewTable modifiableViewTable =
        validatorTable.unwrap(ModifiableViewTable.class);
    if (modifiableViewTable != null) {
      final Table table = modifiableViewTable.unwrap(Table.class);
      final RelDataType tableRowType = table.getRowType(typeFactory);

      final Map<Integer, RexNode> projectMap =
          RelOptUtil.getColumnConstraints(modifiableViewTable, targetRowType,
              typeFactory);
      final Map<String, Integer> nameToIndex =
          SqlValidatorUtil.mapNameToIndex(tableRowType.getFieldList());

      // Validate update values against the view constraint.
      final List<SqlNode> targets = update.getTargetColumnList().getList();
      final List<SqlNode> sources = update.getSourceExpressionList().getList();
      for (final Pair<SqlNode, SqlNode> column : Pair.zip(targets, sources)) {
        final String columnName = ((SqlIdentifier) column.left).getSimple();
        final Integer columnIndex = nameToIndex.get(columnName);
        if (projectMap.containsKey(columnIndex)) {
          final RexNode columnConstraint = projectMap.get(columnIndex);
          final ValidationError validationError =
              new ValidationError(column.right,
                  RESOURCE.viewConstraintNotSatisfied(columnName,
                      Util.last(validatorTable.getQualifiedName())));
          RelOptUtil.validateValueAgainstConstraint(column.right,
              columnConstraint, validationError);
        }
      }
    }
  }

  private void checkFieldCount(
      SqlNode node,
      SqlValidatorTable table,
      RelDataType logicalSourceRowType,
      RelDataType logicalTargetRowType) {
    final int sourceFieldCount = logicalSourceRowType.getFieldCount();
    final int targetFieldCount = logicalTargetRowType.getFieldCount();
    if (sourceFieldCount != targetFieldCount) {
      throw newValidationError(node,
          RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount));
    }
    // Ensure that non-nullable fields are targeted.
    final InitializerContext rexBuilder =
        new InitializerContext() {
          public RexBuilder getRexBuilder() {
            return new RexBuilder(typeFactory);
          }
        };
    for (final RelDataTypeField field : table.getRowType().getFieldList()) {
      if (!field.getType().isNullable()) {
        final RelDataTypeField targetField =
            logicalTargetRowType.getField(field.getName(), true, false);
        if (targetField == null
            && !table.columnHasDefaultValue(table.getRowType(),
                field.getIndex(), rexBuilder)) {
          throw newValidationError(node,
              RESOURCE.columnNotNullable(field.getName()));
        }
      }
    }
  }

  protected RelDataType getLogicalTargetRowType(
      RelDataType targetRowType,
      SqlInsert insert) {
    if (insert.getTargetColumnList() == null
        && conformance.isInsertSubsetColumnsAllowed()) {
      // Target an implicit subset of columns.
      final SqlNode source = insert.getSource();
      final RelDataType sourceRowType = getNamespace(source).getRowType();
      final RelDataType logicalSourceRowType =
          getLogicalSourceRowType(sourceRowType, insert);
      final RelDataType implicitTargetRowType =
          typeFactory.createStructType(
              targetRowType.getFieldList()
                  .subList(0, logicalSourceRowType.getFieldCount()));
      final SqlValidatorNamespace targetNamespace = getNamespace(insert);
      validateNamespace(targetNamespace, implicitTargetRowType);
      return implicitTargetRowType;
    } else {
      // Either the set of columns are explicitly targeted, or target the full
      // set of columns.
      return targetRowType;
    }
  }

  protected RelDataType getLogicalSourceRowType(
      RelDataType sourceRowType,
      SqlInsert insert) {
    return sourceRowType;
  }

  protected void checkTypeAssignment(
      RelDataType sourceRowType,
      RelDataType targetRowType,
      final SqlNode query) {
    // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
    // representing system-maintained columns, so stop after all sources
    // matched
    List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
    List<RelDataTypeField> targetFields = targetRowType.getFieldList();
    final int sourceCount = sourceFields.size();
    for (int i = 0; i < sourceCount; ++i) {
      RelDataType sourceType = sourceFields.get(i).getType();
      RelDataType targetType = targetFields.get(i).getType();
      if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
        // FRG-255:  account for UPDATE rewrite; there's
        // probably a better way to do this.
        int iAdjusted = i;
        if (query instanceof SqlUpdate) {
          int nUpdateColumns =
              ((SqlUpdate) query).getTargetColumnList().size();
          assert sourceFields.size() >= nUpdateColumns;
          iAdjusted -= sourceFields.size() - nUpdateColumns;
        }
        SqlNode node = getNthExpr(query, iAdjusted, sourceCount);
        String targetTypeString;
        String sourceTypeString;
        if (SqlTypeUtil.areCharacterSetsMismatched(
            sourceType,
            targetType)) {
          sourceTypeString = sourceType.getFullTypeString();
          targetTypeString = targetType.getFullTypeString();
        } else {
          sourceTypeString = sourceType.toString();
          targetTypeString = targetType.toString();
        }
        throw newValidationError(node,
            RESOURCE.typeNotAssignable(
                targetFields.get(i).getName(), targetTypeString,
                sourceFields.get(i).getName(), sourceTypeString));
      }
    }
  }

  /**
   * Locates the n'th expression in an INSERT or UPDATE query.
   *
   * @param query       Query
   * @param ordinal     Ordinal of expression
   * @param sourceCount Number of expressions
   * @return Ordinal'th expression, never null
   */
  private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
    if (query instanceof SqlInsert) {
      SqlInsert insert = (SqlInsert) query;
      if (insert.getTargetColumnList() != null) {
        return insert.getTargetColumnList().get(ordinal);
      } else {
        return getNthExpr(
            insert.getSource(),
            ordinal,
            sourceCount);
      }
    } else if (query instanceof SqlUpdate) {
      SqlUpdate update = (SqlUpdate) query;
      if (update.getTargetColumnList() != null) {
        return update.getTargetColumnList().get(ordinal);
      } else if (update.getSourceExpressionList() != null) {
        return update.getSourceExpressionList().get(ordinal);
      } else {
        return getNthExpr(
            update.getSourceSelect(),
            ordinal,
            sourceCount);
      }
    } else if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      if (select.getSelectList().size() == sourceCount) {
        return select.getSelectList().get(ordinal);
      } else {
        return query; // give up
      }
    } else {
      return query; // give up
    }
  }

  public void validateDelete(SqlDelete call) {
    final SqlSelect sqlSelect = call.getSourceSelect();
    validateSelect(sqlSelect, unknownType);

    final SqlValidatorNamespace targetNamespace = getNamespace(call);
    validateNamespace(targetNamespace, unknownType);
    final SqlValidatorTable table = targetNamespace.getTable();

    validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE);
  }

  public void validateUpdate(SqlUpdate call) {
    final SqlValidatorNamespace targetNamespace = getNamespace(call);
    validateNamespace(targetNamespace, unknownType);
    final RelOptTable relOptTable = SqlValidatorUtil.getRelOptTable(
        targetNamespace, catalogReader.unwrap(Prepare.CatalogReader.class), null, null);
    final SqlValidatorTable table = relOptTable == null
        ? targetNamespace.getTable()
        : relOptTable.unwrap(SqlValidatorTable.class);

    final RelDataType targetRowType =
        createTargetRowType(
            table,
            call.getTargetColumnList(),
            true);

    final SqlSelect select = call.getSourceSelect();
    validateSelect(select, targetRowType);

    final RelDataType sourceRowType = getNamespace(call).getRowType();
    checkTypeAssignment(sourceRowType, targetRowType, call);

    checkConstraint(table, call, targetRowType);

    validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);
  }

  public void validateMerge(SqlMerge call) {
    SqlSelect sqlSelect = call.getSourceSelect();
    // REVIEW zfong 5/25/06 - Does an actual type have to be passed into
    // validateSelect()?

    // REVIEW jvs 6-June-2006:  In general, passing unknownType like
    // this means we won't be able to correctly infer the types
    // for dynamic parameter markers (SET x = ?).  But
    // maybe validateUpdate and validateInsert below will do
    // the job?

    // REVIEW ksecretan 15-July-2011: They didn't get a chance to
    // since validateSelect() would bail.
    // Let's use the update/insert targetRowType when available.
    IdentifierNamespace targetNamespace =
        (IdentifierNamespace) getNamespace(call.getTargetTable());
    validateNamespace(targetNamespace, unknownType);

    SqlValidatorTable table = targetNamespace.getTable();
    validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);

    RelDataType targetRowType = unknownType;

    if (call.getUpdateCall() != null) {
      targetRowType = createTargetRowType(
          table,
          call.getUpdateCall().getTargetColumnList(),
          true);
    }
    if (call.getInsertCall() != null) {
      targetRowType = createTargetRowType(
          table,
          call.getInsertCall().getTargetColumnList(),
          false);
    }

    validateSelect(sqlSelect, targetRowType);

    if (call.getUpdateCall() != null) {
      validateUpdate(call.getUpdateCall());
    }
    if (call.getInsertCall() != null) {
      validateInsert(call.getInsertCall());
    }
  }

  /**
   * Validates access to a table.
   *
   * @param table          Table
   * @param requiredAccess Access requested on table
   */
  private void validateAccess(
      SqlNode node,
      SqlValidatorTable table,
      SqlAccessEnum requiredAccess) {
    if (table != null) {
      SqlAccessType access = table.getAllowedAccess();
      if (!access.allowsAccess(requiredAccess)) {
        throw newValidationError(node,
            RESOURCE.accessNotAllowed(requiredAccess.name(),
                table.getQualifiedName().toString()));
      }
    }
  }

  /**
   * Validates a VALUES clause.
   *
   * @param node          Values clause
   * @param targetRowType Row type which expression must conform to
   * @param scope         Scope within which clause occurs
   */
  protected void validateValues(
      SqlCall node,
      RelDataType targetRowType,
      final SqlValidatorScope scope) {
    assert node.getKind() == SqlKind.VALUES;

    final List<SqlNode> operands = node.getOperandList();
    for (SqlNode operand : operands) {
      if (!(operand.getKind() == SqlKind.ROW)) {
        throw Util.needToImplement(
            "Values function where operands are scalars");
      }

      SqlCall rowConstructor = (SqlCall) operand;
      if (conformance.isInsertSubsetColumnsAllowed() && targetRowType.isStruct()
          && rowConstructor.operandCount() < targetRowType.getFieldCount()) {
        targetRowType =
            typeFactory.createStructType(
                targetRowType.getFieldList()
                    .subList(0, rowConstructor.operandCount()));
      } else if (targetRowType.isStruct()
          && rowConstructor.operandCount() != targetRowType.getFieldCount()) {
        return;
      }

      inferUnknownTypes(
          targetRowType,
          scope,
          rowConstructor);

      if (targetRowType.isStruct()) {
        for (Pair<SqlNode, RelDataTypeField> pair
            : Pair.zip(rowConstructor.getOperandList(),
                targetRowType.getFieldList())) {
          if (!pair.right.getType().isNullable()
              && SqlUtil.isNullLiteral(pair.left, false)) {
            throw newValidationError(node,
                RESOURCE.columnNotNullable(pair.right.getName()));
          }
        }
      }
    }

    for (SqlNode operand : operands) {
      operand.validate(this, scope);
    }

    // validate that all row types have the same number of columns
    //  and that expressions in each column are compatible.
    // A values expression is turned into something that looks like
    // ROW(type00, type01,...), ROW(type11,...),...
    final int rowCount = operands.size();
    if (rowCount >= 2) {
      SqlCall firstRow = (SqlCall) operands.get(0);
      final int columnCount = firstRow.operandCount();

      // 1. check that all rows have the same cols length
      for (SqlNode operand : operands) {
        SqlCall thisRow = (SqlCall) operand;
        if (columnCount != thisRow.operandCount()) {
          throw newValidationError(node,
              RESOURCE.incompatibleValueType(
                  SqlStdOperatorTable.VALUES.getName()));
        }
      }

      // 2. check if types at i:th position in each row are compatible
      for (int col = 0; col < columnCount; col++) {
        final int c = col;
        final RelDataType type =
            typeFactory.leastRestrictive(
                new AbstractList<RelDataType>() {
                  public RelDataType get(int row) {
                    SqlCall thisRow = (SqlCall) operands.get(row);
                    return deriveType(scope, thisRow.operand(c));
                  }

                  public int size() {
                    return rowCount;
                  }
                });

        if (null == type) {
          throw newValidationError(node,
              RESOURCE.incompatibleValueType(
                  SqlStdOperatorTable.VALUES.getName()));
        }
      }
    }
  }

  public void validateDataType(SqlDataTypeSpec dataType) {
  }

  public void validateDynamicParam(SqlDynamicParam dynamicParam) {
  }

  /**
   * Throws a validator exception with access to the validator context.
   * The exception is determined when an instance is created.
   */
  private class ValidationError implements Supplier<CalciteContextException> {
    private final SqlNode sqlNode;
    private final Resources.ExInst<SqlValidatorException> validatorException;

    ValidationError(SqlNode sqlNode,
        Resources.ExInst<SqlValidatorException> validatorException) {
      this.sqlNode = sqlNode;
      this.validatorException = validatorException;
    }

    public CalciteContextException get() {
      return newValidationError(sqlNode, validatorException);
    }
  }

  /**
   * Throws a validator exception with access to the validator context.
   * The exception is determined when the function is applied.
   */
  class ValidationErrorFunction
      implements Function2<SqlNode, Resources.ExInst<SqlValidatorException>,
            CalciteContextException> {
    @Override public CalciteContextException apply(
        SqlNode v0, Resources.ExInst<SqlValidatorException> v1) {
      return newValidationError(v0, v1);
    }
  }

  public ValidationErrorFunction getValidationErrorFunction() {
    return validationErrorFunction;
  }

  public CalciteContextException newValidationError(SqlNode node,
      Resources.ExInst<SqlValidatorException> e) {
    assert node != null;
    final SqlParserPos pos = node.getParserPosition();
    return SqlUtil.newContextException(pos, e);
  }

  protected SqlWindow getWindowByName(
      SqlIdentifier id,
      SqlValidatorScope scope) {
    SqlWindow window = null;
    if (id.isSimple()) {
      final String name = id.getSimple();
      window = scope.lookupWindow(name);
    }
    if (window == null) {
      throw newValidationError(id, RESOURCE.windowNotFound(id.toString()));
    }
    return window;
  }

  public SqlWindow resolveWindow(
      SqlNode windowOrRef,
      SqlValidatorScope scope,
      boolean populateBounds) {
    SqlWindow window;
    if (windowOrRef instanceof SqlIdentifier) {
      window = getWindowByName((SqlIdentifier) windowOrRef, scope);
    } else {
      window = (SqlWindow) windowOrRef;
    }
    while (true) {
      final SqlIdentifier refId = window.getRefName();
      if (refId == null) {
        break;
      }
      final String refName = refId.getSimple();
      SqlWindow refWindow = scope.lookupWindow(refName);
      if (refWindow == null) {
        throw newValidationError(refId, RESOURCE.windowNotFound(refName));
      }
      window = window.overlay(refWindow, this);
    }

    if (populateBounds) {
      window.populateBounds();
    }
    return window;
  }

  public SqlNode getOriginal(SqlNode expr) {
    SqlNode original = originalExprs.get(expr);
    if (original == null) {
      original = expr;
    }
    return original;
  }

  public void setOriginal(SqlNode expr, SqlNode original) {
    // Don't overwrite the original original.
    if (originalExprs.get(expr) == null) {
      originalExprs.put(expr, original);
    }
  }

  SqlValidatorNamespace lookupFieldNamespace(RelDataType rowType, String name) {
    final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
    final RelDataTypeField field = nameMatcher.field(rowType, name);
    return new FieldNamespace(this, field.getType());
  }

  public void validateWindow(
      SqlNode windowOrId,
      SqlValidatorScope scope,
      SqlCall call) {
    // Enable nested aggregates with window aggregates (OVER operator)
    inWindow = true;

    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
    case IDENTIFIER:
      // Just verify the window exists in this query.  It will validate
      // when the definition is processed
      targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
      break;
    case WINDOW:
      targetWindow = (SqlWindow) windowOrId;
      break;
    default:
      throw Util.unexpected(windowOrId.getKind());
    }

    assert targetWindow.getWindowCall() == null;
    targetWindow.setWindowCall(call);
    targetWindow.validate(this, scope);
    targetWindow.setWindowCall(null);
    call.validate(this, scope);

    validateAggregateParams(call, null, scope);

    // Disable nested aggregates post validation
    inWindow = false;
  }

  @Override public void validateMatchRecognize(SqlCall call) {
    final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
    final MatchRecognizeScope scope =
        (MatchRecognizeScope) getMatchRecognizeScope(matchRecognize);

    final MatchRecognizeNamespace ns =
        getNamespace(call).unwrap(MatchRecognizeNamespace.class);
    assert ns.rowType == null;

    // retrieve pattern variables used in pattern and subset
    SqlNode pattern = matchRecognize.getPattern();
    PatternVarVisitor visitor = new PatternVarVisitor(scope);
    pattern.accept(visitor);

    validateDefinitions(matchRecognize, scope);

    // validate AFTER ... SKIP TO
    final SqlNode skipTo = matchRecognize.getAfter();
    if (skipTo instanceof SqlCall) {
      final SqlCall skipToCall = (SqlCall) skipTo;
      final SqlIdentifier id = skipToCall.operand(0);
      if (!scope.getPatternVars().contains(id.getSimple())) {
        throw newValidationError(id,
            RESOURCE.unknownPattern(id.getSimple()));
      }
    }

    List<Map.Entry<String, RelDataType>> fields =
        validateMeasure(matchRecognize, scope);
    final RelDataType rowType = typeFactory.createStructType(fields);
    if (matchRecognize.getMeasureList().size() == 0) {
      ns.setType(getNamespace(matchRecognize.getTableRef()).getRowType());
    } else {
      ns.setType(rowType);
    }
  }

  private List<Map.Entry<String, RelDataType>> validateMeasure(SqlMatchRecognize mr,
      MatchRecognizeScope scope) {
    final List<String> aliases = new ArrayList<>();
    final List<SqlNode> sqlNodes = new ArrayList<>();
    final SqlNodeList measures = mr.getMeasureList();
    final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();

    for (SqlNode measure : measures) {
      assert measure instanceof SqlCall;
      final String alias = deriveAlias(measure, aliases.size());
      aliases.add(alias);

      SqlNode expand = expand(measure, scope);
      expand = navigationInMeasure(expand);
      setOriginal(expand, measure);

      inferUnknownTypes(unknownType, scope, expand);
      final RelDataType type = deriveType(scope, expand);
      setValidatedNodeType(measure, type);

      fields.add(Pair.of(alias, type));
      sqlNodes.add(
          SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, expand,
              new SqlIdentifier(alias, SqlParserPos.ZERO)));
    }

    SqlNodeList list = new SqlNodeList(sqlNodes, measures.getParserPosition());
    inferUnknownTypes(unknownType, scope, list);

    for (SqlNode node : list) {
      validateExpr(node, scope);
    }

    mr.setOperand(SqlMatchRecognize.OPERAND_MEASURES, list);

    return fields;
  }

  private SqlNode navigationInMeasure(SqlNode node) {
    Set<String> prefix = node.accept(new PatternValidator(true));
    Util.discard(prefix);
    List<SqlNode> ops = ((SqlCall) node).getOperandList();

    SqlOperator defaultOp = SqlStdOperatorTable.FINAL;
    if (!isRunningOrFinal(ops.get(0).getKind())
        || ops.get(0).getKind() == SqlKind.RUNNING) {
      SqlNode newNode = defaultOp.createCall(SqlParserPos.ZERO, ops.get(0));
      node = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, newNode, ops.get(1));
    }
    return node;
  }

  private void validateDefinitions(SqlMatchRecognize mr,
      MatchRecognizeScope scope) {
    final Set<String> aliases = catalogReader.nameMatcher().isCaseSensitive()
        ? new LinkedHashSet<String>()
        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (SqlNode item : mr.getPatternDefList().getList()) {
      final String alias = alias(item);
      if (!aliases.add(alias)) {
        throw newValidationError(item,
            Static.RESOURCE.patternVarAlreadyDefined(alias));
      }
      scope.addPatternVar(alias);
    }

    final List<SqlNode> sqlNodes = new ArrayList<>();
    for (SqlNode item : mr.getPatternDefList().getList()) {
      final String alias = alias(item);
      SqlNode expand = expand(item, scope);
      expand = navigationInDefine(expand, alias);
      setOriginal(expand, item);

      inferUnknownTypes(booleanType, scope, expand);
      expand.validate(this, scope);

      // Some extra work need required here.
      // In PREV, NEXT, FINAL and LAST, only one pattern variable is allowed.
      sqlNodes.add(
          SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, expand,
              new SqlIdentifier(alias, SqlParserPos.ZERO)));

      final RelDataType type = deriveType(scope, expand);
      if (!SqlTypeUtil.inBooleanFamily(type)) {
        throw newValidationError(expand, RESOURCE.condMustBeBoolean("DEFINE"));
      }
      setValidatedNodeType(item, type);
    }

    SqlNodeList list =
        new SqlNodeList(sqlNodes, mr.getPatternDefList().getParserPosition());
    inferUnknownTypes(unknownType, scope, list);
    for (SqlNode node : list) {
      validateExpr(node, scope);
    }
    mr.setOperand(SqlMatchRecognize.OPERAND_PATTERN_DEFINES, list);
  }

  private static String alias(SqlNode item) {
    assert item instanceof SqlCall;
    final SqlIdentifier identifier = ((SqlCall) item).operand(1);
    return identifier.getSimple();
  }

  /**
   * check all pattern var within one function is the same
   */
  private SqlNode navigationInDefine(SqlNode node, String alpha) {
    Set<String> prefix = node.accept(new PatternValidator(false));
    Util.discard(prefix);
    node = new NavigationExpander().go(node);
    node = new NavigationReplacer(alpha).go(node);
    return node;
  }

  public void validateAggregateParams(SqlCall aggCall, SqlNode filter,
      SqlValidatorScope scope) {
    // For "agg(expr)", expr cannot itself contain aggregate function
    // invocations.  For example, "SUM(2 * MAX(x))" is illegal; when
    // we see it, we'll report the error for the SUM (not the MAX).
    // For more than one level of nesting, the error which results
    // depends on the traversal order for validation.
    //
    // For a windowed aggregate "agg(expr)", expr can contain an aggregate
    // function. For example,
    //   SELECT AVG(2 * MAX(x)) OVER (PARTITION BY y)
    //   FROM t
    //   GROUP BY y
    // is legal. Only one level of nesting is allowed since non-windowed
    // aggregates cannot nest aggregates.

    // Store nesting level of each aggregate. If an aggregate is found at an invalid
    // nesting level, throw an assert.
    final AggFinder a;
    if (inWindow) {
      a = overFinder;
    } else {
      a = aggOrOverFinder;
    }

    for (SqlNode param : aggCall.getOperandList()) {
      if (a.findAgg(param) != null) {
        throw newValidationError(aggCall, RESOURCE.nestedAggIllegal());
      }
    }
    if (filter != null) {
      if (a.findAgg(filter) != null) {
        throw newValidationError(filter, RESOURCE.aggregateInFilterIllegal());
      }
    }
  }

  public void validateCall(
      SqlCall call,
      SqlValidatorScope scope) {
    final SqlOperator operator = call.getOperator();
    if ((call.operandCount() == 0)
        && (operator.getSyntax() == SqlSyntax.FUNCTION_ID)
        && !call.isExpanded()
        && !conformance.allowNiladicParentheses()) {
      // For example, "LOCALTIME()" is illegal. (It should be
      // "LOCALTIME", which would have been handled as a
      // SqlIdentifier.)
      throw handleUnresolvedFunction(call, (SqlFunction) operator,
          ImmutableList.<RelDataType>of(), null);
    }

    SqlValidatorScope operandScope = scope.getOperandScope(call);

    if (operator instanceof SqlFunction
        && ((SqlFunction) operator).getFunctionType()
            == SqlFunctionCategory.MATCH_RECOGNIZE
        && !(operandScope instanceof MatchRecognizeScope)) {
      throw newValidationError(call,
          Static.RESOURCE.functionMatchRecognizeOnly(call.toString()));
    }
    // Delegate validation to the operator.
    operator.validateCall(call, this, scope, operandScope);
  }

  /**
   * Validates that a particular feature is enabled. By default, all features
   * are enabled; subclasses may override this method to be more
   * discriminating.
   *
   * @param feature feature being used, represented as a resource instance
   * @param context parser position context for error reporting, or null if
   */
  protected void validateFeature(
      Feature feature,
      SqlParserPos context) {
    // By default, do nothing except to verify that the resource
    // represents a real feature definition.
    assert feature.getProperties().get("FeatureDefinition") != null;
  }

  public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    final Expander expander = new Expander(this, scope);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }
    return newExpr;
  }

  public SqlNode expandGroupByOrHavingExpr(SqlNode expr, SqlValidatorScope scope, SqlSelect select,
      boolean havingExpression) {
    final Expander expander = new ExtendedExpander(this, scope, select, expr, havingExpression);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }
    return newExpr;
  }

  public boolean isSystemField(RelDataTypeField field) {
    return false;
  }

  public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
    if (sqlQuery instanceof SqlExplain) {
      return Collections.emptyList();
    }
    final RelDataType rowType = getValidatedNodeType(sqlQuery);
    final int fieldCount = rowType.getFieldCount();
    if (!sqlQuery.isA(SqlKind.QUERY)) {
      return Collections.nCopies(fieldCount, null);
    }
    final List<List<String>> list = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      list.add(getFieldOrigin(sqlQuery, i));
    }
    return ImmutableNullableList.copyOf(list);
  }

  private List<String> getFieldOrigin(SqlNode sqlQuery, int i) {
    if (sqlQuery instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlQuery;
      final SelectScope scope = getRawSelectScope(sqlSelect);
      final List<SqlNode> selectList = scope.getExpandedSelectList();
      final SqlNode selectItem = stripAs(selectList.get(i));
      if (selectItem instanceof SqlIdentifier) {
        final SqlQualified qualified =
            scope.fullyQualify((SqlIdentifier) selectItem);
        SqlValidatorNamespace namespace = qualified.namespace;
        final SqlValidatorTable table = namespace.getTable();
        if (table == null) {
          return null;
        }
        final List<String> origin =
            new ArrayList<>(table.getQualifiedName());
        for (String name : qualified.suffix()) {
          namespace = namespace.lookupChild(name);
          if (namespace == null) {
            return null;
          }
          origin.add(name);
        }
        return origin;
      }
      return null;
    } else if (sqlQuery instanceof SqlOrderBy) {
      return getFieldOrigin(((SqlOrderBy) sqlQuery).query, i);
    } else {
      return null;
    }
  }

  public RelDataType getParameterRowType(SqlNode sqlQuery) {
    // NOTE: We assume that bind variables occur in depth-first tree
    // traversal in the same order that they occurred in the SQL text.
    final List<RelDataType> types = new ArrayList<>();
    sqlQuery.accept(
        new SqlShuttle() {
          @Override public SqlNode visit(SqlDynamicParam param) {
            RelDataType type = getValidatedNodeType(param);
            types.add(type);
            return param;
          }
        });
    return typeFactory.createStructType(
        types,
        new AbstractList<String>() {
          @Override public String get(int index) {
            return "?" + index;
          }

          @Override public int size() {
            return types.size();
          }
        });
  }

  public void validateColumnListParams(
      SqlFunction function,
      List<RelDataType> argTypes,
      List<SqlNode> operands) {
    throw new UnsupportedOperationException();
  }

  private static boolean isPhysicalNavigation(SqlKind kind) {
    return kind == SqlKind.PREV || kind == SqlKind.NEXT;
  }

  private static boolean isLogicalNavigation(SqlKind kind) {
    return kind == SqlKind.FIRST || kind == SqlKind.LAST;
  }

  private static boolean isAggregation(SqlKind kind) {
    return kind == SqlKind.SUM || kind == SqlKind.SUM0
        || kind == SqlKind.AVG || kind == SqlKind.COUNT
        || kind == SqlKind.MAX || kind == SqlKind.MIN;
  }

  private static boolean isRunningOrFinal(SqlKind kind) {
    return kind == SqlKind.RUNNING || kind == SqlKind.FINAL;
  }

  private static boolean isSingleVarRequired(SqlKind kind) {
    return isPhysicalNavigation(kind)
        || isLogicalNavigation(kind)
        || isAggregation(kind);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Common base class for DML statement namespaces.
   */
  public static class DmlNamespace extends IdentifierNamespace {
    protected DmlNamespace(SqlValidatorImpl validator, SqlNode id,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, id, enclosingNode, parentScope);
    }
  }

  /**
   * Namespace for an INSERT statement.
   */
  private static class InsertNamespace extends DmlNamespace {
    private final SqlInsert node;

    public InsertNamespace(SqlValidatorImpl validator, SqlInsert node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlInsert getNode() {
      return node;
    }
  }

  /**
   * Namespace for an UPDATE statement.
   */
  private static class UpdateNamespace extends DmlNamespace {
    private final SqlUpdate node;

    public UpdateNamespace(SqlValidatorImpl validator, SqlUpdate node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlUpdate getNode() {
      return node;
    }
  }

  /**
   * Namespace for a DELETE statement.
   */
  private static class DeleteNamespace extends DmlNamespace {
    private final SqlDelete node;

    public DeleteNamespace(SqlValidatorImpl validator, SqlDelete node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlDelete getNode() {
      return node;
    }
  }

  /**
   * Namespace for a MERGE statement.
   */
  private static class MergeNamespace extends DmlNamespace {
    private final SqlMerge node;

    public MergeNamespace(SqlValidatorImpl validator, SqlMerge node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlMerge getNode() {
      return node;
    }
  }

  /**
   * retrieve pattern variables defined
   */
  private class PatternVarVisitor implements SqlVisitor<Void> {
    private MatchRecognizeScope scope;
    public PatternVarVisitor(MatchRecognizeScope scope) {
      this.scope = scope;
    }

    @Override public Void visit(SqlLiteral literal) {
      return null;
    }

    @Override public Void visit(SqlCall call) {
      for (int i = 0; i < call.getOperandList().size(); i++) {
        call.getOperandList().get(i).accept(this);
      }
      return null;
    }

    @Override public Void visit(SqlNodeList nodeList) {
      throw Util.needToImplement(nodeList);
    }

    @Override public Void visit(SqlIdentifier id) {
      Preconditions.checkArgument(id.isSimple());
      scope.addPatternVar(id.getSimple());
      return null;
    }

    @Override public Void visit(SqlDataTypeSpec type) {
      throw Util.needToImplement(type);
    }

    @Override public Void visit(SqlDynamicParam param) {
      throw Util.needToImplement(param);
    }

    @Override public Void visit(SqlIntervalQualifier intervalQualifier) {
      throw Util.needToImplement(intervalQualifier);
    }
  }

  /**
   * Visitor which derives the type of a given {@link SqlNode}.
   *
   * <p>Each method must return the derived type. This visitor is basically a
   * single-use dispatcher; the visit is never recursive.
   */
  private class DeriveTypeVisitor implements SqlVisitor<RelDataType> {
    private final SqlValidatorScope scope;

    public DeriveTypeVisitor(SqlValidatorScope scope) {
      this.scope = scope;
    }

    public RelDataType visit(SqlLiteral literal) {
      return literal.createSqlType(typeFactory);
    }

    public RelDataType visit(SqlCall call) {
      final SqlOperator operator = call.getOperator();
      return operator.deriveType(SqlValidatorImpl.this, scope, call);
    }

    public RelDataType visit(SqlNodeList nodeList) {
      // Operand is of a type that we can't derive a type for. If the
      // operand is of a peculiar type, such as a SqlNodeList, then you
      // should override the operator's validateCall() method so that it
      // doesn't try to validate that operand as an expression.
      throw Util.needToImplement(nodeList);
    }

    public RelDataType visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      SqlCall call = SqlUtil.makeCall(opTab, id);
      if (call != null) {
        return call.getOperator().validateOperands(
            SqlValidatorImpl.this,
            scope,
            call);
      }

      RelDataType type = null;
      if (!(scope instanceof EmptyScope)) {
        id = scope.fullyQualify(id).identifier;
      }

      // Resolve the longest prefix of id that we can
      int i;
      for (i = id.names.size() - 1; i > 0; i--) {
        // REVIEW jvs 9-June-2005: The name resolution rules used
        // here are supposed to match SQL:2003 Part 2 Section 6.6
        // (identifier chain), but we don't currently have enough
        // information to get everything right.  In particular,
        // routine parameters are currently looked up via resolve;
        // we could do a better job if they were looked up via
        // resolveColumn.

        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final SqlValidatorScope.ResolvedImpl resolved =
            new SqlValidatorScope.ResolvedImpl();
        scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved);
        if (resolved.count() == 1) {
          // There's a namespace with the name we seek.
          final SqlValidatorScope.Resolve resolve = resolved.only();
          type = resolve.rowType();
          for (SqlValidatorScope.Step p : Util.skip(resolve.path.steps())) {
            type = type.getFieldList().get(p.i).getType();
          }
          break;
        }
      }

      // Give precedence to namespace found, unless there
      // are no more identifier components.
      if (type == null || id.names.size() == 1) {
        // See if there's a column with the name we seek in
        // precisely one of the namespaces in this scope.
        RelDataType colType = scope.resolveColumn(id.names.get(0), id);
        if (colType != null) {
          type = colType;
        }
        ++i;
      }

      if (type == null) {
        final SqlIdentifier last = id.getComponent(i - 1, i);
        throw newValidationError(last,
            RESOURCE.unknownIdentifier(last.toString()));
      }

      // Resolve rest of identifier
      for (; i < id.names.size(); i++) {
        String name = id.names.get(i);
        final RelDataTypeField field;
        if (name.equals("")) {
          // The wildcard "*" is represented as an empty name. It never
          // resolves to a field.
          name = "*";
          field = null;
        } else {
          final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
          field = nameMatcher.field(type, name);
        }
        if (field == null) {
          throw newValidationError(id.getComponent(i),
              RESOURCE.unknownField(name));
        }
        type = field.getType();
      }
      type =
          SqlTypeUtil.addCharsetAndCollation(
              type,
              getTypeFactory());
      return type;
    }

    public RelDataType visit(SqlDataTypeSpec dataType) {
      // Q. How can a data type have a type?
      // A. When it appears in an expression. (Say as the 2nd arg to the
      //    CAST operator.)
      validateDataType(dataType);
      return dataType.deriveType(SqlValidatorImpl.this);
    }

    public RelDataType visit(SqlDynamicParam param) {
      return unknownType;
    }

    public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
      return typeFactory.createSqlIntervalType(intervalQualifier);
    }
  }

  /**
   * Converts an expression into canonical form by fully-qualifying any
   * identifiers.
   */
  private static class Expander extends SqlScopedShuttle {
    protected final SqlValidatorImpl validator;

    Expander(SqlValidatorImpl validator, SqlValidatorScope scope) {
      super(scope);
      this.validator = validator;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have
      // parentheses, like "LOCALTIME".
      SqlCall call =
          SqlUtil.makeCall(
              validator.getOperatorTable(),
              id);
      if (call != null) {
        return call.accept(this);
      }
      final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
      SqlNode expandedExpr = fqId;
      // Convert a column ref into ITEM(*, 'col_name').
      // select col_name from (select * from dynTable)
      // SqlIdentifier "col_name" would be resolved to a dynamic star field in dynTable's rowType.
      // Expand such SqlIdentifier to ITEM operator.
      if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
          && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
        SqlNode[] inputs = new SqlNode[2];
        inputs[0] = fqId;
        inputs[1] = SqlLiteral.createCharString(
          Util.last(id.names),
          id.getParserPosition());
        SqlBasicCall item_call = new SqlBasicCall(
          SqlStdOperatorTable.ITEM,
          inputs,
          id.getParserPosition());
        expandedExpr = item_call;
      }
      validator.setOriginal(expandedExpr, id);
      return expandedExpr;
    }

    @Override protected SqlNode visitScoped(SqlCall call) {
      switch (call.getKind()) {
      case SCALAR_QUERY:
      case CURRENT_VALUE:
      case NEXT_VALUE:
      case WITH:
        return call;
      }
      // Only visits arguments which are expressions. We don't want to
      // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, false);
      call.getOperator().acceptCall(this, call, true, argHandler);
      final SqlNode result = argHandler.result();
      validator.setOriginal(result, call);
      return result;
    }
  }

  /**
   * Shuttle which walks over an expression in the ORDER BY clause, replacing
   * usages of aliases with the underlying expression.
   */
  class OrderExpressionExpander extends SqlScopedShuttle {
    private final List<String> aliasList;
    private final SqlSelect select;
    private final SqlNode root;

    OrderExpressionExpander(SqlSelect select, SqlNode root) {
      super(getOrderScope(select));
      this.select = select;
      this.root = root;
      this.aliasList = getNamespace(select).getRowType().getFieldNames();
    }

    public SqlNode go() {
      return root.accept(this);
    }

    public SqlNode visit(SqlLiteral literal) {
      // Ordinal markers, e.g. 'select a, b from t order by 2'.
      // Only recognize them if they are the whole expression,
      // and if the dialect permits.
      if (literal == root && getConformance().isSortByOrdinal()) {
        switch (literal.getTypeName()) {
        case DECIMAL:
        case DOUBLE:
          final int intValue = literal.intValue(false);
          if (intValue >= 0) {
            if (intValue < 1 || intValue > aliasList.size()) {
              throw newValidationError(
                  literal, RESOURCE.orderByOrdinalOutOfRange());
            }

            // SQL ordinals are 1-based, but Sort's are 0-based
            int ordinal = intValue - 1;
            return nthSelectItem(ordinal, literal.getParserPosition());
          }
          break;
        }
      }

      return super.visit(literal);
    }

    /**
     * Returns the <code>ordinal</code>th item in the select list.
     */
    private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos) {
      // TODO: Don't expand the list every time. Maybe keep an expanded
      // version of each expression -- select lists and identifiers -- in
      // the validator.

      SqlNodeList expandedSelectList =
          expandStar(
              select.getSelectList(),
              select,
              false);
      SqlNode expr = expandedSelectList.get(ordinal);
      expr = stripAs(expr);
      if (expr instanceof SqlIdentifier) {
        expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
      }

      // Create a copy of the expression with the position of the order
      // item.
      return expr.clone(pos);
    }

    public SqlNode visit(SqlIdentifier id) {
      // Aliases, e.g. 'select a as x, b from t order by x'.
      if (id.isSimple()
          && getConformance().isSortByAlias()) {
        String alias = id.getSimple();
        final SqlValidatorNamespace selectNs = getNamespace(select);
        final RelDataType rowType =
            selectNs.getRowTypeSansSystemColumns();
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        RelDataTypeField field = nameMatcher.field(rowType, alias);
        if (field != null) {
          return nthSelectItem(
              field.getIndex(),
              id.getParserPosition());
        }
      }

      // No match. Return identifier unchanged.
      return getScope().fullyQualify(id).identifier;
    }

    protected SqlNode visitScoped(SqlCall call) {
      // Don't attempt to expand sub-queries. We haven't implemented
      // these yet.
      if (call instanceof SqlSelect) {
        return call;
      }
      return super.visitScoped(call);
    }
  }

  /**
   * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing
   * usages of aliases or ordinals with the underlying expression.
   */
  static class ExtendedExpander extends Expander {
    final SqlSelect select;
    final SqlNode root;
    final boolean havingExpr;

    ExtendedExpander(SqlValidatorImpl validator, SqlValidatorScope scope,
        SqlSelect select, SqlNode root, boolean havingExpr) {
      super(validator, scope);
      this.select = select;
      this.root = root;
      this.havingExpr = havingExpr;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (id.isSimple()
          && (havingExpr
              ? validator.getConformance().isHavingAlias()
              : validator.getConformance().isGroupByAlias())) {
        String name = id.getSimple();
        SqlNode expr = null;
        final SqlNameMatcher nameMatcher =
            validator.catalogReader.nameMatcher();
        int n = 0;
        for (SqlNode s : select.getSelectList()) {
          final String alias = SqlValidatorUtil.getAlias(s, -1);
          if (alias != null && nameMatcher.matches(alias, name)) {
            expr = s;
            n++;
          }
        }
        if (n == 0) {
          return super.visit(id);
        } else if (n > 1) {
          // More than one column has this alias.
          throw validator.newValidationError(id,
              RESOURCE.columnAmbiguous(name));
        }
        if (havingExpr && validator.isAggregate(root)) {
          return super.visit(id);
        }
        expr = stripAs(expr);
        if (expr instanceof SqlIdentifier) {
          expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
        }
        return expr;
      }
      return super.visit(id);
    }

    public SqlNode visit(SqlLiteral literal) {
      if (havingExpr || !validator.getConformance().isGroupByOrdinal()) {
        return super.visit(literal);
      }
      boolean isOrdinalLiteral = literal == root;
      switch (root.getKind()) {
      case GROUPING_SETS:
      case ROLLUP:
      case CUBE:
        if (root instanceof SqlBasicCall) {
          List<SqlNode> operandList = ((SqlBasicCall) root).getOperandList();
          for (SqlNode node : operandList) {
            if (node.equals(literal)) {
              isOrdinalLiteral = true;
              break;
            }
          }
        }
        break;
      }
      if (isOrdinalLiteral) {
        switch (literal.getTypeName()) {
        case DECIMAL:
        case DOUBLE:
          final int intValue = literal.intValue(false);
          if (intValue >= 0) {
            if (intValue < 1 || intValue > select.getSelectList().size()) {
              throw validator.newValidationError(literal,
                  RESOURCE.orderByOrdinalOutOfRange());
            }

            // SQL ordinals are 1-based, but Sort's are 0-based
            int ordinal = intValue - 1;
            return select.getSelectList().get(ordinal);
          }
          break;
        }
      }

      return super.visit(literal);
    }
  }

  /** Information about an identifier in a particular scope. */
  protected static class IdInfo {
    public final SqlValidatorScope scope;
    public final SqlIdentifier id;

    public IdInfo(SqlValidatorScope scope, SqlIdentifier id) {
      this.scope = scope;
      this.id = id;
    }
  }

  /**
   * Utility object used to maintain information about the parameters in a
   * function call.
   */
  protected static class FunctionParamInfo {
    /**
     * Maps a cursor (based on its position relative to other cursor
     * parameters within a function call) to the SELECT associated with the
     * cursor.
     */
    public final Map<Integer, SqlSelect> cursorPosToSelectMap;

    /**
     * Maps a column list parameter to the parent cursor parameter it
     * references. The parameters are id'd by their names.
     */
    public final Map<String, String> columnListParamToParentCursorMap;

    public FunctionParamInfo() {
      cursorPosToSelectMap = new HashMap<>();
      columnListParamToParentCursorMap = new HashMap<>();
    }
  }

  /**
   * Modify the nodes in navigation function
   * such as FIRST, LAST, PREV AND NEXT.
   */
  private class NavigationModifier extends SqlBasicVisitor<SqlNode> {
    @Override public SqlNode visit(SqlLiteral literal) {
      return literal;
    }

    @Override public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return intervalQualifier;
    }

    @Override public SqlNode visit(SqlDataTypeSpec type) {
      return type;
    }

    @Override public SqlNode visit(SqlDynamicParam param) {
      return param;
    }

    public SqlNode go(SqlNode node) {
      return node.accept(this);
    }
  }

  /**
   * Expand navigation expression :
   * eg: PREV(A.price + A.amount) to PREV(A.price) + PREV(A.amount)
   * eg: FIRST(A.price * 2) to FIST(A.PRICE) * 2
   */
  private class NavigationExpander extends NavigationModifier {
    SqlOperator currentOperator;
    SqlNode currentOffset;

    public NavigationExpander() {

    }

    public NavigationExpander(SqlOperator operator, SqlNode offset) {
      this.currentOffset = offset;
      this.currentOperator = operator;
    }

    @Override public SqlNode visit(SqlCall call) {
      SqlKind kind = call.getKind();
      List<SqlNode> operands = call.getOperandList();
      List<SqlNode> newOperands = new ArrayList<>();
      if (isLogicalNavigation(kind) || isPhysicalNavigation(kind)) {
        SqlNode inner = operands.get(0);
        SqlNode offset = operands.get(1);

        // merge two straight prev/next, update offset
        if (isPhysicalNavigation(kind)) {
          SqlKind innerKind = inner.getKind();
          if (isPhysicalNavigation(innerKind)) {
            List<SqlNode> innerOperands = ((SqlCall) inner).getOperandList();
            SqlNode innerOffset = innerOperands.get(1);
            SqlOperator newOperator = innerKind == kind
              ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS;
            offset = newOperator.createCall(SqlParserPos.ZERO,
              offset, innerOffset);
            inner = call.getOperator().createCall(SqlParserPos.ZERO,
              innerOperands.get(0), offset);
          }
        }
        return inner.accept(new NavigationExpander(call.getOperator(), offset));
      }

      for (SqlNode node : operands) {
        SqlNode newNode = node.accept(new NavigationExpander());
        if (currentOperator != null) {
          newNode = currentOperator.createCall(SqlParserPos.ZERO, newNode, currentOffset);
        }
        newOperands.add(newNode);
      }
      return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (currentOperator == null) {
        return id;
      } else {
        return currentOperator.createCall(SqlParserPos.ZERO, id, currentOffset);
      }
    }
  }

  /**
   * Replace {@code A as A.price > PREV(B.price)}
   * with {@code PREV(A.price, 0) > last(B.price, 0)}.
   */
  private class NavigationReplacer extends NavigationModifier {
    private final String alpha;

    public NavigationReplacer(String alpha) {
      this.alpha = alpha;
    }

    @Override public SqlNode visit(SqlCall call) {
      SqlKind kind = call.getKind();
      if (isLogicalNavigation(kind)
          || isAggregation(kind)
          || isRunningOrFinal(kind)) {
        return call;
      }

      List<SqlNode> operands = call.getOperandList();
      switch (kind) {
      case PREV:
        String name = ((SqlIdentifier) operands.get(0)).names.get(0);
        return name.equals(alpha) ? call
          : SqlStdOperatorTable.LAST.createCall(SqlParserPos.ZERO, operands);
      default:
        List<SqlNode> newOperands = new ArrayList<>();
        for (SqlNode op : operands) {
          newOperands.add(op.accept(this));
        }
        return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
      }
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (id.isSimple()) {
        return id;
      }
      SqlOperator operator = id.names.get(0).equals(alpha)
        ? SqlStdOperatorTable.PREV : SqlStdOperatorTable.LAST;

      return operator.createCall(SqlParserPos.ZERO, id,
        SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
    }
  }

  /**
   * Within one navigation function, the pattern var should be same
   */
  private class PatternValidator extends SqlBasicVisitor<Set<String>> {
    private final boolean isMeasure;
    int firstLastCount;
    int prevNextCount;
    int aggregateCount;

    PatternValidator(boolean isMeasure) {
      this(isMeasure, 0, 0, 0);
    }

    PatternValidator(boolean isMeasure, int firstLastCount, int prevNextCount,
        int aggregateCount) {
      this.isMeasure = isMeasure;
      this.firstLastCount = firstLastCount;
      this.prevNextCount = prevNextCount;
      this.aggregateCount = aggregateCount;
    }

    @Override public Set<String> visit(SqlCall call) {
      boolean isSingle = false;
      Set<String> vars = new HashSet<>();
      SqlKind kind = call.getKind();
      List<SqlNode> operands = call.getOperandList();

      if (isSingleVarRequired(kind)) {
        isSingle = true;
        if (isPhysicalNavigation(kind)) {
          if (isMeasure) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionInMeasure(call.toString()));
          }
          if (firstLastCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
          }
          prevNextCount++;
        } else if (isLogicalNavigation(kind)) {
          if (firstLastCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
          }
          firstLastCount++;
        } else if (isAggregation(kind)) {
          // cannot apply aggregation in PREV/NEXT, FIRST/LAST
          if (firstLastCount != 0 || prevNextCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternAggregationInNavigation(call.toString()));
          }
          if (kind == SqlKind.COUNT && call.getOperandList().size() > 1) {
            throw newValidationError(call,
                Static.RESOURCE.patternCountFunctionArg());
          }
          aggregateCount++;
        }
      }

      if (isRunningOrFinal(kind) && !isMeasure) {
        throw newValidationError(call,
            Static.RESOURCE.patternRunningFunctionInDefine(call.toString()));
      }

      for (SqlNode node : operands) {
        vars.addAll(
            node.accept(
                new PatternValidator(isMeasure, firstLastCount, prevNextCount,
                    aggregateCount)));
      }

      if (isSingle) {
        switch (kind) {
        case COUNT:
          if (vars.size() > 1) {
            throw newValidationError(call,
                Static.RESOURCE.patternCountFunctionArg());
          }
          break;
        default:
          if (vars.isEmpty()) {
            throw newValidationError(call,
              Static.RESOURCE.patternFunctionNullCheck(call.toString()));
          }
          if (vars.size() != 1) {
            throw newValidationError(call,
                Static.RESOURCE.patternFunctionVariableCheck(call.toString()));
          }
          break;
        }
      }
      return vars;
    }

    @Override public Set<String> visit(SqlIdentifier identifier) {
      boolean check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0;
      Set<String> vars = new HashSet<>();
      if (identifier.names.size() > 1 && check) {
        vars.add(identifier.names.get(0));
      }
      return vars;
    }

    @Override public Set<String> visit(SqlLiteral literal) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlIntervalQualifier qualifier) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlDataTypeSpec type) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlDynamicParam param) {
      return ImmutableSet.of();
    }
  }

  //~ Enums ------------------------------------------------------------------

  /**
   * Validation status.
   */
  public enum Status {
    /**
     * Validation has not started for this scope.
     */
    UNVALIDATED,

    /**
     * Validation is in progress for this scope.
     */
    IN_PROGRESS,

    /**
     * Validation has completed (perhaps unsuccessfully).
     */
    VALID
  }

}

// End SqlValidatorImpl.java
