/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.validate;

import java.math.*;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;

import net.hydromatic.linq4j.Linq4j;


/**
 * Default implementation of {@link SqlValidator}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 3, 2005
 */
public class SqlValidatorImpl
    implements SqlValidatorWithHints
{
    //~ Static fields/initializers ---------------------------------------------

    public static final Logger tracer = EigenbaseTrace.parserTracer;

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

    //~ Enums ------------------------------------------------------------------

    /**
     * Validation status.
     */
    public enum Status
    {
        /**
         * Validation has not started for this scope.
         */
        Unvalidated,

        /**
         * Validation is in progress for this scope.
         */
        InProgress,

        /**
         * Validation has completed (perhaps unsuccessfully).
         */
        Valid
    }

    //~ Instance fields --------------------------------------------------------

    private final SqlOperatorTable opTab;
    final SqlValidatorCatalogReader catalogReader;

    /**
     * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
     * objects at these positions
     */
    protected final Map<String, IdInfo> idPositions =
        new HashMap<String, IdInfo>();

    /**
     * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
     * scope created from them}.
     */
    protected final Map<SqlNode, SqlValidatorScope> scopes =
        new IdentityHashMap<SqlNode, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
     * clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> whereScopes =
        new IdentityHashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
     * clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> selectScopes =
        new IdentityHashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
     */
    private final Map<SqlSelect, SqlValidatorScope> orderScopes =
        new IdentityHashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node that is the argument to a CURSOR
     * constructor to the scope of the result of that select node
     */
    private final Map<SqlSelect, SqlValidatorScope> cursorScopes =
        new IdentityHashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlNode node} to the {@link SqlValidatorNamespace
     * namespace} which describes what columns they contain.
     */
    protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
        new IdentityHashMap<SqlNode, SqlValidatorNamespace>();

    /**
     * Set of select expressions used as cursor definitions. In standard SQL,
     * only the top-level SELECT is a cursor; Eigenbase extends this with
     * cursors as inputs to table functions.
     */
    private final Set<SqlNode> cursorSet = new IdentityHashSet<SqlNode>();

    /**
     * Stack of objects that maintain information about function calls. A stack
     * is needed to handle nested function calls. The function call currently
     * being validated is at the top of the stack.
     */
    protected final Stack<FunctionParamInfo> functionCallStack =
        new Stack<FunctionParamInfo>();

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
        new IdentityHashMap<SqlNode, RelDataType>();
    private final AggFinder aggFinder = new AggFinder(false);
    private final AggFinder aggOrOverFinder = new AggFinder(true);
    private final SqlConformance conformance;
    private final Map<SqlNode, SqlNode> originalExprs =
        new HashMap<SqlNode, SqlNode>();

    // REVIEW jvs 30-June-2006: subclasses may override shouldExpandIdentifiers
    // in a way that ignores this; we should probably get rid of the protected
    // method and always use this variable (or better, move preferences like
    // this to a separate "parameter" class)
    protected boolean expandIdentifiers;

    protected boolean expandColumnReferences;

    private boolean rewriteCalls;

    // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
    // if it's OK to expand the signature of that method.
    private boolean validatingSqlMerge;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param conformance Compatibility mode
     */
    protected SqlValidatorImpl(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance)
    {
        Linq4j.requireNonNull(opTab);
        Linq4j.requireNonNull(catalogReader);
        Linq4j.requireNonNull(typeFactory);
        Linq4j.requireNonNull(conformance);
        this.opTab = opTab;
        this.catalogReader = catalogReader;
        this.typeFactory = typeFactory;
        this.conformance = conformance;

        // NOTE jvs 23-Dec-2003:  This is used as the type for dynamic
        // parameters and null literals until a real type is imposed for them.
        unknownType = typeFactory.createSqlType(SqlTypeName.NULL);
        booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        rewriteCalls = true;
        expandColumnReferences = true;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlConformance getConformance()
    {
        return conformance;
    }

    public SqlValidatorCatalogReader getCatalogReader()
    {
        return catalogReader;
    }

    public SqlOperatorTable getOperatorTable()
    {
        return opTab;
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    public RelDataType getUnknownType()
    {
        return unknownType;
    }

    public SqlNodeList expandStar(
        SqlNodeList selectList,
        SqlSelect select,
        boolean includeSystemVars)
    {
        List<SqlNode> list = new ArrayList<SqlNode>();
        List<Map.Entry<String, RelDataType>> types =
            new ArrayList<Map.Entry<String, RelDataType>>();
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode selectItem = selectList.get(i);
            expandSelectItem(
                selectItem,
                select,
                list,
                new LinkedHashSet<String>(),
                types,
                includeSystemVars);
        }
        getRawSelectScope(select).setExpandedSelectList(list);
        return new SqlNodeList(list, SqlParserPos.ZERO);
    }

    // implement SqlValidator
    public void declareCursor(SqlSelect select, SqlValidatorScope parentScope)
    {
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
    public void pushFunctionCall()
    {
        FunctionParamInfo funcInfo = new FunctionParamInfo();
        functionCallStack.push(funcInfo);
    }

    // implement SqlValidator
    public void popFunctionCall()
    {
        functionCallStack.pop();
    }

    // implement SqlValidator
    public String getParentCursor(String columnListParamName)
    {
        FunctionParamInfo funcParamInfo = functionCallStack.peek();
        Map<String, String> parentCursorMap =
            funcParamInfo.columnListParamToParentCursorMap;
        return parentCursorMap.get(columnListParamName);
    }

    /**
     * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
     * true; otherwise writes the unexpanded item.
     *
     * @param selectItem Select-list item
     * @param select Containing select clause
     * @param selectItems List that expanded items are written to
     * @param aliases Set of aliases
     * @param types List of data types in alias order
     * @param includeSystemVars If true include system vars in lists
     *
     * @return Whether the node was expanded
     */
    private boolean expandSelectItem(
        final SqlNode selectItem,
        SqlSelect select,
        List<SqlNode> selectItems,
        Set<String> aliases,
        List<Map.Entry<String, RelDataType>> types,
        final boolean includeSystemVars)
    {
        final SelectScope scope = (SelectScope) getWhereScope(select);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if ((identifier.names.length == 1)
                && identifier.names[0].equals("*"))
            {
                SqlParserPos starPosition = identifier.getParserPosition();
                for (Pair<String, SqlValidatorNamespace> p : scope.children) {
                    final SqlNode from = p.right.getNode();
                    final SqlValidatorNamespace fromNs = getNamespace(from);
                    assert fromNs != null;
                    final RelDataType rowType = fromNs.getRowType();
                    for (RelDataTypeField field : rowType.getFields()) {
                        String columnName = field.getName();

                        // TODO: do real implicit collation here
                        final SqlNode exp =
                            new SqlIdentifier(
                                new String[] { p.left, columnName },
                                starPosition);
                        addToSelectList(
                            selectItems,
                            aliases,
                            types,
                            exp,
                            scope,
                            includeSystemVars);
                    }
                }
                return true;
            } else if (
                (identifier.names.length == 2)
                && identifier.names[1].equals("*"))
            {
                final String tableName = identifier.names[0];
                SqlParserPos starPosition = identifier.getParserPosition();
                final SqlValidatorNamespace childNs = scope.getChild(tableName);
                if (childNs == null) {
                    // e.g. "select r.* from e"
                    throw newValidationError(
                        identifier.getComponent(0),
                        EigenbaseResource.instance().UnknownIdentifier.ex(
                            tableName));
                }
                final SqlNode from = childNs.getNode();
                final SqlValidatorNamespace fromNs = getNamespace(from);
                assert fromNs != null;
                final RelDataType rowType = fromNs.getRowType();
                for (RelDataTypeField field : rowType.getFields()) {
                    String columnName = field.getName();

                    // TODO: do real implicit collation here
                    final SqlIdentifier exp =
                        new SqlIdentifier(
                            new String[] { tableName, columnName },
                            starPosition);
                    addToSelectList(
                        selectItems,
                        aliases,
                        types,
                        exp,
                        scope,
                        includeSystemVars);
                }
                return true;
            }
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
        if (expanded != selectItem) {
            String newAlias =
                deriveAlias(
                    expanded,
                    aliases.size());
            if (!newAlias.equals(alias)) {
                expanded =
                    SqlStdOperatorTable.asOperator.createCall(
                        selectItem.getParserPosition(),
                        expanded,
                        new SqlIdentifier(alias, SqlParserPos.ZERO));
                deriveTypeImpl(scope, expanded);
            }
        }

        selectItems.add(expanded);
        aliases.add(alias);

        final RelDataType type = deriveType(scope, selectItem);
        setValidatedNodeTypeImpl(selectItem, type);
        types.add(Pair.of(alias, type));
        return false;
    }

    public SqlNode validate(SqlNode topNode)
    {
        SqlValidatorScope scope = new EmptyScope(this);
        final SqlNode topNode2 = validateScopedExpression(topNode, scope);
        final RelDataType type = getValidatedNodeType(topNode2);
        Util.discard(type);
        return topNode2;
    }

    public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos)
    {
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
            throw Util.newInternal("Not a query: " + outermostNode);
        }
        List<SqlMoniker> hintList = new ArrayList<SqlMoniker>();
        lookupSelectHints(ns, pos, hintList);
        return hintList;
    }

    public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos)
    {
        final String posString = pos.toString();
        IdInfo info = idPositions.get(posString);
        if (info != null) {
            return new SqlIdentifierMoniker(info.scope.fullyQualify(info.id));
        } else {
            return null;
        }
    }

    /**
     * Looks up completion hints for a syntactically correct select SQL that has
     * been parsed into an expression tree.
     *
     * @param select the Select node of the parsed expression tree
     * @param pos indicates the position in the sql statement we want to get
     * completion hints for
     * @param hintList list of {@link SqlMoniker} (sql identifiers) that can
     * fill in at the indicated position
     */
    void lookupSelectHints(
        SqlSelect select,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        IdInfo info = idPositions.get(pos.toString());
        if ((info == null) || (info.scope == null)) {
            SqlNode fromNode = select.getFrom();
            final SqlValidatorScope fromScope = getFromScope(select);
            lookupFromHints(fromNode, fromScope, pos, hintList);
        } else {
            lookupNameCompletionHints(
                info.scope,
                Arrays.asList(info.id.names),
                info.id.getParserPosition(),
                hintList);
        }
    }

    private void lookupSelectHints(
        SqlValidatorNamespace ns,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        final SqlNode node = ns.getNode();
        if (node instanceof SqlSelect) {
            lookupSelectHints((SqlSelect) node, pos, hintList);
        }
    }

    private void lookupFromHints(
        SqlNode node,
        SqlValidatorScope scope,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns.isWrapperFor(IdentifierNamespace.class)) {
            IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
            final SqlIdentifier id = idNs.getId();
            for (int i = 0; i < id.names.length; i++) {
                if (pos.toString().equals(
                        id.getComponent(i).getParserPosition().toString()))
                {
                    List<SqlMoniker> objNames = new ArrayList<SqlMoniker>();
                    SqlValidatorUtil.getSchemaObjectMonikers(
                        getCatalogReader(),
                        Arrays.asList(id.names).subList(0, i + 1),
                        objNames);
                    for (SqlMoniker objName : objNames) {
                        if (objName.getType() != SqlMonikerType.Function) {
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
        List<SqlMoniker> hintList)
    {
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
        SqlJoinOperator.ConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = scopes.get(join);
        switch (conditionType) {
        case On:
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
     * @param scope Validation scope
     * @param names Components of the identifier
     * @param pos position
     * @param hintList a list of valid options
     */
    public final void lookupNameCompletionHints(
        SqlValidatorScope scope,
        List<String> names,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        // Remove the last part of name - it is a dummy
        List<String> subNames = names.subList(0, names.size() - 1);

        if (subNames.size() > 0) {
            // If there's a prefix, resolve it to a namespace.
            SqlValidatorNamespace ns = null;
            for (String name : subNames) {
                if (ns == null) {
                    ns = scope.resolve(name, null, null);
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
                            SqlMonikerType.Column));
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
                && (selectScope.getChildren().size() == 1))
            {
                RelDataType rowType =
                    selectScope.getChildren().get(0).getRowType();
                for (RelDataTypeField field : rowType.getFieldList()) {
                    hintList.add(
                        new SqlMonikerImpl(
                            field.getName(),
                            SqlMonikerType.Column));
                }
            }
        }

        findAllValidUdfNames(names, this, hintList);
    }

    private static void findAllValidUdfNames(
        List<String> names,
        SqlValidator validator,
        List<SqlMoniker> result)
    {
        List<SqlMoniker> objNames = new ArrayList<SqlMoniker>();
        SqlValidatorUtil.getSchemaObjectMonikers(
            validator.getCatalogReader(),
            names,
            objNames);
        for (SqlMoniker objName : objNames) {
            if (objName.getType() == SqlMonikerType.Function) {
                result.add(objName);
            }
        }
    }

    private static void findAllValidFunctionNames(
        List<String> names,
        SqlValidator validator,
        List<SqlMoniker> result,
        SqlParserPos pos)
    {
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
                        SqlMonikerType.Function));
            } else {
                if ((op.getSyntax() == SqlSyntax.Function)
                    || (op.getSyntax() == SqlSyntax.Prefix))
                {
                    if (op.getOperandTypeChecker() != null) {
                        String sig = op.getAllowedSignatures();
                        sig = sig.replaceAll("'", "");
                        result.add(
                            new SqlMonikerImpl(
                                sig,
                                SqlMonikerType.Function));
                        continue;
                    }
                    result.add(
                        new SqlMonikerImpl(
                            op.getName(),
                            SqlMonikerType.Function));
                }
            }
        }
    }

    public SqlNode validateParameterizedExpression(
        SqlNode topNode,
        final Map<String, RelDataType> nameToTypeMap)
    {
        SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
        return validateScopedExpression(topNode, scope);
    }

    private SqlNode validateScopedExpression(
        SqlNode topNode,
        SqlValidatorScope scope)
    {
        SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
        cursorSet.add(outermostNode);
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer(
                "After unconditional rewrite: "
                + outermostNode.toString());
        }
        if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
            registerQuery(
                scope,
                null,
                outermostNode,
                outermostNode,
                null,
                false);
        }
        outermostNode.validate(this, scope);
        if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
            // force type derivation so that we can provide it to the
            // caller later without needing the scope
            deriveType(scope, outermostNode);
        }
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer("After validation: " + outermostNode.toString());
        }
        return outermostNode;
    }

    public void validateQuery(SqlNode node, SqlValidatorScope scope)
    {
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns == null) {
            throw Util.newInternal("Not a query: " + node);
        }

        if (node.getKind() == SqlKind.TABLESAMPLE) {
            SqlNode [] operands = ((SqlCall) node).operands;
            SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands[1]);
            if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
                validateFeature(
                    EigenbaseResource.instance().SQLFeature_T613,
                    node.getParserPosition());
            } else if (
                sampleSpec
                instanceof SqlSampleSpec.SqlSubstitutionSampleSpec)
            {
                validateFeature(
                    EigenbaseResource.instance()
                    .SQLFeatureExt_T613_Substitution,
                    node.getParserPosition());
            }
        }

        validateNamespace(ns);
        validateAccess(
            node,
            ns.getTable(),
            SqlAccessEnum.SELECT);
    }

    /**
     * Validates a namespace.
     */
    protected void validateNamespace(final SqlValidatorNamespace namespace)
    {
        namespace.validate();
        setValidatedNodeType(
            namespace.getNode(),
            namespace.getRowType());
    }

    public SqlValidatorScope getCursorScope(SqlSelect select)
    {
        return cursorScopes.get(select);
    }

    public SqlValidatorScope getWhereScope(SqlSelect select)
    {
        return whereScopes.get(select);
    }

    public SqlValidatorScope getSelectScope(SqlSelect select)
    {
        return selectScopes.get(select);
    }

    public SelectScope getRawSelectScope(SqlSelect select)
    {
        SqlValidatorScope scope = getSelectScope(select);
        if (scope instanceof AggregatingSelectScope) {
            scope = ((AggregatingSelectScope) scope).getParent();
        }
        return (SelectScope) scope;
    }

    public SqlValidatorScope getHavingScope(SqlSelect select)
    {
        // Yes, it's the same as getSelectScope
        return selectScopes.get(select);
    }

    public SqlValidatorScope getGroupScope(SqlSelect select)
    {
        // Yes, it's the same as getWhereScope
        return whereScopes.get(select);
    }

    public SqlValidatorScope getFromScope(SqlSelect select)
    {
        return scopes.get(select);
    }

    public SqlValidatorScope getOrderScope(SqlSelect select)
    {
        return orderScopes.get(select);
    }

    public SqlValidatorScope getJoinScope(SqlNode node)
    {
        switch (node.getKind()) {
        case AS:
            return getJoinScope(((SqlCall) node).operands[0]);
        default:
            return scopes.get(node);
        }
    }

    public SqlValidatorScope getOverScope(SqlNode node)
    {
        return scopes.get(node);
    }

    /**
     * Returns the appropriate scope for validating a particular clause of a
     * SELECT statement.
     *
     * <p>Consider SELECT * FROM foo WHERE EXISTS ( SELECT deptno AS x FROM emp,
     * dept WHERE emp.deptno = 5 GROUP BY deptno ORDER BY x) In FROM, you can
     * only see 'foo'. In WHERE, GROUP BY and SELECT, you can see 'emp', 'dept',
     * and 'foo'. In ORDER BY, you can see the column alias 'x', 'emp', 'dept',
     * and 'foo'.
     */
    public SqlValidatorScope getScope(SqlSelect select, int operandType)
    {
        switch (operandType) {
        case SqlSelect.FROM_OPERAND:
            return scopes.get(select);
        case SqlSelect.WHERE_OPERAND:
        case SqlSelect.GROUP_OPERAND:
            return whereScopes.get(select);
        case SqlSelect.HAVING_OPERAND:
        case SqlSelect.SELECT_OPERAND:
            return selectScopes.get(select);
        case SqlSelect.ORDER_OPERAND:
            return orderScopes.get(select);
        default:
            throw Util.newInternal("Unexpected operandType " + operandType);
        }
    }

    public SqlValidatorNamespace getNamespace(SqlNode node)
    {
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
            return getNamespace(((SqlCall) node).operands[0]);
        default:
            return namespaces.get(node);
        }
    }

    /**
     * Performs expression rewrites which are always used unconditionally. These
     * rewrites massage the expression tree into a standard form so that the
     * rest of the validation logic can be simpler.
     *
     * @param node expression to be rewritten
     * @param underFrom whether node appears directly under a FROM clause
     *
     * @return rewritten expression
     */
    protected SqlNode performUnconditionalRewrites(
        SqlNode node,
        boolean underFrom)
    {
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
            final SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                boolean childUnderFrom;
                if (kind == SqlKind.SELECT) {
                    childUnderFrom = (i == SqlSelect.FROM_OPERAND);
                } else if (kind == SqlKind.AS && (i == 0)) {
                    // for an aliased expression, it is under FROM if
                    // the AS expression is under FROM
                    childUnderFrom = underFrom;
                } else {
                    childUnderFrom = false;
                }
                newOperand =
                    performUnconditionalRewrites(operand, childUnderFrom);
                if (newOperand != null) {
                    call.setOperand(i, newOperand);
                }
            }

            if (call.getOperator() instanceof SqlFunction) {
                SqlFunction function = (SqlFunction) call.getOperator();
                if (function.getFunctionType() == null) {
                    // This function hasn't been resolved yet.  Perform
                    // a half-hearted resolution now in case it's a
                    // builtin function requiring special casing.  If it's
                    // not, we'll handle it later during overload
                    // resolution.
                    List<SqlOperator> overloads =
                        opTab.lookupOperatorOverloads(
                            function.getNameAsId(),
                            null,
                            SqlSyntax.Function);
                    if (overloads.size() == 1) {
                        call.setOperator(overloads.get(0));
                    }
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
            if (underFrom || true) {
                // leave FROM (VALUES(...)) [ AS alias ] clauses alone,
                // otherwise they grow cancerously if this rewrite is invoked
                // over and over
                return node;
            } else {
                final SqlNodeList selectList =
                    new SqlNodeList(SqlParserPos.ZERO);
                selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
                return SqlStdOperatorTable.selectOperator.createCall(
                    null,
                    selectList,
                    node,
                    null,
                    null,
                    null,
                    null,
                    null,
                    node.getParserPosition());
            }

        case ORDER_BY:
        {
            SqlCall orderBy = (SqlCall) node;
            SqlNode query =
                orderBy.getOperands()[SqlOrderByOperator.QUERY_OPERAND];
            SqlNodeList orderList =
                (SqlNodeList)
                orderBy.getOperands()[SqlOrderByOperator.ORDER_OPERAND];
            if (query instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) query;

                // Don't clobber existing ORDER BY.  It may be needed for
                // an order-sensitive function like RANK.
                if (select.getOrderList() == null) {
                    // push ORDER BY into existing select
                    select.setOperand(SqlSelect.ORDER_OPERAND, orderList);
                    return select;
                }
            }
            final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            return SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                query,
                null,
                null,
                null,
                null,
                orderList,
                SqlParserPos.ZERO);
        }

        case EXPLICIT_TABLE:
        {
            // (TABLE t) is equivalent to (SELECT * FROM t)
            SqlCall call = (SqlCall) node;
            final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            return SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                call.getOperands()[0],
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        }

        case DELETE:
        {
            SqlDelete call = (SqlDelete) node;
            SqlSelect select = createSourceSelectForDelete(call);
            call.setOperand(SqlDelete.SOURCE_SELECT_OPERAND, select);
            break;
        }

        case UPDATE:
        {
            SqlUpdate call = (SqlUpdate) node;
            SqlSelect select = createSourceSelectForUpdate(call);
            call.setOperand(SqlUpdate.SOURCE_SELECT_OPERAND, select);

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

        case MERGE:
        {
            SqlMerge call = (SqlMerge) node;
            rewriteMerge(call);
            break;
        }
        }
        return node;
    }

    private void rewriteMerge(SqlMerge call)
    {
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
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
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
        SqlJoinOperator.JoinType joinType =
            (insertCall == null) ? SqlJoinOperator.JoinType.Inner
            : SqlJoinOperator.JoinType.Left;
        SqlNode leftJoinTerm = (SqlNode) sourceTableRef.clone();
        SqlNode outerJoin =
            SqlStdOperatorTable.joinOperator.createCall(
                leftJoinTerm,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                SqlLiteral.createSymbol(joinType, SqlParserPos.ZERO),
                targetTable,
                SqlLiteral.createSymbol(
                    SqlJoinOperator.ConditionType.On,
                    SqlParserPos.ZERO),
                call.getCondition(),
                SqlParserPos.ZERO);
        SqlSelect select =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                outerJoin,
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        call.setOperand(SqlMerge.SOURCE_SELECT_OPERAND, select);

        // Source for the insert call is a select of the source table
        // reference with the select list being the value expressions;
        // note that the values clause has already been converted to a
        // select on the values row constructor; so we need to extract
        // that via the from clause on the select
        if (insertCall != null) {
            SqlSelect valuesSelect = (SqlSelect) insertCall.getSource();
            SqlCall valuesCall = (SqlCall) valuesSelect.getFrom();
            SqlCall rowCall = (SqlCall) valuesCall.getOperands()[0];
            selectList =
                new SqlNodeList(
                    Arrays.asList(rowCall.getOperands()),
                    SqlParserPos.ZERO);
            SqlNode insertSource = (SqlNode) sourceTableRef.clone();
            select =
                SqlStdOperatorTable.selectOperator.createCall(
                    null,
                    selectList,
                    insertSource,
                    null,
                    null,
                    null,
                    null,
                    null,
                    SqlParserPos.ZERO);
            insertCall.setOperand(SqlInsert.SOURCE_OPERAND, select);
        }
    }

    private SqlNode rewriteUpdateToMerge(
        SqlUpdate updateCall,
        SqlNode selfJoinSrcExpr)
    {
        // Make sure target has an alias.
        if (updateCall.getAlias() == null) {
            updateCall.setOperand(
                SqlUpdate.ALIAS_OPERAND,
                new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO));
        }
        SqlNode selfJoinTgtExpr =
            getSelfJoinExprForUpdate(
                updateCall.getTargetTable(),
                updateCall.getAlias().getSimple());
        assert (selfJoinTgtExpr != null);

        // Create join condition between source and target exprs,
        // creating a conjunction with the user-level WHERE
        // clause if one was supplied
        SqlNode condition = updateCall.getCondition();
        SqlNode selfJoinCond =
            SqlStdOperatorTable.equalsOperator.createCall(
                SqlParserPos.ZERO,
                selfJoinSrcExpr,
                selfJoinTgtExpr);
        if (condition == null) {
            condition = selfJoinCond;
        } else {
            condition =
                SqlStdOperatorTable.andOperator.createCall(
                    SqlParserPos.ZERO,
                    selfJoinCond,
                    condition);
        }
        SqlIdentifier target =
            (SqlIdentifier) updateCall.getTargetTable().clone(
                SqlParserPos.ZERO);

        // For the source, we need to anonymize the fields, so
        // that for a statement like UPDATE T SET I = I + 1,
        // there's no ambiguity for the "I" in "I + 1";
        // this is OK because the source and target have
        // identical values due to the self-join.
        // Note that we anonymize the source rather than the
        // target because downstream, the optimizer rules
        // don't want to see any projection on top of the target.
        IdentifierNamespace ns =
            new IdentifierNamespace(
                this,
                target,
                null);
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
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                source,
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
        SqlMerge mergeCall =
            new SqlMerge(
                SqlStdOperatorTable.mergeOperator,
                target,
                condition,
                source,
                updateCall,
                null,
                updateCall.getAlias(),
                updateCall.getParserPosition());
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
     * for unqualified references; if this is equal to {@value
     * #UPDATE_SRC_ALIAS}, then column references have been anonymized to
     * "SYS$ANONx", where x is the 1-based column number.
     *
     * @return expression for unique identifier, or null to prevent conversion
     */
    protected SqlNode getSelfJoinExprForUpdate(
        SqlIdentifier table,
        String alias)
    {
        return null;
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into an UPDATE
     * statement to be updated.
     *
     * @param call Call to the UPDATE operator
     *
     * @return select statement
     */
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate call)
    {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
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
        return SqlStdOperatorTable.selectOperator.createCall(
            null,
            selectList,
            sourceTable,
            call.getCondition(),
            null,
            null,
            null,
            null,
            SqlParserPos.ZERO);
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into a DELETE
     * statement to be deleted.
     *
     * @param call Call to the DELETE operator
     *
     * @return select statement
     */
    protected SqlSelect createSourceSelectForDelete(SqlDelete call)
    {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
        SqlNode sourceTable = call.getTargetTable();
        if (call.getAlias() != null) {
            sourceTable =
                SqlValidatorUtil.addAlias(
                    sourceTable,
                    call.getAlias().getSimple());
        }
        return SqlStdOperatorTable.selectOperator.createCall(
            null,
            selectList,
            sourceTable,
            call.getCondition(),
            null,
            null,
            null,
            null,
            SqlParserPos.ZERO);
    }

    /** Returns null if there is no common type. E.g. if the rows have a
     * different number of columns. */
    RelDataType getTableConstructorRowType(
        SqlCall values,
        SqlValidatorScope scope)
    {
        assert values.getOperands().length >= 1;
        List<RelDataType> rowTypes = new ArrayList<RelDataType>();
        for (int iRow = 0; iRow < values.getOperands().length; ++iRow) {
            final SqlNode operand = values.getOperands()[iRow];
            assert (operand.getKind() == SqlKind.ROW);
            SqlCall rowConstructor = (SqlCall) operand;

            // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
            // rows, need to infer aliases from there.
            SqlNode [] operands = rowConstructor.getOperands();
            final List<String> aliasList = new ArrayList<String>();
            final List<RelDataType> typeList = new ArrayList<RelDataType>();
            for (int iCol = 0; iCol < operands.length; ++iCol) {
                final String alias = deriveAlias(operands[iCol], iCol);
                aliasList.add(alias);
                final RelDataType type = deriveType(scope, operands[iCol]);
                typeList.add(type);
            }
            rowTypes.add(typeFactory.createStructType(typeList, aliasList));
        }
        if (values.getOperands().length == 1) {
            // TODO jvs 10-Oct-2005:  get rid of this workaround once
            // leastRestrictive can handle all cases
            return rowTypes.get(0);
        }
        return typeFactory.leastRestrictive(rowTypes);
    }

    public RelDataType getValidatedNodeType(SqlNode node)
    {
        RelDataType type = getValidatedNodeTypeIfKnown(node);
        if (type == null) {
            throw Util.needToImplement(node);
        } else {
            return type;
        }
    }

    public RelDataType getValidatedNodeTypeIfKnown(SqlNode node)
    {
        final RelDataType type = nodeToTypeMap.get(node);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns != null) {
            return ns.getRowType();
        }
        final SqlNode original = originalExprs.get(node);
        if (original != null) {
            return getValidatedNodeType(original);
        }
        return null;
    }

    public void setValidatedNodeType(
        SqlNode node,
        RelDataType type)
    {
        setValidatedNodeTypeImpl(node, type);
    }

    public void removeValidatedNodeType(SqlNode node)
    {
        nodeToTypeMap.remove(node);
    }

    void setValidatedNodeTypeImpl(SqlNode node, RelDataType type)
    {
        Util.pre(type != null, "type != null");
        Util.pre(node != null, "node != null");
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node, type);
    }

    public RelDataType deriveType(
        SqlValidatorScope scope,
        SqlNode expr)
    {
        Util.pre(scope != null, "scope != null");
        Util.pre(expr != null, "expr != null");

        // if we already know the type, no need to re-derive
        RelDataType type = nodeToTypeMap.get(expr);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(expr);
        if (ns != null) {
            return ns.getRowType();
        }
        type = deriveTypeImpl(scope, expr);
        Util.permAssert(
            type != null,
            "SqlValidator.deriveTypeInternal returned null");
        setValidatedNodeTypeImpl(expr, type);
        return type;
    }

    /**
     * Derives the type of a node.
     *
     * @post return != null
     */
    RelDataType deriveTypeImpl(
        SqlValidatorScope scope,
        SqlNode operand)
    {
        DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
        return operand.accept(v);
    }

    public RelDataType deriveConstructorType(
        SqlValidatorScope scope,
        SqlCall call,
        SqlFunction unresolvedConstructor,
        SqlFunction resolvedConstructor,
        RelDataType [] argTypes)
    {
        SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
        assert (sqlIdentifier != null);
        RelDataType type = catalogReader.getNamedType(sqlIdentifier);
        if (type == null) {
            // TODO jvs 12-Feb-2005:  proper type name formatting
            throw newValidationError(
                sqlIdentifier,
                EigenbaseResource.instance().UnknownDatatypeName.ex(
                    sqlIdentifier.toString()));
        }

        if (resolvedConstructor == null) {
            if (call.getOperands().length > 0) {
                // This is not a default constructor invocation, and
                // no user-defined constructor could be found
                handleUnresolvedFunction(call, unresolvedConstructor, argTypes);
            }
        } else {
            SqlCall testCall =
                resolvedConstructor.createCall(
                    call.getParserPosition(),
                    call.getOperands());
            RelDataType returnType =
                resolvedConstructor.validateOperands(
                    this,
                    scope,
                    testCall);
            assert (type == returnType);
        }

        if (shouldExpandIdentifiers()) {
            if (resolvedConstructor != null) {
                call.setOperator(resolvedConstructor);
            } else {
                // fake a fully-qualified call to the default constructor
                SqlReturnTypeInference returnTypeInference =
                    new ExplicitReturnTypeInference(type);
                call.setOperator(
                    new SqlFunction(
                        type.getSqlIdentifier(),
                        returnTypeInference,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.UserDefinedConstructor));
            }
        }
        return type;
    }

    public void handleUnresolvedFunction(
        SqlCall call,
        SqlFunction unresolvedFunction,
        RelDataType [] argTypes)
    {
        // For builtins, we can give a better error message
        List<SqlOperator> overloads =
            opTab.lookupOperatorOverloads(
                unresolvedFunction.getNameAsId(),
                null,
                SqlSyntax.Function);
        if (overloads.size() == 1) {
            SqlFunction fun = (SqlFunction) overloads.get(0);
            if ((fun.getSqlIdentifier() == null)
                && (fun.getSyntax() != SqlSyntax.FunctionId))
            {
                final int expectedArgCount =
                    fun.getOperandCountRange().getMin();
                throw newValidationError(
                    call,
                    EigenbaseResource.instance().InvalidArgCount.ex(
                        call.getOperator().getName(),
                        expectedArgCount));
            }
        }

        AssignableOperandTypeChecker typeChecking =
            new AssignableOperandTypeChecker(argTypes);
        String signature =
            typeChecking.getAllowedSignatures(
                unresolvedFunction,
                unresolvedFunction.getName());
        throw newValidationError(
            call,
            EigenbaseResource.instance().ValidatorUnknownFunction.ex(
                signature));
    }

    protected void inferUnknownTypes(
        RelDataType inferredType,
        SqlValidatorScope scope,
        SqlNode node)
    {
        final SqlValidatorScope newScope = scopes.get(node);
        if (newScope != null) {
            scope = newScope;
        }
        boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
        if ((node instanceof SqlDynamicParam) || isNullLiteral) {
            if (inferredType.equals(unknownType)) {
                if (isNullLiteral) {
                    throw newValidationError(
                        node,
                        EigenbaseResource.instance().NullIllegal.ex());
                } else {
                    throw newValidationError(
                        node,
                        EigenbaseResource.instance().DynamicParamIllegal.ex());
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
            setValidatedNodeTypeImpl(node, newInferredType);
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
                    type = inferredType.getFields()[i].getType();
                    ++i;
                } else {
                    type = inferredType;
                }
                inferUnknownTypes(type, scope, child);
            }
        } else if (node instanceof SqlCase) {
            // REVIEW wael: can this be done in a paramtypeinference strategy
            // object?
            SqlCase caseCall = (SqlCase) node;
            RelDataType returnType = deriveType(scope, node);

            SqlNodeList whenList = caseCall.getWhenOperands();
            for (int i = 0; i < whenList.size(); i++) {
                SqlNode sqlNode = whenList.get(i);
                inferUnknownTypes(unknownType, scope, sqlNode);
            }
            SqlNodeList thenList = caseCall.getThenOperands();
            for (int i = 0; i < thenList.size(); i++) {
                SqlNode sqlNode = thenList.get(i);
                inferUnknownTypes(returnType, scope, sqlNode);
            }

            if (!SqlUtil.isNullLiteral(
                    caseCall.getElseOperand(),
                    false))
            {
                inferUnknownTypes(
                    returnType,
                    scope,
                    caseCall.getElseOperand());
            } else {
                setValidatedNodeTypeImpl(
                    caseCall.getElseOperand(),
                    returnType);
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            SqlOperandTypeInference operandTypeInference =
                call.getOperator().getOperandTypeInference();
            SqlNode [] operands = call.getOperands();
            RelDataType [] operandTypes = new RelDataType[operands.length];
            if (operandTypeInference == null) {
                // TODO:  eventually should assert(operandTypeInference != null)
                // instead; for now just eat it
                Arrays.fill(operandTypes, unknownType);
            } else {
                operandTypeInference.inferOperandTypes(
                    new SqlCallBinding(this, scope, call),
                    inferredType,
                    operandTypes);
            }
            for (int i = 0; i < operands.length; ++i) {
                inferUnknownTypes(operandTypes[i], scope, operands[i]);
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
        final boolean includeSystemVars)
    {
        String alias = SqlValidatorUtil.getAlias(exp, -1);
        String uniqueAlias = SqlValidatorUtil.uniquify(alias, aliases);
        if (!alias.equals(uniqueAlias)) {
            exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
        }
        fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
        list.add(exp);
    }

    public String deriveAlias(
        SqlNode node,
        int ordinal)
    {
        return SqlValidatorUtil.getAlias(node, ordinal);
    }

    // implement SqlValidator
    public void setIdentifierExpansion(boolean expandIdentifiers)
    {
        this.expandIdentifiers = expandIdentifiers;
    }

    // implement SqlValidator
    public void setColumnReferenceExpansion(
        boolean expandColumnReferences)
    {
        this.expandColumnReferences = expandColumnReferences;
    }

    // implement SqlValidator
    public boolean getColumnReferenceExpansion()
    {
        return expandColumnReferences;
    }

    // implement SqlValidator
    public void setCallRewrite(boolean rewriteCalls)
    {
        this.rewriteCalls = rewriteCalls;
    }

    public boolean shouldExpandIdentifiers()
    {
        return expandIdentifiers;
    }

    protected boolean shouldAllowIntermediateOrderBy()
    {
        return true;
    }

    /**
     * Registers a new namespace, and adds it as a child of its parent scope.
     * Derived class can override this method to tinker with namespaces as they
     * are created.
     *
     * @param usingScope Parent scope (which will want to look for things in
     * this namespace)
     * @param alias Alias by which parent will refer to this namespace
     * @param ns Namespace
     * @param forceNullable Whether to force the type of namespace to be
     */
    protected void registerNamespace(
        SqlValidatorScope usingScope,
        String alias,
        SqlValidatorNamespace ns,
        boolean forceNullable)
    {
        if (forceNullable) {
            ns.makeNullable();
        }
        namespaces.put(
            ns.getNode(),
            ns);
        if (usingScope != null) {
            usingScope.addChild(ns, alias);
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
     * @param parentScope Parent scope which this scope turns to in order to
     * resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Node which namespace is based on
     * @param enclosingNode Outermost node for namespace, including decorations
     * such as alias and sample clause
     * @param alias Alias
     * @param forceNullable Whether to force the type of namespace to be
     * nullable because it is in an outer join
     *
     * @return registered node, usually the same as {@code node}
     */
    private SqlNode registerFrom(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        final SqlNode node,
        SqlNode enclosingNode,
        String alias,
        boolean forceNullable)
    {
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
                alias = call.operands[1].toString();
            }
            SqlValidatorScope usingScope2 = usingScope;
            if (call.getOperands().length > 2) {
                usingScope2 = null;
            }
            expr = call.operands[0];
            newExpr =
                registerFrom(
                    parentScope,
                    usingScope2,
                    expr,
                    enclosingNode,
                    alias,
                    forceNullable);
            if (newExpr != expr) {
                call.setOperand(0, newExpr);
            }

            // If alias has a column list, introduce a namespace to translate
            // column names.
            if (call.getOperands().length > 2) {
                registerNamespace(
                    usingScope,
                    alias,
                    new AliasNamespace(this, call, enclosingNode),
                    false);
            }
            return node;

        case TABLESAMPLE:
            call = (SqlCall) node;
            expr = call.operands[0];
            newExpr =
                registerFrom(
                    parentScope,
                    usingScope,
                    expr,
                    enclosingNode,
                    alias,
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
            boolean rightIsLateral = false;

            if (right.getKind() == SqlKind.LATERAL
                || (right.getKind() == SqlKind.AS
                    && ((SqlCall) right).operands[0].getKind()
                       == SqlKind.LATERAL))
            {
                rightIsLateral = true;
            }

            boolean forceLeftNullable = forceNullable;
            boolean forceRightNullable = forceNullable;
            if (join.getJoinType() == SqlJoinOperator.JoinType.Left) {
                forceRightNullable = true;
            }
            if (join.getJoinType() == SqlJoinOperator.JoinType.Right) {
                forceLeftNullable = true;
            }
            if (join.getJoinType() == SqlJoinOperator.JoinType.Full) {
                forceLeftNullable = true;
                forceRightNullable = true;
            }
            final SqlNode newLeft =
                registerFrom(
                    parentScope,
                    joinScope,
                    left,
                    left,
                    null,
                    forceLeftNullable);
            if (newLeft != left) {
                join.setOperand(SqlJoin.LEFT_OPERAND, newLeft);
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
                    forceRightNullable);
            if (newRight != right) {
                join.setOperand(SqlJoin.RIGHT_OPERAND, newRight);
            }
            final JoinNamespace joinNamespace = new JoinNamespace(this, join);
            registerNamespace(null, null, joinNamespace, forceNullable);
            return join;

        case IDENTIFIER:
            final SqlIdentifier id = (SqlIdentifier) node;
            final IdentifierNamespace newNs =
                new IdentifierNamespace(
                    this,
                    id,
                    enclosingNode);
            registerNamespace(usingScope, alias, newNs, forceNullable);
            return newNode;

        case LATERAL:
            return registerFrom(
                parentScope,
                usingScope,
                ((SqlCall) node).operands[0],
                enclosingNode,
                alias,
                forceNullable);

        case COLLECTION_TABLE:
            call = (SqlCall) node;
            operand = call.operands[0];
            newOperand =
                registerFrom(
                    parentScope,
                    usingScope,
                    operand,
                    enclosingNode,
                    alias,
                    forceNullable);
            if (newOperand != operand) {
                call.setOperand(0, newOperand);
            }
            return newNode;

        case SELECT:
        case UNION:
        case INTERSECT:
        case EXCEPT:
        case VALUES:
        case UNNEST:
        case OTHER_FUNCTION:
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
            operand = call.operands[0];
            newOperand =
                registerFrom(
                    parentScope,
                    overScope,
                    operand,
                    enclosingNode,
                    alias,
                    forceNullable);
            if (newOperand != operand) {
                call.setOperand(0, newOperand);
            }

            for (Pair<String, SqlValidatorNamespace> p : overScope.children) {
                registerNamespace(
                    usingScope,
                    p.left,
                    p.right,
                    forceNullable);
            }

            return newNode;

        default:
            throw Util.unexpected(kind);
        }
    }

    protected boolean shouldAllowOverRelation()
    {
        return false;
    }

    /**
     * Creates a namespace for a <code>SELECT</code> node. Derived class may
     * override this factory method.
     *
     * @param select Select node
     * @param enclosingNode Enclosing node
     *
     * @return Select namespace
     */
    protected SelectNamespace createSelectNamespace(
        SqlSelect select,
        SqlNode enclosingNode)
    {
        return new SelectNamespace(this, select, enclosingNode);
    }

    /**
     * Creates a namespace for a set operation (<code>UNION</code>, <code>
     * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override
     * this factory method.
     *
     * @param call Call to set operation
     * @param enclosingNode Enclosing node
     *
     * @return Set operation namespace
     */
    protected SetopNamespace createSetopNamespace(
        SqlCall call,
        SqlNode enclosingNode)
    {
        return new SetopNamespace(this, call, enclosingNode);
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to
     * resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if
     * usingScope != null
     *
     * @pre usingScope == null || alias != null
     */
    private void registerQuery(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        SqlNode enclosingNode,
        String alias,
        boolean forceNullable)
    {
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
     * resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if
     * usingScope != null
     * @param checkUpdate if true, validate that the update feature is supported
     * if validating the update statement
     *
     * @pre usingScope == null || alias != null
     */
    private void registerQuery(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        SqlNode enclosingNode,
        String alias,
        boolean forceNullable,
        boolean checkUpdate)
    {
        assert node != null;
        assert enclosingNode != null;
        Util.pre(
            (usingScope == null)
            || (alias != null),
            "usingScope == null || alias != null");

        SqlCall call;
        SqlNode [] operands;
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
            registerOperandSubqueries(
                selectScope,
                select,
                SqlSelect.WHERE_OPERAND);

            // Register FROM with the inherited scope 'parentScope', not
            // 'selectScope', otherwise tables in the FROM clause would be
            // able to see each other.
            final SqlNode from = select.getFrom();
            final SqlNode newFrom =
                registerFrom(
                    parentScope,
                    selectScope,
                    from,
                    from,
                    null,
                    false);
            if (newFrom != from) {
                select.setOperand(SqlSelect.FROM_OPERAND, newFrom);
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
            registerSubqueries(selectScope, select.getGroup());
            registerOperandSubqueries(
                aggScope,
                select,
                SqlSelect.HAVING_OPERAND);
            registerSubqueries(aggScope, select.getSelectList());
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
                registerSubqueries(orderScope, orderList);

                if (!isAggregate(select)) {
                    // Since this is not an aggregating query,
                    // there cannot be any aggregates in the ORDER BY clause.
                    SqlNode agg = aggFinder.findAgg(orderList);
                    if (agg != null) {
                        throw newValidationError(
                            agg,
                            EigenbaseResource.instance()
                            .AggregateIllegalInOrderBy.ex());
                    }
                }
            }
            break;

        case INTERSECT:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_F302,
                node.getParserPosition());
            registerSetop(
                parentScope,
                usingScope,
                node,
                node,
                alias,
                forceNullable);
            break;

        case EXCEPT:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_E071_03,
                node.getParserPosition());
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
            operands = call.getOperands();
            for (int i = 0; i < operands.length; ++i) {
                assert (operands[i].getKind() == SqlKind.ROW);

                // FIXME jvs 9-Feb-2005:  Correlation should
                // be illegal in these subqueries.  Same goes for
                // any non-lateral SELECT in the FROM list.
                registerOperandSubqueries(parentScope, call, i);
            }
            break;

        case INSERT:
            SqlInsert insertCall = (SqlInsert) node;
            InsertNamespace insertNs =
                new InsertNamespace(
                    this,
                    insertCall,
                    enclosingNode);
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
                    enclosingNode);
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
                validateFeature(
                    EigenbaseResource.instance().SQLFeature_E101_03,
                    node.getParserPosition());
            }
            SqlUpdate updateCall = (SqlUpdate) node;
            UpdateNamespace updateNs =
                new UpdateNamespace(
                    this,
                    updateCall,
                    enclosingNode);
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
            validateFeature(
                EigenbaseResource.instance().SQLFeature_F312,
                node.getParserPosition());
            SqlMerge mergeCall = (SqlMerge) node;
            MergeNamespace mergeNs =
                new MergeNamespace(
                    this,
                    mergeCall,
                    enclosingNode);
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
                new UnnestNamespace(this, call, usingScope, enclosingNode);
            registerNamespace(
                usingScope,
                alias,
                unnestNs,
                forceNullable);
            registerOperandSubqueries(usingScope, call, 0);
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
            registerSubqueries(parentScope, call);
            break;

        case MULTISET_QUERY_CONSTRUCTOR:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_S271,
                node.getParserPosition());
            call = (SqlCall) node;
            CollectScope cs = new CollectScope(parentScope, usingScope, call);
            final CollectNamespace ttableConstructorNs =
                new CollectNamespace(call, cs, enclosingNode);
            final String alias2 = deriveAlias(node, nextGeneratedId++);
            registerNamespace(
                usingScope,
                alias2,
                ttableConstructorNs,
                forceNullable);
            operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                registerOperandSubqueries(parentScope, call, i);
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
        boolean forceNullable)
    {
        SqlCall call = (SqlCall) node;
        final SetopNamespace setopNamespace =
            createSetopNamespace(call, enclosingNode);
        registerNamespace(usingScope, alias, setopNamespace, forceNullable);

        // A setop is in the same scope as its parent.
        scopes.put(call, parentScope);
        for (SqlNode operand : call.operands) {
            registerQuery(
                parentScope,
                null,
                operand,
                operand,
                null,
                false);
        }
    }

    public boolean isAggregate(SqlSelect select)
    {
        return (select.getGroup() != null) || (select.getHaving() != null)
            || (aggFinder.findAgg(select.getSelectList()) != null);
    }

    public boolean isAggregate(SqlNode selectNode)
    {
        return (aggFinder.findAgg(selectNode) != null);
    }

    private void validateNodeFeature(SqlNode node)
    {
        switch (node.getKind()) {
        case MULTISET_VALUE_CONSTRUCTOR:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_S271,
                node.getParserPosition());
            break;
        }
    }

    private void registerSubqueries(
        SqlValidatorScope parentScope,
        SqlNode node)
    {
        if (node == null) {
            return;
        } else if (node.getKind().belongsTo(SqlKind.QUERY)) {
            registerQuery(parentScope, null, node, node, null, false);
        } else if (node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR) {
            registerQuery(parentScope, null, node, node, null, false);
        } else if (node instanceof SqlCall) {
            validateNodeFeature(node);
            SqlCall call = (SqlCall) node;
            final SqlNode[] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                registerOperandSubqueries(parentScope, call, i);
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                SqlNode listNode = list.get(i);
                if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
                    listNode =
                        SqlStdOperatorTable.scalarQueryOperator.createCall(
                            listNode.getParserPosition(),
                            listNode);
                    list.set(i, listNode);
                }
                registerSubqueries(parentScope, listNode);
            }
        } else {
            // atomic node -- can be ignored
        }
    }

    /**
     * Registers any subqueries inside a given call operand, and converts the
     * operand to a scalar subquery if the operator requires it.
     *
     * @param parentScope Parent scope
     * @param call Call
     * @param operandOrdinal Ordinal of operand within call
     *
     * @see SqlOperator#argumentMustBeScalar(int)
     */
    private void registerOperandSubqueries(
        SqlValidatorScope parentScope,
        SqlCall call,
        int operandOrdinal)
    {
        SqlNode operand = call.getOperands()[operandOrdinal];
        if (operand == null) {
            return;
        }
        if (operand.getKind().belongsTo(SqlKind.QUERY)
            && call.getOperator().argumentMustBeScalar(operandOrdinal))
        {
            operand =
                SqlStdOperatorTable.scalarQueryOperator.createCall(
                    operand.getParserPosition(),
                    operand);
            call.setOperand(operandOrdinal, operand);
        }
        registerSubqueries(parentScope, operand);
    }

    public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope)
    {
        final SqlIdentifier fqId = scope.fullyQualify(id);
        if (expandColumnReferences) {
            // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
            // own ideas about qualification.
            id.assignNamesFrom(fqId);
        } else {
            Util.discard(fqId);
        }
    }

    public void validateLiteral(SqlLiteral literal)
    {
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
                throw newValidationError(
                    literal,
                    EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                        bd.toString()));
            }
            break;

        case DOUBLE:
            validateLiteralAsDouble(literal);
            break;

        case BINARY:
            final BitString bitString = (BitString) literal.getValue();
            if ((bitString.getBitCount() % 8) != 0) {
                throw newValidationError(
                    literal,
                    EigenbaseResource.instance().BinaryLiteralOdd.ex());
            }
            break;

        case DATE:
        case TIME:
        case TIMESTAMP:
            Calendar calendar = (Calendar) literal.getValue();
            final int year = calendar.get(Calendar.YEAR);
            final int era = calendar.get(Calendar.ERA);
            if ((year < 1) || (era == GregorianCalendar.BC) || (year > 9999)) {
                throw newValidationError(
                    literal,
                    EigenbaseResource.instance().DateLiteralOutOfRange.ex(
                        literal.toString()));
            }
            break;

        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
            if (literal instanceof SqlIntervalLiteral) {
                SqlIntervalLiteral.IntervalValue interval =
                    (SqlIntervalLiteral.IntervalValue)
                    ((SqlIntervalLiteral) literal).getValue();
                SqlIntervalQualifier intervalQualifier =
                    interval.getIntervalQualifier();

                // ensure qualifier is good before attempting to validate
                // literal
                validateIntervalQualifier(intervalQualifier);
                String intervalStr = interval.getIntervalLiteral();
                try {
                    int [] values =
                        intervalQualifier.evaluateIntervalLiteral(intervalStr);
                    Util.discard(values);
                } catch (SqlValidatorException e) {
                    throw newValidationError(literal, e);
                }
            }
            break;
        default:
            // default is to do nothing
        }
    }

    private void validateLiteralAsDouble(SqlLiteral literal)
    {
        BigDecimal bd = (BigDecimal) literal.getValue();
        double d = bd.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            // overflow
            throw newValidationError(
                literal,
                EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                    Util.toScientificNotation(bd)));
        }

        // REVIEW jvs 4-Aug-2004:  what about underflow?
    }

    public void validateIntervalQualifier(SqlIntervalQualifier qualifier)
    {
        assert (qualifier != null);
        boolean startPrecisionOutOfRange = false;
        boolean fractionalSecondPrecisionOutOfRange = false;

        if (qualifier.isYearMonth()) {
            if ((qualifier.getStartPrecision()
                    < SqlTypeName.INTERVAL_YEAR_MONTH.getMinPrecision())
                || (qualifier.getStartPrecision()
                    > SqlTypeName.INTERVAL_YEAR_MONTH.getMaxPrecision()))
            {
                startPrecisionOutOfRange = true;
            } else if (
                (qualifier.getFractionalSecondPrecision()
                    < SqlTypeName.INTERVAL_YEAR_MONTH.getMinScale())
                || (qualifier.getFractionalSecondPrecision()
                    > SqlTypeName.INTERVAL_YEAR_MONTH.getMaxScale()))
            {
                fractionalSecondPrecisionOutOfRange = true;
            }
        } else {
            if ((qualifier.getStartPrecision()
                    < SqlTypeName.INTERVAL_DAY_TIME.getMinPrecision())
                || (qualifier.getStartPrecision()
                    > SqlTypeName.INTERVAL_DAY_TIME.getMaxPrecision()))
            {
                startPrecisionOutOfRange = true;
            } else if (
                (qualifier.getFractionalSecondPrecision()
                    < SqlTypeName.INTERVAL_DAY_TIME.getMinScale())
                || (qualifier.getFractionalSecondPrecision()
                    > SqlTypeName.INTERVAL_DAY_TIME.getMaxScale()))
            {
                fractionalSecondPrecisionOutOfRange = true;
            }
        }

        if (startPrecisionOutOfRange) {
            throw newValidationError(
                qualifier,
                EigenbaseResource.instance().IntervalStartPrecisionOutOfRange
                .ex(
                    Integer.toString(qualifier.getStartPrecision()),
                    "INTERVAL " + qualifier.toString()));
        } else if (fractionalSecondPrecisionOutOfRange) {
            throw newValidationError(
                qualifier,
                EigenbaseResource.instance()
                .IntervalFractionalSecondPrecisionOutOfRange.ex(
                    Integer.toString(qualifier.getFractionalSecondPrecision()),
                    "INTERVAL " + qualifier.toString()));
        }
    }

    /**
     * Validates the FROM clause of a query, or (recursively) a child node of
     * the FROM clause: AS, OVER, JOIN, VALUES, or subquery.
     *
     * @param node Node in FROM clause, typically a table or derived table
     * @param targetRowType Desired row type of this expression, or {@link
     * #unknownType} if not fussy. Must not be null.
     * @param scope Scope
     */
    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        SqlValidatorScope scope)
    {
        Util.pre(targetRowType != null, "targetRowType != null");
        switch (node.getKind()) {
        case AS:
            validateFrom(
                ((SqlCall) node).getOperands()[0],
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
            validateQuery(node, scope);
            break;
        }

        // Validate the namespace representation of the node, just in case the
        // validation did not occur implicitly.
        getNamespace(node).validate();
    }

    protected void validateOver(SqlCall call, SqlValidatorScope scope)
    {
        throw Util.newInternal("OVER unexpected in this context");
    }

    protected void validateJoin(SqlJoin join, SqlValidatorScope scope)
    {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        boolean natural = join.isNatural();
        SqlJoinOperator.JoinType joinType = join.getJoinType();
        SqlJoinOperator.ConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = scopes.get(join);
        validateFrom(left, unknownType, joinScope);
        validateFrom(right, unknownType, joinScope);

        // Validate condition.
        switch (conditionType) {
        case None:
            Util.permAssert(condition == null, "condition == null");
            break;
        case On:
            Util.permAssert(condition != null, "condition != null");
            validateWhereOrOn(joinScope, condition, "ON");
            break;
        case Using:
            SqlNodeList list = (SqlNodeList) condition;

            // Parser ensures that using clause is not empty.
            Util.permAssert(list.size() > 0, "Empty USING clause");
            for (int i = 0; i < list.size(); i++) {
                SqlIdentifier id = (SqlIdentifier) list.get(i);
                final RelDataType leftColType = validateUsingCol(id, left);
                final RelDataType rightColType = validateUsingCol(id, right);
                if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
                    throw newValidationError(
                        id,
                        EigenbaseResource.instance()
                        .NaturalOrUsingColumnNotCompatible.ex(
                            id.getSimple(),
                            leftColType.toString(),
                            rightColType.toString()));
                }
            }
            break;
        default:
            throw Util.unexpected(conditionType);
        }

        // Validate NATURAL.
        if (natural) {
            if (condition != null) {
                throw newValidationError(
                    condition,
                    EigenbaseResource.instance().NaturalDisallowsOnOrUsing
                    .ex());
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
            for (String name : naturalColumnNames) {
                final RelDataType leftColType =
                    leftRowType.getField(name).getType();
                final RelDataType rightColType =
                    rightRowType.getField(name).getType();
                if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
                    throw newValidationError(
                        join,
                        EigenbaseResource.instance()
                        .NaturalOrUsingColumnNotCompatible.ex(
                            name,
                            leftColType.toString(),
                            rightColType.toString()));
                }
            }
        }

        // Which join types require/allow a ON/USING condition, or allow
        // a NATURAL keyword?
        switch (joinType) {
        case Inner:
        case Left:
        case Right:
        case Full:
            if ((condition == null) && !natural) {
                throw newValidationError(
                    join,
                    EigenbaseResource.instance().JoinRequiresCondition.ex());
            }
            break;
        case Comma:
        case Cross:
            if (condition != null) {
                throw newValidationError(
                    join.operands[SqlJoin.CONDITION_TYPE_OPERAND],
                    EigenbaseResource.instance().CrossJoinDisallowsCondition
                    .ex());
            }
            if (natural) {
                throw newValidationError(
                    join.operands[SqlJoin.CONDITION_TYPE_OPERAND],
                    EigenbaseResource.instance().CrossJoinDisallowsCondition
                    .ex());
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
     * @param condition Parse tree
     * @param clause Name of clause: "WHERE", "GROUP BY", "ON"
     */
    private void validateNoAggs(SqlNode condition, String clause)
    {
        final SqlNode agg = aggOrOverFinder.findAgg(condition);
        if (agg != null) {
            if (SqlUtil.isCallTo(agg, SqlStdOperatorTable.overOperator)) {
                throw newValidationError(
                    agg,
                    EigenbaseResource.instance()
                    .WindowedAggregateIllegalInClause.ex(clause));
            } else {
                throw newValidationError(
                    agg,
                    EigenbaseResource.instance().AggregateIllegalInClause.ex(
                        clause));
            }
        }
    }

    private RelDataType validateUsingCol(SqlIdentifier id, SqlNode leftOrRight)
    {
        if (id.names.length == 1) {
            String name = id.names[0];
            final SqlValidatorNamespace namespace = getNamespace(leftOrRight);
            final RelDataType rowType = namespace.getRowType();
            final RelDataTypeField field = rowType.getField(name);
            if (field != null) {
                if (SqlValidatorUtil.countOccurrences(
                        name,
                        SqlTypeUtil.getFieldNames(rowType))
                    > 1)
                {
                    throw newValidationError(
                        id,
                        EigenbaseResource.instance().ColumnInUsingNotUnique.ex(
                            id.toString()));
                }
                return field.getType();
            }
        }
        throw newValidationError(
            id,
            EigenbaseResource.instance().ColumnNotFound.ex(
                id.toString()));
    }

    /**
     * Validates a SELECT statement.
     *
     * @param select Select statement
     * @param targetRowType Desired row type, must not be null, may be the data
     * type 'unknown'.
     *
     * @pre targetRowType != null
     */
    protected void validateSelect(
        SqlSelect select,
        RelDataType targetRowType)
    {
        assert targetRowType != null;

        // Namespace is either a select namespace or a wrapper around one.
        final SelectNamespace ns =
            getNamespace(select).unwrap(SelectNamespace.class);

        // Its rowtype is null, meaning it hasn't been validated yet.
        // This is important, because we need to take the targetRowType into
        // account.
        assert ns.rowType == null;

        if (select.isDistinct()) {
            validateFeature(
                EigenbaseResource.instance().SQLFeature_E051_01,
                select.getModifierNode(
                    SqlSelectKeyword.Distinct).getParserPosition());
        }

        final SqlNodeList selectItems = select.getSelectList();
        RelDataType fromType = unknownType;
        if (selectItems.size() == 1) {
            final SqlNode selectItem = selectItems.get(0);
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) selectItem;
                if (id.isStar() && (id.names.length == 1)) {
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
        final SqlValidatorScope fromScope = getFromScope(select);
        final List<Pair<String, SqlValidatorNamespace>> children =
            ((SelectScope) fromScope).children;
        int duplicateAliasOrdinal = firstDuplicate(Pair.left(children));
        if (duplicateAliasOrdinal >= 0) {
            final Pair<String, SqlValidatorNamespace> child =
                children.get(duplicateAliasOrdinal);
            throw newValidationError(
                child.right.getEnclosingNode(),
                EigenbaseResource.instance().FromAliasDuplicate.ex(child.left));
        }

        validateFrom(
            select.getFrom(),
            fromType,
            fromScope);

        validateWhereClause(select);
        validateGroupClause(select);
        validateHavingClause(select);
        validateWindowClause(select);

        // Validate the SELECT clause late, because a select item might
        // depend on the GROUP BY list, or the window function might reference
        // window name in the WINDOW clause etc.
        final RelDataType rowType =
            validateSelectList(selectItems, select, targetRowType);
        ns.setRowType(rowType);

        // Validate ORDER BY after we have set ns.rowType because in some
        // dialects you can refer to columns of the select list, e.g.
        // "SELECT empno AS x FROM emp ORDER BY x"
        validateOrderList(select);
    }

    /**
     * Returns the ordinal of the first element in the list which is equal to a
     * previous element in the list.
     *
     * <p>For example, <code>firstDuplicate(Arrays.asList("a", "b", "c", "b",
     * "a"))</code> returns 3, the ordinal of the 2nd "b".
     *
     * @param list List
     *
     * @return Ordinal of first duplicate, or -1 if not found
     */
    private static <T> int firstDuplicate(List<T> list)
    {
        // For large lists, it's more efficient to build a set to do a quick
        // check for duplicates before we do an O(n^2) search.
        if ((list.size() > 10)
            && (new HashSet<T>(list).size() == list.size()))
        {
            return -1;
        }
        for (int i = 1; i < list.size(); i++) {
            final T e0 = list.get(i);
            for (int j = 0; j < i; j++) {
                final T e1 = list.get(j);
                if (e0.equals(e1)) {
                    return i; // ordinal of the later item
                }
            }
        }
        return -1;
    }

    protected void validateWindowClause(SqlSelect select)
    {
        final SqlNodeList windowList = select.getWindowList();
        if ((windowList == null) || (windowList.size() == 0)) {
            return;
        }

        final SelectScope windowScope = (SelectScope) getFromScope(select);
        Util.permAssert(windowScope != null, "windowScope != null");

        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        for (SqlNode node : windowList) {
            final SqlWindow child = (SqlWindow) node;
            SqlIdentifier declName = child.getDeclName();
            if (!declName.isSimple()) {
                throw newValidationError(
                    declName,
                    EigenbaseResource.instance().WindowNameMustBeSimple.ex());
            }

            if (windowScope.existingWindowName(declName.toString())) {
                throw newValidationError(
                    declName,
                    EigenbaseResource.instance().DuplicateWindowName.ex());
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
                if (window1.equalsDeep(window2, false)) {
                    throw newValidationError(
                        window2,
                        EigenbaseResource.instance().DupWindowSpec.ex());
                }
            }
        }

        // Hand off to validate window spec components
        windowList.validate(this, windowScope);
    }

    /**
     * Validates the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     */
    protected void validateOrderList(SqlSelect select)
    {
        // ORDER BY is validated in a scope where aliases in the SELECT clause
        // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
        // is valid.
        SqlNodeList orderList = select.getOrderList();
        if (orderList == null) {
            return;
        }
        if (!shouldAllowIntermediateOrderBy()) {
            if (!cursorSet.contains(select)) {
                throw newValidationError(
                    select,
                    EigenbaseResource.instance().InvalidOrderByPos.ex());
            }
        }
        final SqlValidatorScope orderScope = getOrderScope(select);

        Util.permAssert(orderScope != null, "orderScope != null");
        for (SqlNode orderItem : orderList) {
            validateOrderItem(select, orderItem);
        }
    }

    private void validateOrderItem(SqlSelect select, SqlNode orderItem)
    {
        if (SqlUtil.isCallTo(
                orderItem,
                SqlStdOperatorTable.descendingOperator))
        {
            validateFeature(
                EigenbaseResource.instance().SQLConformance_OrderByDesc,
                orderItem.getParserPosition());
            validateOrderItem(
                select,
                ((SqlCall) orderItem).operands[0]);
            return;
        }

        final SqlValidatorScope orderScope = getOrderScope(select);
        validateExpr(orderItem, orderScope);
    }

    public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr)
    {
        return new OrderExpressionExpander(select, orderExpr).go();
    }

    /**
     * Validates the GROUP BY clause of a SELECT statement. This method is
     * called even if no GROUP BY clause is present.
     */
    protected void validateGroupClause(SqlSelect select)
    {
        SqlNodeList groupList = select.getGroup();
        if (groupList == null) {
            return;
        }
        validateNoAggs(groupList, "GROUP BY");
        final SqlValidatorScope groupScope = getGroupScope(select);
        inferUnknownTypes(unknownType, groupScope, groupList);

        groupList.validate(this, groupScope);

        // Derive the type of each GROUP BY item. We don't need the type, but
        // it resolves functions, and that is necessary for deducing
        // monotonicity.
        final SqlValidatorScope selectScope = getSelectScope(select);
        AggregatingSelectScope aggregatingScope = null;
        if (selectScope instanceof AggregatingSelectScope) {
            aggregatingScope = (AggregatingSelectScope) selectScope;
        }
        for (SqlNode groupItem : groupList) {
            final RelDataType type = deriveType(groupScope, groupItem);
            setValidatedNodeTypeImpl(groupItem, type);
            if (aggregatingScope != null) {
                aggregatingScope.addGroupExpr(groupItem);
            }
        }

        SqlNode agg = aggFinder.findAgg(groupList);
        if (agg != null) {
            throw newValidationError(
                agg,
                EigenbaseResource.instance().AggregateIllegalInGroupBy.ex());
        }
    }

    protected void validateWhereClause(SqlSelect select)
    {
        // validate WHERE clause
        final SqlNode where = select.getWhere();
        if (where == null) {
            return;
        }
        final SqlValidatorScope whereScope = getWhereScope(select);
        validateWhereOrOn(whereScope, where, "WHERE");
    }

    protected void validateWhereOrOn(
        SqlValidatorScope scope,
        SqlNode condition,
        String keyword)
    {
        validateNoAggs(condition, keyword);
        inferUnknownTypes(
            booleanType,
            scope,
            condition);
        condition.validate(this, scope);
        final RelDataType type = deriveType(scope, condition);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(
                condition,
                EigenbaseResource.instance().CondMustBeBoolean.ex(keyword));
        }
    }

    protected void validateHavingClause(SqlSelect select)
    {
        // HAVING is validated in the scope after groups have been created.
        // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
        // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
        // clause is illegal.
        final SqlNode having = select.getHaving();
        if (having == null) {
            return;
        }
        final AggregatingScope havingScope =
            (AggregatingScope) getSelectScope(select);
        havingScope.checkAggregateExpr(having, true);
        inferUnknownTypes(
            booleanType,
            havingScope,
            having);
        having.validate(this, havingScope);
        final RelDataType type = deriveType(havingScope, having);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(
                having,
                EigenbaseResource.instance().HavingMustBeBoolean.ex());
        }
    }

    protected RelDataType validateSelectList(
        final SqlNodeList selectItems,
        SqlSelect select,
        RelDataType targetRowType)
    {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final SqlValidatorScope selectScope = getSelectScope(select);
        final List<SqlNode> expandedSelectItems = new ArrayList<SqlNode>();
        final Set<String> aliases = new HashSet<String>();
        final List<Map.Entry<String, RelDataType>> fieldList =
            new ArrayList<Map.Entry<String, RelDataType>>();

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
                    expandedSelectItems,
                    aliases,
                    fieldList,
                    false);
            }
        }

        // Check expanded select list for aggregation.
        if (selectScope instanceof AggregatingScope) {
            AggregatingScope aggScope = (AggregatingScope) selectScope;
            for (SqlNode selectItem : expandedSelectItems) {
                boolean matches = aggScope.checkAggregateExpr(selectItem, true);
                Util.discard(matches);
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
            select.setOperand(SqlSelect.SELECT_OPERAND, newSelectList);
        }
        getRawSelectScope(select).setExpandedSelectList(expandedSelectItems);

        // TODO: when SELECT appears as a value subquery, should be using
        // something other than unknownType for targetRowType
        inferUnknownTypes(targetRowType, selectScope, newSelectList);

        for (SqlNode selectItem : expandedSelectItems) {
            validateExpr(selectItem, selectScope);
        }

        assert fieldList.size() >= aliases.size();
        return typeFactory.createStructType(fieldList);
    }

    /**
     * Validates an expression.
     *
     * @param expr Expression
     * @param scope Scope in which expression occurs
     */
    private void validateExpr(SqlNode expr, SqlValidatorScope scope)
    {
        // Call on the expression to validate itself.
        expr.validateExpr(this, scope);

        // Perform any validation specific to the scope. For example, an
        // aggregating scope requires that expressions are valid aggregations.
        scope.validateExpr(expr);
    }

    /**
     * Processes SubQuery found in Select list. Checks that is actually Scalar
     * subquery and makes proper entries in each of the 3 lists used to create
     * the final rowType entry.
     *
     * @param parentSelect base SqlSelect item
     * @param selectItem child SqlSelect from select list
     * @param expandedSelectItems Select items after processing
     * @param aliasList built from user or system values
     * @param fieldList Built up entries for each select list entry
     */
    private void handleScalarSubQuery(
        SqlSelect parentSelect,
        SqlSelect selectItem,
        List<SqlNode> expandedSelectItems,
        Set<String> aliasList,
        List<Map.Entry<String, RelDataType>> fieldList)
    {
        // A scalar subquery only has one output column.
        if (1 != selectItem.getSelectList().size()) {
            throw newValidationError(
                selectItem,
                EigenbaseResource.instance().OnlyScalarSubqueryAllowed.ex());
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
        setValidatedNodeTypeImpl(selectItem, type);

        // we do not want to pass on the RelRecordType returned
        // by the sub query.  Just the type of the single expression
        // in the subquery select list.
        assert type instanceof RelRecordType;
        RelRecordType rec = (RelRecordType) type;

        RelDataType nodeType = rec.getFields()[0].getType();
        nodeType = typeFactory.createTypeWithNullability(nodeType, true);
        fieldList.add(Pair.of(alias, nodeType));
    }

    /**
     * Derives a row-type for INSERT and UPDATE operations.
     *
     * @param table Target table for INSERT/UPDATE
     * @param targetColumnList List of target columns, or null if not specified
     * @param append Whether to append fields to those in <code>
     * baseRowType</code>
     *
     * @return Rowtype
     */
    protected RelDataType createTargetRowType(
        SqlValidatorTable table,
        SqlNodeList targetColumnList,
        boolean append)
    {
        RelDataType baseRowType = table.getRowType();
        if (targetColumnList == null) {
            return baseRowType;
        }
        RelDataTypeField [] targetFields = baseRowType.getFields();
        int targetColumnCount = targetColumnList.size();
        if (append) {
            targetColumnCount += baseRowType.getFieldCount();
        }
        RelDataType [] types = new RelDataType[targetColumnCount];
        String [] fieldNames = new String[targetColumnCount];
        int iTarget = 0;
        if (append) {
            iTarget += baseRowType.getFieldCount();
            for (int i = 0; i < iTarget; ++i) {
                types[i] = targetFields[i].getType();
                fieldNames[i] = SqlUtil.deriveAliasFromOrdinal(i);
            }
        }
        Set<String> assignedColumnNames = new HashSet<String>();
        for (SqlNode node : targetColumnList) {
            SqlIdentifier id = (SqlIdentifier) node;
            int iColumn = baseRowType.getFieldOrdinal(id.getSimple());
            if (!assignedColumnNames.add(id.getSimple())) {
                throw newValidationError(
                    id,
                    EigenbaseResource.instance().DuplicateTargetColumn.ex(
                        id.getSimple()));
            }
            if (iColumn == -1) {
                throw newValidationError(
                    id,
                    EigenbaseResource.instance().UnknownTargetColumn.ex(
                        id.getSimple()));
            }
            fieldNames[iTarget] = targetFields[iColumn].getName();
            types[iTarget] = targetFields[iColumn].getType();
            ++iTarget;
        }
        return typeFactory.createStructType(types, fieldNames);
    }

    public void validateInsert(SqlInsert insert)
    {
        SqlValidatorNamespace targetNamespace = getNamespace(insert);
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        // INSERT has an optional column name list.  If present then
        // reduce the rowtype to the columns specified.  If not present
        // then the entire target rowtype is used.
        RelDataType targetRowType =
            createTargetRowType(
                table,
                insert.getTargetColumnList(),
                false);

        SqlNode source = insert.getSource();
        if (source instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) source;
            validateSelect(sqlSelect, targetRowType);
        } else {
            SqlValidatorScope scope = scopes.get(source);
            validateQuery(source, scope);
        }

        // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
        // discarding the type inferred by inferUnknownTypes (which was invoked
        // from validateSelect above).  It would be better if that information
        // were used here so that we never saw any untyped nulls during
        // checkTypeAssignment.
        RelDataType sourceRowType = getNamespace(source).getRowType();
        RelDataType logicalTargetRowType =
            getLogicalTargetRowType(targetRowType, insert);
        setValidatedNodeType(insert, logicalTargetRowType);
        RelDataType logicalSourceRowType =
            getLogicalSourceRowType(sourceRowType, insert);

        checkFieldCount(insert, logicalSourceRowType, logicalTargetRowType);

        checkTypeAssignment(logicalSourceRowType, logicalTargetRowType, insert);

        validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT);
    }

    private void checkFieldCount(
        SqlNode node,
        RelDataType logicalSourceRowType,
        RelDataType logicalTargetRowType)
    {
        final int sourceFieldCount = logicalSourceRowType.getFieldCount();
        final int targetFieldCount = logicalTargetRowType.getFieldCount();
        if (sourceFieldCount != targetFieldCount) {
            throw newValidationError(
                node,
                EigenbaseResource.instance().UnmatchInsertColumn.ex(
                    targetFieldCount,
                    sourceFieldCount));
        }
    }

    protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType,
        SqlInsert insert)
    {
        return targetRowType;
    }

    protected RelDataType getLogicalSourceRowType(
        RelDataType sourceRowType,
        SqlInsert insert)
    {
        return sourceRowType;
    }

    protected void checkTypeAssignment(
        RelDataType sourceRowType,
        RelDataType targetRowType,
        final SqlNode query)
    {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        RelDataTypeField [] sourceFields = sourceRowType.getFields();
        RelDataTypeField [] targetFields = targetRowType.getFields();
        final int sourceCount = sourceFields.length;
        for (int i = 0; i < sourceCount; ++i) {
            RelDataType sourceType = sourceFields[i].getType();
            RelDataType targetType = targetFields[i].getType();
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                // FRG-255:  account for UPDATE rewrite; there's
                // probably a better way to do this.
                int iAdjusted = i;
                if (query instanceof SqlUpdate) {
                    int nUpdateColumns =
                        ((SqlUpdate) query).getTargetColumnList().size();
                    assert (sourceFields.length >= nUpdateColumns);
                    iAdjusted -= (sourceFields.length - nUpdateColumns);
                }
                SqlNode node = getNthExpr(query, iAdjusted, sourceCount);
                String targetTypeString;
                String sourceTypeString;
                if (SqlTypeUtil.areCharacterSetsMismatched(
                        sourceType,
                        targetType))
                {
                    sourceTypeString = sourceType.getFullTypeString();
                    targetTypeString = targetType.getFullTypeString();
                } else {
                    sourceTypeString = sourceType.toString();
                    targetTypeString = targetType.toString();
                }
                throw newValidationError(
                    node,
                    EigenbaseResource.instance().TypeNotAssignable.ex(
                        targetFields[i].getName(),
                        targetTypeString,
                        sourceFields[i].getName(),
                        sourceTypeString));
            }
        }
    }

    /**
     * Locates the n'th expression in an INSERT or UPDATE query.
     *
     * @param query Query
     * @param ordinal Ordinal of expression
     * @param sourceCount Number of expressions
     *
     * @return Ordinal'th expression, never null
     */
    private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount)
    {
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

    public void validateDelete(SqlDelete call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect, unknownType);

        IdentifierNamespace targetNamespace =
            getNamespace(call.getTargetTable()).unwrap(
                IdentifierNamespace.class);
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE);
    }

    public void validateUpdate(SqlUpdate call)
    {
        IdentifierNamespace targetNamespace =
            getNamespace(call.getTargetTable()).unwrap(
                IdentifierNamespace.class);
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        RelDataType targetRowType =
            createTargetRowType(
                table,
                call.getTargetColumnList(),
                true);

        SqlSelect select = call.getSourceSelect();
        validateSelect(select, targetRowType);

        RelDataType sourceRowType = getNamespace(select).getRowType();
        checkTypeAssignment(sourceRowType, targetRowType, call);

        validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);
    }

    public void validateMerge(SqlMerge call)
    {
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
        validateNamespace(targetNamespace);

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
     * @param table Table
     * @param requiredAccess Access requested on table
     */
    private void validateAccess(
        SqlNode node,
        SqlValidatorTable table,
        SqlAccessEnum requiredAccess)
    {
        if (table != null) {
            SqlAccessType access = table.getAllowedAccess();
            if (!access.allowsAccess(requiredAccess)) {
                throw newValidationError(
                    node,
                    EigenbaseResource.instance().AccessNotAllowed.ex(
                        requiredAccess.name(),
                        Arrays.asList(table.getQualifiedName()).toString()));
            }
        }
    }

    /**
     * Validates a VALUES clause.
     *
     * @param node Values clause
     * @param targetRowType Row type which expression must conform to
     * @param scope Scope within which clause occurs
     */
    protected void validateValues(
        SqlCall node,
        RelDataType targetRowType,
        final SqlValidatorScope scope)
    {
        assert node.getKind() == SqlKind.VALUES;

        final SqlNode [] operands = node.getOperands();
        for (SqlNode operand : operands) {
            if (!(operand.getKind() == SqlKind.ROW)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars");
            }

            SqlCall rowConstructor = (SqlCall) operand;
            if (targetRowType.isStruct()
                && (rowConstructor.getOperands().length
                    != targetRowType.getFieldCount()))
            {
                return;
            }

            inferUnknownTypes(
                targetRowType,
                scope,
                rowConstructor);
        }

        for (SqlNode operand : operands) {
            operand.validate(this, scope);
        }

        // validate that all row types have the same number of columns
        //  and that expressions in each column are compatible.
        // A values expression is turned into something that looks like
        // ROW(type00, type01,...), ROW(type11,...),...
        final int rowCount = operands.length;
        if (rowCount >= 2) {
            SqlCall firstRow = (SqlCall) operands[0];
            final int columnCount = firstRow.getOperands().length;

            // 1. check that all rows have the same cols length
            for (int row = 0; row < rowCount; row++) {
                SqlCall thisRow = (SqlCall) operands[row];
                if (columnCount != thisRow.operands.length) {
                    throw newValidationError(
                        node,
                        EigenbaseResource.instance().IncompatibleValueType.ex(
                            SqlStdOperatorTable.valuesOperator.getName()));
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (int col = 0; col < columnCount; col++) {
                final int c = col;
                final RelDataType type =
                    typeFactory.leastRestrictive(
                        new AbstractList<RelDataType>() {
                            public RelDataType get(int row) {
                                SqlCall thisRow = (SqlCall) operands[row];
                                return deriveType(scope, thisRow.operands[c]);
                            }

                            public int size() {
                                return rowCount;
                            }
                        }
                    );

                if (null == type) {
                    throw newValidationError(
                        node,
                        EigenbaseResource.instance().IncompatibleValueType.ex(
                            SqlStdOperatorTable.valuesOperator.getName()));
                }
            }
        }
    }

    public void validateDataType(SqlDataTypeSpec dataType)
    {
    }

    public void validateDynamicParam(SqlDynamicParam dynamicParam)
    {
    }

    public EigenbaseException newValidationError(
        SqlNode node,
        SqlValidatorException e)
    {
        Util.pre(node != null, "node != null");
        final SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }

    protected SqlWindow getWindowByName(
        SqlIdentifier id,
        SqlValidatorScope scope)
    {
        SqlWindow window = null;
        if (id.isSimple()) {
            final String name = id.getSimple();
            window = scope.lookupWindow(name);
        }
        if (window == null) {
            throw newValidationError(
                id,
                EigenbaseResource.instance().WindowNotFound.ex(
                    id.toString()));
        }
        return window;
    }

    public SqlWindow resolveWindow(
        SqlNode windowOrRef,
        SqlValidatorScope scope,
        boolean populateBounds)
    {
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
                throw newValidationError(
                    refId,
                    EigenbaseResource.instance().WindowNotFound.ex(refName));
            }
            window = window.overlay(refWindow, this);
        }

        // Fill in missing bounds.
        if (populateBounds) {
            if (window.getLowerBound() == null) {
                window.setLowerBound(
                    SqlWindowOperator.createCurrentRow(SqlParserPos.ZERO));
            }
            if (window.getUpperBound() == null) {
                window.setUpperBound(
                    SqlWindowOperator.createCurrentRow(SqlParserPos.ZERO));
            }
        }
        return window;
    }

    public SqlNode getOriginal(SqlNode expr)
    {
        SqlNode original = originalExprs.get(expr);
        if (original == null) {
            original = expr;
        }
        return original;
    }

    public void setOriginal(SqlNode expr, SqlNode original)
    {
        // Don't overwrite the original original.
        if (originalExprs.get(expr) == null) {
            originalExprs.put(expr, original);
        }
    }

    SqlValidatorNamespace lookupFieldNamespace(
        RelDataType rowType,
        String name)
    {
        final RelDataType dataType =
            SqlValidatorUtil.lookupFieldType(rowType, name);
        return new FieldNamespace(this, dataType);
    }

    public void validateWindow(
        SqlNode windowOrId,
        SqlValidatorScope scope,
        SqlCall call)
    {
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

        Util.pre(
            null == targetWindow.getWindowCall(),
            "(null == targetWindow.getWindowFunctionCall()");
        targetWindow.setWindowCall(call);
        targetWindow.validate(this, scope);
        targetWindow.setWindowCall(null);
    }

    public void validateAggregateParams(
        SqlCall aggFunction,
        SqlValidatorScope scope)
    {
        // For agg(expr), expr cannot itself contain aggregate function
        // invocations.  For example, SUM(2*MAX(x)) is illegal; when
        // we see it, we'll report the error for the SUM (not the MAX).
        // For more than one level of nesting, the error which results
        // depends on the traversal order for validation.
        for (SqlNode param : aggFunction.getOperands()) {
            final SqlNode agg = aggOrOverFinder.findAgg(param);
            if (aggOrOverFinder.findAgg(param) != null) {
                throw newValidationError(
                    aggFunction,
                    EigenbaseResource.instance().NestedAggIllegal.ex());
            }
        }
    }

    public void validateCall(
        SqlCall call,
        SqlValidatorScope scope)
    {
        final SqlOperator operator = call.getOperator();
        if ((call.operands.length == 0)
            && (operator.getSyntax() == SqlSyntax.FunctionId)
            && !call.isExpanded())
        {
            // For example, "LOCALTIME()" is illegal. (It should be
            // "LOCALTIME", which would have been handled as a
            // SqlIdentifier.)
            handleUnresolvedFunction(
                call,
                (SqlFunction) operator,
                new RelDataType[0]);
        }

        SqlValidatorScope operandScope = scope.getOperandScope(call);

        // Delegate validation to the operator.
        operator.validateCall(call, this, scope, operandScope);
    }

    /**
     * Validates that a particular feature is enabled. By default, all features
     * are enabled; subclasses may override this method to be more
     * discriminating.
     *
     * @param feature feature being used, represented as a resource definition
     * from {@link EigenbaseResource}
     * @param context parser position context for error reporting, or null if
     * none available
     */
    protected void validateFeature(
        ResourceDefinition feature,
        SqlParserPos context)
    {
        // By default, do nothing except to verify that the resource
        // represents a real feature definition.
        assert (feature.getProperties().get("FeatureDefinition") != null);
    }

    public SqlNode expand(SqlNode expr, SqlValidatorScope scope)
    {
        final Expander expander = new Expander(this, scope);
        SqlNode newExpr = expr.accept(expander);
        if (expr != newExpr) {
            setOriginal(newExpr, expr);
        }
        return newExpr;
    }

    public boolean isSystemField(RelDataTypeField field)
    {
        return false;
    }

    public List<List<String>> getFieldOrigins(SqlNode sqlQuery)
    {
        if (sqlQuery instanceof SqlExplain) {
            return Collections.emptyList();
        }
        final RelDataType rowType = getValidatedNodeType(sqlQuery);
        final int fieldCount = rowType.getFieldCount();
        if (!sqlQuery.isA(SqlKind.QUERY)) {
            return Collections.nCopies(fieldCount, null);
        }
        final ArrayList<List<String>> list = new ArrayList<List<String>>();
        for (int i = 0; i < fieldCount; i++) {
            List<String> origin = getFieldOrigin(sqlQuery, i);
//            assert origin == null || origin.size() >= 4 : origin;
            list.add(origin);
        }
        return list;
    }

    private List<String> getFieldOrigin(SqlNode sqlQuery, int i)
    {
        if (sqlQuery instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlQuery;
            final SelectScope scope = getRawSelectScope(sqlSelect);
            final List<SqlNode> selectList = scope.getExpandedSelectList();
            SqlNode selectItem = selectList.get(i);
            if (SqlUtil.isCallTo(selectItem, SqlStdOperatorTable.asOperator)) {
                selectItem = ((SqlCall) selectItem).getOperands()[0];
            }
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) selectItem;
                SqlValidatorNamespace namespace = null;
                List<String> origin = new ArrayList<String>();
                for (String name : id.names) {
                    if (namespace == null) {
                        namespace = scope.resolve(name, null, null);
                        final SqlValidatorTable table = namespace.getTable();
                        if (table != null) {
                            origin.addAll(
                                Arrays.asList(table.getQualifiedName()));
                        } else {
                            return null;
                        }
                    } else {
                        namespace = namespace.lookupChild(name);
                        if (namespace != null) {
                            origin.add(name);
                        } else {
                            return null;
                        }
                    }
                }
                return origin;
            }
        }
        return null;
    }

    public void validateColumnListParams(
        SqlFunction function,
        RelDataType [] argTypes,
        SqlNode [] operands)
    {
        throw new UnsupportedOperationException();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Common base class for DML statement namespaces.
     */
    public static class DmlNamespace
        extends IdentifierNamespace
    {
        protected DmlNamespace(
            SqlValidatorImpl validator,
            SqlIdentifier id,
            SqlNode enclosingNode)
        {
            super(
                validator,
                id,
                enclosingNode);
        }
    }

    /**
     * Namespace for an INSERT statement.
     */
    private static class InsertNamespace
        extends DmlNamespace
    {
        private final SqlInsert node;

        public InsertNamespace(
            SqlValidatorImpl validator,
            SqlInsert node,
            SqlNode enclosingNode)
        {
            super(
                validator,
                node.getTargetTable(),
                enclosingNode);
            this.node = node;
            assert node != null;
        }

        public SqlInsert getNode()
        {
            return node;
        }
    }

    /**
     * Namespace for an UPDATE statement.
     */
    private static class UpdateNamespace
        extends DmlNamespace
    {
        private final SqlUpdate node;

        public UpdateNamespace(
            SqlValidatorImpl validator,
            SqlUpdate node,
            SqlNode enclosingNode)
        {
            super(
                validator,
                node.getTargetTable(),
                enclosingNode);
            this.node = node;
            assert node != null;
        }

        public SqlUpdate getNode()
        {
            return node;
        }
    }

    /**
     * Namespace for a DELETE statement.
     */
    private static class DeleteNamespace
        extends DmlNamespace
    {
        private final SqlDelete node;

        public DeleteNamespace(
            SqlValidatorImpl validator,
            SqlDelete node,
            SqlNode enclosingNode)
        {
            super(
                validator,
                node.getTargetTable(),
                enclosingNode);
            this.node = node;
            assert node != null;
        }

        public SqlDelete getNode()
        {
            return node;
        }
    }

    /**
     * Namespace for a MERGE statement.
     */
    private static class MergeNamespace
        extends DmlNamespace
    {
        private final SqlMerge node;

        public MergeNamespace(
            SqlValidatorImpl validator,
            SqlMerge node,
            SqlNode enclosingNode)
        {
            super(
                validator,
                node.getTargetTable(),
                enclosingNode);
            this.node = node;
            assert node != null;
        }

        public SqlMerge getNode()
        {
            return node;
        }
    }

    /**
     * Visitor which derives the type of a given {@link SqlNode}.
     *
     * <p>Each method must return the derived type. This visitor is basically a
     * single-use dispatcher; the visit is never recursive.
     */
    private class DeriveTypeVisitor
        implements SqlVisitor<RelDataType>
    {
        private final SqlValidatorScope scope;

        public DeriveTypeVisitor(SqlValidatorScope scope)
        {
            this.scope = scope;
        }

        public RelDataType visit(SqlLiteral literal)
        {
            return literal.createSqlType(typeFactory);
        }

        public RelDataType visit(SqlCall call)
        {
            final SqlOperator operator = call.getOperator();
            return operator.deriveType(SqlValidatorImpl.this, scope, call);
        }

        public RelDataType visit(SqlNodeList nodeList)
        {
            // Operand is of a type that we can't derive a type for. If the
            // operand is of a peculiar type, such as a SqlNodeList, then you
            // should override the operator's validateCall() method so that it
            // doesn't try to validate that operand as an expression.
            throw Util.needToImplement(nodeList);
        }

        public RelDataType visit(SqlIdentifier id)
        {
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
                id = scope.fullyQualify(id);
            }
            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 9-June-2005: The name resolution rules used
                    // here are supposed to match SQL:2003 Part 2 Section 6.6
                    // (identifier chain), but we don't currently have enough
                    // information to get everything right.  In particular,
                    // routine parameters are currently looked up via resolve;
                    // we could do a better job if they were looked up via
                    // resolveColumn.

                    // TODO jvs 9-June-2005:  Support schema-qualified table
                    // names here (FRG-140).  This was illegal in SQL-92, but
                    // became legal in SQL:1999.  (SQL:2003 Part 2 Section
                    // 6.6 Syntax Rule 8.b.vi)
                    Util.discard(Bug.Frg140Fixed);

                    SqlValidatorNamespace resolvedNs =
                        scope.resolve(name, null, null);

                    if (resolvedNs != null) {
                        // There's a namespace with the name we seek.
                        type = resolvedNs.getRowType();
                    }

                    // Give precedence to namespace found, unless there
                    // are no more identifier components.
                    if ((type == null) || (id.names.length == 1)) {
                        // See if there's a column with the name we seek in
                        // precisely one of the namespaces in this scope.
                        RelDataType colType = scope.resolveColumn(name, id);
                        if (colType != null) {
                            type = colType;
                        }
                    }

                    if (type == null) {
                        throw newValidationError(
                            id.getComponent(i),
                            EigenbaseResource.instance().UnknownIdentifier.ex(
                                name));
                    }
                } else {
                    RelDataType fieldType =
                        SqlValidatorUtil.lookupFieldType(type, name);
                    if (fieldType == null) {
                        throw newValidationError(
                            id.getComponent(i),
                            EigenbaseResource.instance().UnknownField.ex(name));
                    }
                    type = fieldType;
                }
            }
            type =
                SqlTypeUtil.addCharsetAndCollation(
                    type,
                    getTypeFactory());
            return type;
        }

        public RelDataType visit(SqlDataTypeSpec dataType)
        {
            // Q. How can a data type have a type?
            // A. When it appears in an expression. (Say as the 2nd arg to the
            //    CAST operator.)
            validateDataType(dataType);
            return dataType.deriveType(SqlValidatorImpl.this);
        }

        public RelDataType visit(SqlDynamicParam param)
        {
            return unknownType;
        }

        public RelDataType visit(SqlIntervalQualifier intervalQualifier)
        {
            return typeFactory.createSqlIntervalType(intervalQualifier);
        }
    }

    /**
     * Converts an expression into canonical form by fully-qualifying any
     * identifiers.
     */
    private static class Expander
        extends SqlScopedShuttle
    {
        private final SqlValidatorImpl validator;

        public Expander(
            SqlValidatorImpl validator,
            SqlValidatorScope scope)
        {
            super(scope);
            this.validator = validator;
        }

        public SqlNode visit(SqlIdentifier id)
        {
            // First check for builtin functions which don't have
            // parentheses, like "LOCALTIME".
            SqlCall call =
                SqlUtil.makeCall(
                    validator.getOperatorTable(),
                    id);
            if (call != null) {
                return call.accept(this);
            }
            final SqlIdentifier fqId = getScope().fullyQualify(id);
            validator.setOriginal(fqId, id);
            return fqId;
        }

        // implement SqlScopedShuttle
        protected SqlNode visitScoped(SqlCall call)
        {
            // Only visits arguments which are expressions. We don't want to
            // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
            ArgHandler<SqlNode> argHandler =
                new CallCopyingArgHandler(call, false);
            call.getOperator().acceptCall(this, call, true, argHandler);
            return argHandler.result();
        }
    }

    /**
     * Shuttle which walks over an expression in the ORDER BY clause, replacing
     * usages of aliases with the underlying expression.
     */
    class OrderExpressionExpander
        extends SqlScopedShuttle
    {
        private final List<String> aliasList;
        private final SqlSelect select;
        private final SqlNode root;

        OrderExpressionExpander(SqlSelect select, SqlNode root)
        {
            super(getOrderScope(select));
            this.select = select;
            this.root = root;
            this.aliasList = getNamespace(select).getRowType().getFieldNames();
        }

        public SqlNode go()
        {
            return root.accept(this);
        }

        public SqlNode visit(SqlLiteral literal)
        {
            // Ordinal markers, e.g. 'select a, b from t order by 2'.
            // Only recognize them if they are the whole expression,
            // and if the dialect permits.
            if ((literal == root)
                && getConformance().isSortByOrdinal())
            {
                if ((literal.getTypeName() == SqlTypeName.DECIMAL)
                    || (literal.getTypeName() == SqlTypeName.DOUBLE))
                {
                    final int intValue = literal.intValue(false);
                    if (intValue >= 0) {
                        if ((intValue < 1) || (intValue > aliasList.size())) {
                            throw newValidationError(
                                literal,
                                EigenbaseResource.instance()
                                .OrderByOrdinalOutOfRange.ex());
                        }

                        // SQL ordinals are 1-based, but SortRel's are 0-based
                        int ordinal = intValue - 1;
                        return nthSelectItem(
                            ordinal,
                            literal.getParserPosition());
                    }
                }
            }

            return super.visit(literal);
        }

        /**
         * Returns the <code>ordinal</code>th item in the select list.
         */
        private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos)
        {
            // TODO: Don't expand the list every time. Maybe keep an expanded
            // version of each expression -- select lists and identifiers -- in
            // the validator.

            SqlNodeList expandedSelectList =
                expandStar(
                    select.getSelectList(),
                    select,
                    false);
            SqlNode expr = expandedSelectList.get(ordinal);
            if (expr instanceof SqlCall) {
                SqlCall call = (SqlCall) expr;
                if (call.getOperator() == SqlStdOperatorTable.asOperator) {
                    expr = call.operands[0];
                }
            }
            if (expr instanceof SqlIdentifier) {
                expr = getScope().fullyQualify((SqlIdentifier) expr);
            }

            // Create a copy of the expression with the position of the order
            // item.
            return expr.clone(pos);
        }

        public SqlNode visit(SqlIdentifier id)
        {
            // Aliases, e.g. 'select a as x, b from t order by x'.
            if (id.isSimple()
                && getConformance().isSortByAlias())
            {
                String alias = id.getSimple();
                final SqlValidatorNamespace selectNs = getNamespace(select);
                final RelDataType rowType =
                    selectNs.getRowTypeSansSystemColumns();
                RelDataTypeField field =
                    SqlValidatorUtil.lookupField(rowType, alias);
                if (field != null) {
                    return nthSelectItem(
                        field.getIndex(),
                        id.getParserPosition());
                }
            }

            // No match. Return identifier unchanged.
            return getScope().fullyQualify(id);
        }

        protected SqlNode visitScoped(SqlCall call)
        {
            // Don't attempt to expand sub-queries. We haven't implemented
            // these yet.
            if (call instanceof SqlSelect) {
                return call;
            }
            return super.visitScoped(call);
        }
    }

    protected static class IdInfo
    {
        public final SqlValidatorScope scope;
        public final SqlIdentifier id;

        public IdInfo(SqlValidatorScope scope, SqlIdentifier id)
        {
            this.scope = scope;
            this.id = id;
        }
    }

    /**
     * Utility object used to maintain information about the parameters in a
     * function call.
     */
    protected static class FunctionParamInfo
    {
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

        public FunctionParamInfo()
        {
            cursorPosToSelectMap = new HashMap<Integer, SqlSelect>();
            columnListParamToParentCursorMap = new HashMap<String, String>();
        }
    }
}

// End SqlValidatorImpl.java
