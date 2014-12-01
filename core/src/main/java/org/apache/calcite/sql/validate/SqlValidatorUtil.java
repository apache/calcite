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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods related to validation.
 */
public class SqlValidatorUtil {
  private SqlValidatorUtil() {}

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a {@link SqlValidatorScope} into a {@link RelOptTable}. This is
   * only possible if the scope represents an identifier, such as "sales.emp".
   * Otherwise, returns null.
   *
   * @param namespace     Namespace
   * @param catalogReader Schema
   * @param datasetName   Name of sample dataset to substitute, or null to use
   *                      the regular table
   * @param usedDataset   Output parameter which is set to true if a sample
   *                      dataset is found; may be null
   */
  public static RelOptTable getRelOptTable(
      SqlValidatorNamespace namespace,
      Prepare.CatalogReader catalogReader,
      String datasetName,
      boolean[] usedDataset) {
    if (namespace.isWrapperFor(TableNamespace.class)) {
      TableNamespace tableNamespace =
          namespace.unwrap(TableNamespace.class);
      final List<String> names = tableNamespace.getTable().getQualifiedName();
      if ((datasetName != null)
          && (catalogReader instanceof RelOptSchemaWithSampling)) {
        return ((RelOptSchemaWithSampling) catalogReader)
            .getTableForMember(
                names,
                datasetName,
                usedDataset);
      } else {
        // Schema does not support substitution. Ignore the dataset,
        // if any.
        return catalogReader.getTableForMember(names);
      }
    } else {
      return null;
    }
  }

  /**
   * Looks up a field with a given name, returning null if not found.
   *
   * @param rowType    Row type
   * @param columnName Field name
   * @return Field, or null if not found
   */
  public static RelDataTypeField lookupField(
      boolean caseSensitive,
      final RelDataType rowType,
      String columnName) {
    return rowType.getField(columnName, caseSensitive);
  }

  public static void checkCharsetAndCollateConsistentIfCharType(
      RelDataType type) {
    //(every charset must have a default collation)
    if (SqlTypeUtil.inCharFamily(type)) {
      Charset strCharset = type.getCharset();
      Charset colCharset = type.getCollation().getCharset();
      assert null != strCharset;
      assert null != colCharset;
      if (!strCharset.equals(colCharset)) {
        if (false) {
          // todo: enable this checking when we have a charset to
          //   collation mapping
          throw new Error(type.toString()
              + " was found to have charset '" + strCharset.name()
              + "' and a mismatched collation charset '"
              + colCharset.name() + "'");
        }
      }
    }
  }

  /**
   * Converts an expression "expr" into "expr AS alias".
   */
  public static SqlNode addAlias(
      SqlNode expr,
      String alias) {
    final SqlParserPos pos = expr.getParserPosition();
    final SqlIdentifier id = new SqlIdentifier(alias, pos);
    return SqlStdOperatorTable.AS.createCall(pos, expr, id);
  }

  /**
   * Derives an alias for a node, and invents a mangled identifier if it
   * cannot.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Alias: "1 + 2 as foo" yields "foo"
   * <li>Identifier: "foo.bar.baz" yields "baz"
   * <li>Anything else yields "expr$<i>ordinal</i>"
   * </ul>
   *
   * @return An alias, if one can be derived; or a synthetic alias
   * "expr$<i>ordinal</i>" if ordinal &lt; 0; otherwise null
   */
  public static String getAlias(SqlNode node, int ordinal) {
    switch (node.getKind()) {
    case AS:
      // E.g. "1 + 2 as foo" --> "foo"
      return ((SqlCall) node).operand(1).toString();

    case OVER:
      // E.g. "bids over w" --> "bids"
      return getAlias(((SqlCall) node).operand(0), ordinal);

    case IDENTIFIER:
      // E.g. "foo.bar" --> "bar"
      return Util.last(((SqlIdentifier) node).names);

    default:
      if (ordinal < 0) {
        return null;
      } else {
        return SqlUtil.deriveAliasFromOrdinal(ordinal);
      }
    }
  }

  /**
   * Makes a name distinct from other names which have already been used, adds
   * it to the list, and returns it.
   *
   * @param name      Suggested name, may not be unique
   * @param nameList  Collection of names already used
   * @param suggester Base for name when input name is null
   * @return Unique name
   */
  public static String uniquify(
      String name,
      Set<String> nameList,
      Suggester suggester) {
    if (name != null) {
      if (nameList.add(name)) {
        return name;
      }
    }
    final String originalName = name;
    for (int j = 0;; j++) {
      name = suggester.apply(originalName, j, nameList.size());
      if (nameList.add(name)) {
        return name;
      }
    }
  }

  /**
   * Factory method for {@link SqlValidator}.
   */
  public static SqlValidatorWithHints newValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
    return new SqlValidatorImpl(
        opTab,
        catalogReader,
        typeFactory,
        SqlConformance.DEFAULT);
  }

  /**
   * Makes sure that the names in a list are unique.
   *
   * <p>Does not modify the input list. Returns the input list if the strings
   * are unique, otherwise allocates a new list.
   *
   * @param nameList List of strings
   * @return List of unique strings
   */
  public static List<String> uniquify(List<String> nameList) {
    return uniquify(nameList, EXPR_SUGGESTER);
  }

  public static List<String> uniquify(
      List<String> nameList,
      Suggester suggester) {
    Set<String> used = new LinkedHashSet<String>();
    int changeCount = 0;
    for (String name : nameList) {
      String uniqueName = uniquify(name, used, suggester);
      if (!uniqueName.equals(name)) {
        ++changeCount;
      }
    }
    return changeCount == 0
        ? nameList
        : new ArrayList<String>(used);
  }

  /**
   * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
   * namespace. The returned namespace, never null, may represent a
   * schema, table, column, etc.
   */
  public static SqlValidatorNamespace lookup(
      SqlValidatorScope scope,
      List<String> names) {
    assert names.size() > 0;
    SqlValidatorNamespace namespace = null;
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      if (i == 0) {
        namespace = scope.resolve(name, null, null);
      } else {
        namespace = namespace.lookupChild(name);
      }
    }
    assert namespace != null;
    return namespace;
  }

  public static void getSchemaObjectMonikers(
      SqlValidatorCatalogReader catalogReader,
      List<String> names,
      List<SqlMoniker> hints) {
    // Assume that the last name is 'dummy' or similar.
    List<String> subNames = Util.skipLast(names);
    hints.addAll(catalogReader.getAllSchemaObjectNames(subNames));

    // If the name has length 0, try prepending the name of the default
    // schema. So, the empty name would yield a list of tables in the
    // default schema, as well as a list of schemas from the above code.
    if (subNames.size() == 0) {
      hints.addAll(
          catalogReader.getAllSchemaObjectNames(
              catalogReader.getSchemaName()));
    }
  }

  public static SelectScope getEnclosingSelectScope(SqlValidatorScope scope) {
    while (scope instanceof DelegatingScope) {
      if (scope instanceof SelectScope) {
        return (SelectScope) scope;
      }
      scope = ((DelegatingScope) scope).getParent();
    }
    return null;
  }

  public static AggregatingSelectScope
  getEnclosingAggregateSelectScope(SqlValidatorScope scope) {
    while (scope instanceof DelegatingScope) {
      if (scope instanceof AggregatingSelectScope) {
        return (AggregatingSelectScope) scope;
      }
      scope = ((DelegatingScope) scope).getParent();
    }
    return null;
  }

  /**
   * Derives the list of column names suitable for NATURAL JOIN. These are the
   * columns that occur exactly once on each side of the join.
   *
   * @param leftRowType  Row type of left input to the join
   * @param rightRowType Row type of right input to the join
   * @return List of columns that occur once on each side
   */
  public static List<String> deriveNaturalJoinColumnList(
      RelDataType leftRowType,
      RelDataType rightRowType) {
    List<String> naturalColumnNames = new ArrayList<String>();
    final List<String> leftNames = leftRowType.getFieldNames();
    final List<String> rightNames = rightRowType.getFieldNames();
    for (String name : leftNames) {
      if ((Collections.frequency(leftNames, name) == 1)
          && (Collections.frequency(rightNames, name) == 1)) {
        naturalColumnNames.add(name);
      }
    }
    return naturalColumnNames;
  }

  public static RelDataType createTypeFromProjection(RelDataType type,
      List<String> columnNameList, RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    // If the names in columnNameList and type have case-sensitive differences,
    // the resulting type will use those from type. These are presumably more
    // canonical.
    final List<RelDataTypeField> fields =
        new ArrayList<RelDataTypeField>(columnNameList.size());
    for (String name : columnNameList) {
      RelDataTypeField field = type.getField(name, caseSensitive);
      fields.add(type.getFieldList().get(field.getIndex()));
    }
    return typeFactory.createStructType(fields);
  }

  /** Analyzes an expression in a GROUP BY clause.
   *
   * <p>It may be an expression, an empty list (), or a call to
   * {@code GROUPING SETS}, {@code CUBE} or {@code ROLLUP}.
   *
   * <p>Each group item produces a list of group sets, which are written to
   * {@code topBuilder}. To find the grouping sets of the query, we will take
   * the cartesian product of the group sets. */
  public static void analyzeGroupItem(SqlValidatorScope scope,
      List<SqlNode> groupExprs, Map<Integer, Integer> groupExprProjection,
      ImmutableList.Builder<ImmutableList<ImmutableBitSet>> topBuilder,
      SqlNode groupExpr) {
    final ImmutableList.Builder<ImmutableBitSet> builder;
    switch (groupExpr.getKind()) {
    case CUBE:
    case ROLLUP:
      // E.g. ROLLUP(a, (b, c)) becomes [{0}, {1, 2}]
      // then we roll up to [(0, 1, 2), (0), ()]  -- note no (0, 1)
      List<ImmutableBitSet> bitSets =
          analyzeGroupTuple(scope, groupExprs,
              groupExprProjection, ((SqlCall) groupExpr).getOperandList());
      switch (groupExpr.getKind()) {
      case ROLLUP:
        topBuilder.add(rollup(bitSets));
        return;
      default:
        topBuilder.add(cube(bitSets));
        return;
      }
    case OTHER:
      if (groupExpr instanceof SqlNodeList) {
        SqlNodeList list = (SqlNodeList) groupExpr;
        for (SqlNode node : list) {
          analyzeGroupItem(scope, groupExprs, groupExprProjection, topBuilder,
              node);
        }
        return;
      }
      // fall through
    case GROUPING_SETS:
    default:
      builder = ImmutableList.builder();
      convertGroupSet(scope, groupExprs, groupExprProjection, builder,
          groupExpr);
      topBuilder.add(builder.build());
    }
  }

  /** Analyzes a GROUPING SETS item in a GROUP BY clause. */
  private static void convertGroupSet(SqlValidatorScope scope,
      List<SqlNode> groupExprs, Map<Integer, Integer> groupExprProjection,
      ImmutableList.Builder<ImmutableBitSet> builder, SqlNode groupExpr) {
    switch (groupExpr.getKind()) {
    case GROUPING_SETS:
      final SqlCall call = (SqlCall) groupExpr;
      for (SqlNode node : call.getOperandList()) {
        convertGroupSet(scope, groupExprs, groupExprProjection, builder, node);
      }
      return;
    case ROW:
      final List<ImmutableBitSet> bitSets =
          analyzeGroupTuple(scope, groupExprs, groupExprProjection,
              ((SqlCall) groupExpr).getOperandList());
      builder.add(ImmutableBitSet.union(bitSets));
      return;
    default:
      builder.add(
          analyzeGroupExpr(scope, groupExprs, groupExprProjection, groupExpr));
      return;
    }
  }

  /** Analyzes a tuple in a GROUPING SETS clause.
   *
   * <p>For example, in {@code GROUP BY GROUPING SETS ((a, b), a, c)},
   * {@code (a, b)} is a tuple.
   *
   * <p>Gathers into {@code groupExprs} the set of distinct expressions being
   * grouped, and returns a bitmap indicating which expressions this tuple
   * is grouping. */
  private static List<ImmutableBitSet>
  analyzeGroupTuple(SqlValidatorScope scope, List<SqlNode> groupExprs,
      Map<Integer, Integer> groupExprProjection, List<SqlNode> operandList) {
    List<ImmutableBitSet> list = Lists.newArrayList();
    for (SqlNode operand : operandList) {
      list.add(
          analyzeGroupExpr(scope, groupExprs, groupExprProjection, operand));
    }
    return list;
  }

  /** Analyzes a component of a tuple in a GROUPING SETS clause. */
  private static ImmutableBitSet analyzeGroupExpr(SqlValidatorScope scope,
      List<SqlNode> groupExprs, Map<Integer, Integer> groupExprProjection,
      SqlNode groupExpr) {
    final SqlNode expandedGroupExpr =
        scope.getValidator().expand(groupExpr, scope);

    switch (expandedGroupExpr.getKind()) {
    case ROW:
      return ImmutableBitSet.union(
          analyzeGroupTuple(scope, groupExprs, groupExprProjection,
              ((SqlCall) expandedGroupExpr).getOperandList()));
    case OTHER:
      if (expandedGroupExpr instanceof SqlNodeList
          && ((SqlNodeList) expandedGroupExpr).size() == 0) {
        return ImmutableBitSet.of();
      }
    }

    final int ref = lookupGroupExpr(groupExprs, groupExpr);
    if (expandedGroupExpr instanceof SqlIdentifier) {
      // SQL 2003 does not allow expressions of column references
      SqlIdentifier expr = (SqlIdentifier) expandedGroupExpr;

      // column references should be fully qualified.
      assert expr.names.size() == 2;
      String originalRelName = expr.names.get(0);
      String originalFieldName = expr.names.get(1);

      int[] nsIndexes = {-1};
      final SqlValidatorScope[] ancestorScopes = {null};
      SqlValidatorNamespace foundNs =
          scope.resolve(
              originalRelName,
              ancestorScopes,
              nsIndexes);

      assert foundNs != null;
      assert nsIndexes.length == 1;
      int childNamespaceIndex = nsIndexes[0];

      int namespaceOffset = 0;

      if (childNamespaceIndex > 0) {
        // If not the first child, need to figure out the width of
        // output types from all the preceding namespaces
        assert ancestorScopes[0] instanceof ListScope;
        List<SqlValidatorNamespace> children =
            ((ListScope) ancestorScopes[0]).getChildren();

        for (int j = 0; j < childNamespaceIndex; j++) {
          namespaceOffset +=
              children.get(j).getRowType().getFieldCount();
        }
      }

      RelDataTypeField field =
          scope.getValidator().getCatalogReader().field(foundNs.getRowType(),
              originalFieldName);
      int origPos = namespaceOffset + field.getIndex();

      groupExprProjection.put(origPos, ref);
    }

    return ImmutableBitSet.of(ref);
  }

  private static int lookupGroupExpr(List<SqlNode> groupExprs, SqlNode expr) {
    for (Ord<SqlNode> node : Ord.zip(groupExprs)) {
      if (node.e.equalsDeep(expr, false)) {
        return node.i;
      }
    }
    groupExprs.add(expr);
    return groupExprs.size() - 1;
  }

  /** Computes the rollup of bit sets.
   *
   * <p>For example, <code>rollup({0}, {1})</code>
   * returns <code>({0, 1}, {0}, {})</code>.
   *
   * <p>Bit sets are not necessarily singletons:
   * <code>rollup({0, 2}, {3, 5})</code>
   * returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>. */
  @VisibleForTesting
  public static ImmutableList<ImmutableBitSet>
  rollup(List<ImmutableBitSet> bitSets) {
    Set<ImmutableBitSet> builder = Sets.newLinkedHashSet();
    for (;;) {
      final ImmutableBitSet union = ImmutableBitSet.union(bitSets);
      builder.add(union);
      if (union.isEmpty()) {
        break;
      }
      bitSets = bitSets.subList(0, bitSets.size() - 1);
    }
    return ImmutableList.copyOf(builder);
  }

  /** Computes the cube of bit sets.
   *
   * <p>For example,  <code>rollup({0}, {1})</code>
   * returns <code>({0, 1}, {0}, {})</code>.
   *
   * <p>Bit sets are not necessarily singletons:
   * <code>rollup({0, 2}, {3, 5})</code>
   * returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>. */
  @VisibleForTesting
  public static ImmutableList<ImmutableBitSet>
  cube(List<ImmutableBitSet> bitSets) {
    // Given the bit sets [{1}, {2, 3}, {5}],
    // form the lists [[{1}, {}], [{2, 3}, {}], [{5}, {}]].
    final Set<List<ImmutableBitSet>> builder = Sets.newLinkedHashSet();
    for (ImmutableBitSet bitSet : bitSets) {
      builder.add(Arrays.asList(bitSet, ImmutableBitSet.of()));
    }
    Set<ImmutableBitSet> flattenedBitSets = Sets.newLinkedHashSet();
    for (List<ImmutableBitSet> o : Linq4j.product(builder)) {
      flattenedBitSets.add(ImmutableBitSet.union(o));
    }
    return ImmutableList.copyOf(flattenedBitSets);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, copying every node, and fully-qualifying every
   * identifier.
   */
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
    }

    /** Copies a list of nodes. */
    public static SqlNodeList copy(SqlValidatorScope scope, SqlNodeList list) {
      return (SqlNodeList) list.accept(new DeepCopier(scope));
    }

    public SqlNode visit(SqlNodeList list) {
      SqlNodeList copy = new SqlNodeList(list.getParserPosition());
      for (SqlNode node : list) {
        copy.add(node.accept(this));
      }
      return copy;
    }

    // Override to copy all arguments regardless of whether visitor changes
    // them.
    protected SqlNode visitScoped(SqlCall call) {
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return argHandler.result();
    }

    public SqlNode visit(SqlLiteral literal) {
      return (SqlNode) literal.clone();
    }

    public SqlNode visit(SqlIdentifier id) {
      return getScope().fullyQualify(id);
    }

    public SqlNode visit(SqlDataTypeSpec type) {
      return (SqlNode) type.clone();
    }

    public SqlNode visit(SqlDynamicParam param) {
      return (SqlNode) param.clone();
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return (SqlNode) intervalQualifier.clone();
    }
  }

  /** Suggests candidates for unique names, given the number of attempts so far
   * and the number of expressions in the project list. */
  interface Suggester {
    String apply(String original, int attempt, int size);
  }

  public static final Suggester EXPR_SUGGESTER =
      new Suggester() {
        public String apply(String original, int attempt, int size) {
          return Util.first(original, "EXPR$") + attempt;
        }
      };

  public static final Suggester F_SUGGESTER =
      new Suggester() {
        public String apply(String original, int attempt, int size) {
          return Util.first(original, "$f") + Math.max(size, attempt);
        }
      };
}

// End SqlValidatorUtil.java
