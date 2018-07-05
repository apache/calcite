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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
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
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.util.Static.RESOURCE;

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
      final TableNamespace tableNamespace =
          namespace.unwrap(TableNamespace.class);
      return getRelOptTable(tableNamespace, catalogReader, datasetName, usedDataset,
          tableNamespace.extendedFields);
    } else if (namespace.isWrapperFor(SqlValidatorImpl.DmlNamespace.class)) {
      final SqlValidatorImpl.DmlNamespace dmlNamespace = namespace.unwrap(
          SqlValidatorImpl.DmlNamespace.class);
      final SqlValidatorNamespace resolvedNamespace = dmlNamespace.resolve();
      if (resolvedNamespace.isWrapperFor(TableNamespace.class)) {
        final TableNamespace tableNamespace = resolvedNamespace.unwrap(TableNamespace.class);
        final SqlValidatorTable validatorTable = tableNamespace.getTable();
        final RelDataTypeFactory typeFactory = catalogReader.getTypeFactory();
        final List<RelDataTypeField> extendedFields = dmlNamespace.extendList == null
            ? ImmutableList.of()
            : getExtendedColumns(typeFactory, validatorTable, dmlNamespace.extendList);
        return getRelOptTable(
            tableNamespace, catalogReader, datasetName, usedDataset, extendedFields);
      }
    }
    return null;
  }

  private static RelOptTable getRelOptTable(
      TableNamespace tableNamespace,
      Prepare.CatalogReader catalogReader,
      String datasetName,
      boolean[] usedDataset,
      List<RelDataTypeField> extendedFields) {
    final List<String> names = tableNamespace.getTable().getQualifiedName();
    RelOptTable table;
    if (datasetName != null
        && catalogReader instanceof RelOptSchemaWithSampling) {
      final RelOptSchemaWithSampling reader =
          (RelOptSchemaWithSampling) catalogReader;
      table = reader.getTableForMember(names, datasetName, usedDataset);
    } else {
      // Schema does not support substitution. Ignore the data set, if any.
      table = catalogReader.getTableForMember(names);
    }
    if (!extendedFields.isEmpty()) {
      table = table.extend(extendedFields);
    }
    return table;
  }

  /**
   * Gets a list of extended columns with field indices to the underlying table.
   */
  public static List<RelDataTypeField> getExtendedColumns(
      RelDataTypeFactory typeFactory, SqlValidatorTable table, SqlNodeList extendedColumns) {
    final ImmutableList.Builder<RelDataTypeField> extendedFields =
        ImmutableList.builder();
    final ExtensibleTable extTable = table.unwrap(ExtensibleTable.class);
    int extendedFieldOffset =
        extTable == null
            ? table.getRowType().getFieldCount()
            : extTable.getExtendedColumnOffset();
    for (final Pair<SqlIdentifier, SqlDataTypeSpec> pair : pairs(extendedColumns)) {
      final SqlIdentifier identifier = pair.left;
      final SqlDataTypeSpec type = pair.right;
      extendedFields.add(
          new RelDataTypeFieldImpl(identifier.toString(),
              extendedFieldOffset++,
              type.deriveType(typeFactory)));
    }
    return extendedFields.build();
  }

  /** Converts a list of extended columns
   * (of the form [name0, type0, name1, type1, ...])
   * into a list of (name, type) pairs. */
  private static List<Pair<SqlIdentifier, SqlDataTypeSpec>> pairs(
      SqlNodeList extendedColumns) {
    final List list = extendedColumns.getList();
    //noinspection unchecked
    return Pair.zip(Util.quotientList(list, 2, 0),
        Util.quotientList(list, 2, 1));
  }

  /**
   * Gets a map of indexes from the source to fields in the target for the
   * intersecting set of source and target fields.
   *
   * @param sourceFields The source of column names that determine indexes
   * @param targetFields The target fields to be indexed
   */
  public static ImmutableMap<Integer, RelDataTypeField> getIndexToFieldMap(
      List<RelDataTypeField> sourceFields,
      RelDataType targetFields) {
    final ImmutableMap.Builder<Integer, RelDataTypeField> output =
        ImmutableMap.builder();
    for (final RelDataTypeField source : sourceFields) {
      final RelDataTypeField target = targetFields.getField(source.getName(), true, false);
      if (target != null) {
        output.put(source.getIndex(), target);
      }
    }
    return output.build();
  }

  /**
   * Gets the bit-set to the column ordinals in the source for columns that intersect in the target.
   * @param sourceRowType The source upon which to ordinate the bit set.
   * @param targetRowType The target to overlay on the source to create the bit set.
   */
  public static ImmutableBitSet getOrdinalBitSet(
      RelDataType sourceRowType, RelDataType targetRowType) {
    Map<Integer, RelDataTypeField> indexToField =
        getIndexToFieldMap(sourceRowType.getFieldList(), targetRowType);
    return getOrdinalBitSet(sourceRowType, indexToField);
  }

  /**
   * Gets the bit-set to the column ordinals in the source for columns that
   * intersect in the target.
   *
   * @param sourceRowType The source upon which to ordinate the bit set.
   * @param indexToField  The map of ordinals to target fields.
   */
  public static ImmutableBitSet getOrdinalBitSet(
      RelDataType sourceRowType,
      Map<Integer, RelDataTypeField> indexToField) {
    ImmutableBitSet source = ImmutableBitSet.of(
        Lists.transform(sourceRowType.getFieldList(),
            RelDataTypeField::getIndex));
    ImmutableBitSet target =
        ImmutableBitSet.of(indexToField.keySet());
    return source.intersect(target);
  }

  /** Returns a map from field names to indexes. */
  public static Map<String, Integer> mapNameToIndex(List<RelDataTypeField> fields) {
    ImmutableMap.Builder<String, Integer> output = ImmutableMap.builder();
    for (RelDataTypeField field : fields) {
      output.put(field.getName(), field.getIndex());
    }
    return output.build();
  }

  @Deprecated // to be removed before 2.0
  public static RelDataTypeField lookupField(boolean caseSensitive,
      final RelDataType rowType, String columnName) {
    return rowType.getField(columnName, caseSensitive, false);
  }

  public static void checkCharsetAndCollateConsistentIfCharType(
      RelDataType type) {
    // (every charset must have a default collation)
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
   * Checks that there are no duplicates in a list of {@link SqlIdentifier}.
   */
  static void checkIdentifierListForDuplicates(List<SqlNode> columnList,
      SqlValidatorImpl.ValidationErrorFunction validationErrorFunction) {
    final List<List<String>> names = Lists.transform(columnList,
        o -> ((SqlIdentifier) o).names);
    final int i = Util.firstDuplicate(names);
    if (i >= 0) {
      throw validationErrorFunction.apply(columnList.get(i),
          RESOURCE.duplicateNameInColumnList(Util.last(names.get(i))));
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
   * Factory method for {@link SqlValidator}.
   */
  public static SqlValidatorWithHints newValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    return new SqlValidatorImpl(opTab, catalogReader, typeFactory,
        conformance);
  }

  /**
   * Factory method for {@link SqlValidator}, with default conformance.
   */
  @Deprecated // to be removed before 2.0
  public static SqlValidatorWithHints newValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
    return newValidator(opTab, catalogReader, typeFactory,
        SqlConformanceEnum.DEFAULT);
  }

  /**
   * Makes a name distinct from other names which have already been used, adds
   * it to the list, and returns it.
   *
   * @param name      Suggested name, may not be unique
   * @param usedNames  Collection of names already used
   * @param suggester Base for name when input name is null
   * @return Unique name
   */
  public static String uniquify(String name, Set<String> usedNames,
      Suggester suggester) {
    if (name != null) {
      if (usedNames.add(name)) {
        return name;
      }
    }
    final String originalName = name;
    for (int j = 0;; j++) {
      name = suggester.apply(originalName, j, usedNames.size());
      if (usedNames.add(name)) {
        return name;
      }
    }
  }

  /**
   * Makes sure that the names in a list are unique.
   *
   * <p>Does not modify the input list. Returns the input list if the strings
   * are unique, otherwise allocates a new list. Deprecated in favor of caseSensitive
   * aware version.
   *
   * @param nameList List of strings
   * @return List of unique strings
   */
  @Deprecated // to be removed before 2.0
  public static List<String> uniquify(List<String> nameList) {
    return uniquify(nameList, EXPR_SUGGESTER, true);
  }


  /**
   * Makes sure that the names in a list are unique.
   *
   * <p>Does not modify the input list. Returns the input list if the strings
   * are unique, otherwise allocates a new list.
   *
   * @deprecated Use {@link #uniquify(List, Suggester, boolean)}
   *
   * @param nameList List of strings
   * @param suggester How to generate new names if duplicate names are found
   * @return List of unique strings
   */
  @Deprecated // to be removed before 2.0
  public static List<String> uniquify(List<String> nameList, Suggester suggester) {
    return uniquify(nameList, suggester, true);
  }

  /**
   * Makes sure that the names in a list are unique.
   *
   * <p>Does not modify the input list. Returns the input list if the strings
   * are unique, otherwise allocates a new list.
   *
   * @param nameList List of strings
   * @param caseSensitive Whether upper and lower case names are considered
   *     distinct
   * @return List of unique strings
   */
  public static List<String> uniquify(List<String> nameList,
      boolean caseSensitive) {
    return uniquify(nameList, EXPR_SUGGESTER, caseSensitive);
  }

  /**
   * Makes sure that the names in a list are unique.
   *
   * <p>Does not modify the input list. Returns the input list if the strings
   * are unique, otherwise allocates a new list.
   *
   * @param nameList List of strings
   * @param suggester How to generate new names if duplicate names are found
   * @param caseSensitive Whether upper and lower case names are considered
   *     distinct
   * @return List of unique strings
   */
  public static List<String> uniquify(
      List<String> nameList,
      Suggester suggester,
      boolean caseSensitive) {
    final Set<String> used = caseSensitive
        ? new LinkedHashSet<>()
        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    int changeCount = 0;
    final List<String> newNameList = new ArrayList<>();
    for (String name : nameList) {
      String uniqueName = uniquify(name, used, suggester);
      if (!uniqueName.equals(name)) {
        ++changeCount;
      }
      newNameList.add(uniqueName);
    }
    return changeCount == 0
        ? nameList
        : newNameList;
  }

  /**
   * Derives the type of a join relational expression.
   *
   * @param leftType        Row type of left input to join
   * @param rightType       Row type of right input to join
   * @param joinType        Type of join
   * @param typeFactory     Type factory
   * @param fieldNameList   List of names of fields; if null, field names are
   *                        inherited and made unique
   * @param systemFieldList List of system fields that will be prefixed to
   *                        output row type; typically empty but must not be
   *                        null
   * @return join type
   */
  public static RelDataType deriveJoinRowType(
      RelDataType leftType,
      RelDataType rightType,
      JoinRelType joinType,
      RelDataTypeFactory typeFactory,
      List<String> fieldNameList,
      List<RelDataTypeField> systemFieldList) {
    assert systemFieldList != null;
    switch (joinType) {
    case LEFT:
      rightType = typeFactory.createTypeWithNullability(rightType, true);
      break;
    case RIGHT:
      leftType = typeFactory.createTypeWithNullability(leftType, true);
      break;
    case FULL:
      leftType = typeFactory.createTypeWithNullability(leftType, true);
      rightType = typeFactory.createTypeWithNullability(rightType, true);
      break;
    default:
      break;
    }
    return createJoinType(typeFactory, leftType, rightType, fieldNameList,
        systemFieldList);
  }

  /**
   * Returns the type the row which results when two relations are joined.
   *
   * <p>The resulting row type consists of
   * the system fields (if any), followed by
   * the fields of the left type, followed by
   * the fields of the right type. The field name list, if present, overrides
   * the original names of the fields.
   *
   * @param typeFactory     Type factory
   * @param leftType        Type of left input to join
   * @param rightType       Type of right input to join, or null for semi-join
   * @param fieldNameList   If not null, overrides the original names of the
   *                        fields
   * @param systemFieldList List of system fields that will be prefixed to
   *                        output row type; typically empty but must not be
   *                        null
   * @return type of row which results when two relations are joined
   */
  public static RelDataType createJoinType(
      RelDataTypeFactory typeFactory,
      RelDataType leftType,
      RelDataType rightType,
      List<String> fieldNameList,
      List<RelDataTypeField> systemFieldList) {
    assert (fieldNameList == null)
        || (fieldNameList.size()
        == (systemFieldList.size()
        + leftType.getFieldCount()
        + rightType.getFieldCount()));
    List<String> nameList = new ArrayList<>();
    final List<RelDataType> typeList = new ArrayList<>();

    // Use a set to keep track of the field names; this is needed
    // to ensure that the contains() call to check for name uniqueness
    // runs in constant time; otherwise, if the number of fields is large,
    // doing a contains() on a list can be expensive.
    final Set<String> uniqueNameList =
        typeFactory.getTypeSystem().isSchemaCaseSensitive()
            ? new HashSet<>()
            : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addFields(systemFieldList, typeList, nameList, uniqueNameList);
    addFields(leftType.getFieldList(), typeList, nameList, uniqueNameList);
    if (rightType != null) {
      addFields(
          rightType.getFieldList(), typeList, nameList, uniqueNameList);
    }
    if (fieldNameList != null) {
      assert fieldNameList.size() == nameList.size();
      nameList = fieldNameList;
    }
    return typeFactory.createStructType(typeList, nameList);
  }

  private static void addFields(List<RelDataTypeField> fieldList,
      List<RelDataType> typeList, List<String> nameList,
      Set<String> uniqueNames) {
    for (RelDataTypeField field : fieldList) {
      String name = field.getName();

      // Ensure that name is unique from all previous field names
      if (uniqueNames.contains(name)) {
        String nameBase = name;
        for (int j = 0;; j++) {
          name = nameBase + j;
          if (!uniqueNames.contains(name)) {
            break;
          }
        }
      }
      nameList.add(name);
      uniqueNames.add(name);
      typeList.add(field.getType());
    }
  }

  /**
   * Resolve a target column name in the target table.
   *
   * @return the target field or null if the name cannot be resolved
   * @param rowType the target row type
   * @param id      the target column identifier
   * @param table   the target table or null if it is not a RelOptTable instance
   */
  public static RelDataTypeField getTargetField(
      RelDataType rowType, RelDataTypeFactory typeFactory,
      SqlIdentifier id, SqlValidatorCatalogReader catalogReader,
      RelOptTable table) {
    final Table t = table == null ? null : table.unwrap(Table.class);
    if (!(t instanceof CustomColumnResolvingTable)) {
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      return nameMatcher.field(rowType, id.getSimple());
    }

    final List<Pair<RelDataTypeField, List<String>>> entries =
        ((CustomColumnResolvingTable) t).resolveColumn(
            rowType, typeFactory, id.names);
    switch (entries.size()) {
    case 1:
      if (!entries.get(0).getValue().isEmpty()) {
        return null;
      }
      return entries.get(0).getKey();
    default:
      return null;
    }
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
    final SqlNameMatcher nameMatcher =
        scope.getValidator().getCatalogReader().nameMatcher();
    final SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();
    scope.resolve(ImmutableList.of(names.get(0)), nameMatcher, false, resolved);
    assert resolved.count() == 1;
    SqlValidatorNamespace namespace = resolved.only().namespace;
    for (String name : Util.skip(names)) {
      namespace = namespace.lookupChild(name);
      assert namespace != null;
    }
    return namespace;
  }

  public static void getSchemaObjectMonikers(
      SqlValidatorCatalogReader catalogReader,
      List<String> names,
      List<SqlMoniker> hints) {
    // Assume that the last name is 'dummy' or similar.
    List<String> subNames = Util.skipLast(names);

    // Try successively with catalog.schema, catalog and no prefix
    for (List<String> x : catalogReader.getSchemaPaths()) {
      final List<String> names2 =
          ImmutableList.<String>builder().addAll(x).addAll(subNames).build();
      hints.addAll(catalogReader.getAllSchemaObjectNames(names2));
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

  public static AggregatingSelectScope getEnclosingAggregateSelectScope(
      SqlValidatorScope scope) {
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
   * @param nameMatcher Whether matches are case-sensitive
   * @param leftRowType  Row type of left input to the join
   * @param rightRowType Row type of right input to the join
   * @return List of columns that occur once on each side
   */
  public static List<String> deriveNaturalJoinColumnList(
      SqlNameMatcher nameMatcher,
      RelDataType leftRowType,
      RelDataType rightRowType) {
    final List<String> naturalColumnNames = new ArrayList<>();
    final List<String> leftNames = leftRowType.getFieldNames();
    final List<String> rightNames = rightRowType.getFieldNames();
    for (String name : leftNames) {
      if (nameMatcher.frequency(leftNames, name) == 1
          && nameMatcher.frequency(rightNames, name) == 1) {
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
        new ArrayList<>(columnNameList.size());
    for (String name : columnNameList) {
      RelDataTypeField field = type.getField(name, caseSensitive, false);
      fields.add(type.getFieldList().get(field.getIndex()));
    }
    return typeFactory.createStructType(fields);
  }

  /** Analyzes an expression in a GROUP BY clause.
   *
   * <p>It may be an expression, an empty list (), or a call to
   * {@code GROUPING SETS}, {@code CUBE}, {@code ROLLUP},
   * {@code TUMBLE}, {@code HOP} or {@code SESSION}.
   *
   * <p>Each group item produces a list of group sets, which are written to
   * {@code topBuilder}. To find the grouping sets of the query, we will take
   * the cartesian product of the group sets. */
  public static void analyzeGroupItem(SqlValidatorScope scope,
      GroupAnalyzer groupAnalyzer,
      ImmutableList.Builder<ImmutableList<ImmutableBitSet>> topBuilder,
      SqlNode groupExpr) {
    final ImmutableList.Builder<ImmutableBitSet> builder;
    switch (groupExpr.getKind()) {
    case CUBE:
    case ROLLUP:
      // E.g. ROLLUP(a, (b, c)) becomes [{0}, {1, 2}]
      // then we roll up to [(0, 1, 2), (0), ()]  -- note no (0, 1)
      List<ImmutableBitSet> bitSets =
          analyzeGroupTuple(scope, groupAnalyzer,
              ((SqlCall) groupExpr).getOperandList());
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
          analyzeGroupItem(scope, groupAnalyzer, topBuilder,
              node);
        }
        return;
      }
      // fall through
    case HOP:
    case TUMBLE:
    case SESSION:
    case GROUPING_SETS:
    default:
      builder = ImmutableList.builder();
      convertGroupSet(scope, groupAnalyzer, builder,
          groupExpr);
      topBuilder.add(builder.build());
    }
  }

  /** Analyzes a GROUPING SETS item in a GROUP BY clause. */
  private static void convertGroupSet(SqlValidatorScope scope,
      GroupAnalyzer groupAnalyzer,
      ImmutableList.Builder<ImmutableBitSet> builder, SqlNode groupExpr) {
    switch (groupExpr.getKind()) {
    case GROUPING_SETS:
      final SqlCall call = (SqlCall) groupExpr;
      for (SqlNode node : call.getOperandList()) {
        convertGroupSet(scope, groupAnalyzer, builder, node);
      }
      return;
    case ROW:
      final List<ImmutableBitSet> bitSets =
          analyzeGroupTuple(scope, groupAnalyzer,
              ((SqlCall) groupExpr).getOperandList());
      builder.add(ImmutableBitSet.union(bitSets));
      return;
    case ROLLUP:
    case CUBE: {
      // GROUPING SETS ( (a), ROLLUP(c,b), CUBE(d,e) )
      // is EQUIVALENT to
      // GROUPING SETS ( (a), (c,b), (b) ,(), (d,e), (d), (e) ).
      // Expand all ROLLUP/CUBE nodes
      List<ImmutableBitSet> operandBitSet =
          analyzeGroupTuple(scope, groupAnalyzer,
              ((SqlCall) groupExpr).getOperandList());
      switch (groupExpr.getKind()) {
      case ROLLUP:
        builder.addAll(rollup(operandBitSet));
        return;
      default:
        builder.addAll(cube(operandBitSet));
        return;
      }
    }
    default:
      builder.add(
          analyzeGroupExpr(scope, groupAnalyzer, groupExpr));
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
  private static List<ImmutableBitSet> analyzeGroupTuple(SqlValidatorScope scope,
       GroupAnalyzer groupAnalyzer, List<SqlNode> operandList) {
    List<ImmutableBitSet> list = new ArrayList<>();
    for (SqlNode operand : operandList) {
      list.add(
          analyzeGroupExpr(scope, groupAnalyzer, operand));
    }
    return list;
  }

  /** Analyzes a component of a tuple in a GROUPING SETS clause. */
  private static ImmutableBitSet analyzeGroupExpr(SqlValidatorScope scope,
      GroupAnalyzer groupAnalyzer,
      SqlNode groupExpr) {
    final SqlNode expandedGroupExpr =
        scope.getValidator().expand(groupExpr, scope);

    switch (expandedGroupExpr.getKind()) {
    case ROW:
      return ImmutableBitSet.union(
          analyzeGroupTuple(scope, groupAnalyzer,
              ((SqlCall) expandedGroupExpr).getOperandList()));
    case OTHER:
      if (expandedGroupExpr instanceof SqlNodeList
          && ((SqlNodeList) expandedGroupExpr).size() == 0) {
        return ImmutableBitSet.of();
      }
    }

    final int ref = lookupGroupExpr(groupAnalyzer, groupExpr);
    if (expandedGroupExpr instanceof SqlIdentifier) {
      // SQL 2003 does not allow expressions of column references
      SqlIdentifier expr = (SqlIdentifier) expandedGroupExpr;

      // column references should be fully qualified.
      assert expr.names.size() == 2;
      String originalRelName = expr.names.get(0);
      String originalFieldName = expr.names.get(1);

      final SqlNameMatcher nameMatcher =
          scope.getValidator().getCatalogReader().nameMatcher();
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      scope.resolve(ImmutableList.of(originalRelName), nameMatcher, false,
          resolved);

      assert resolved.count() == 1;
      final SqlValidatorScope.Resolve resolve = resolved.only();
      final RelDataType rowType = resolve.rowType();
      final int childNamespaceIndex = resolve.path.steps().get(0).i;

      int namespaceOffset = 0;

      if (childNamespaceIndex > 0) {
        // If not the first child, need to figure out the width of
        // output types from all the preceding namespaces
        final SqlValidatorScope ancestorScope = resolve.scope;
        assert ancestorScope instanceof ListScope;
        List<SqlValidatorNamespace> children =
            ((ListScope) ancestorScope).getChildren();

        for (int j = 0; j < childNamespaceIndex; j++) {
          namespaceOffset +=
              children.get(j).getRowType().getFieldCount();
        }
      }

      RelDataTypeField field = nameMatcher.field(rowType, originalFieldName);
      int origPos = namespaceOffset + field.getIndex();

      groupAnalyzer.groupExprProjection.put(origPos, ref);
    }

    return ImmutableBitSet.of(ref);
  }

  private static int lookupGroupExpr(GroupAnalyzer groupAnalyzer,
      SqlNode expr) {
    for (Ord<SqlNode> node : Ord.zip(groupAnalyzer.groupExprs)) {
      if (node.e.equalsDeep(expr, Litmus.IGNORE)) {
        return node.i;
      }
    }

    switch (expr.getKind()) {
    case HOP:
    case TUMBLE:
    case SESSION:
      groupAnalyzer.extraExprs.add(expr);
      break;
    }
    groupAnalyzer.groupExprs.add(expr);
    return groupAnalyzer.groupExprs.size() - 1;
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
  public static ImmutableList<ImmutableBitSet> rollup(
      List<ImmutableBitSet> bitSets) {
    Set<ImmutableBitSet> builder = new LinkedHashSet<>();
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
  public static ImmutableList<ImmutableBitSet> cube(
      List<ImmutableBitSet> bitSets) {
    // Given the bit sets [{1}, {2, 3}, {5}],
    // form the lists [[{1}, {}], [{2, 3}, {}], [{5}, {}]].
    final Set<List<ImmutableBitSet>> builder = new LinkedHashSet<>();
    for (ImmutableBitSet bitSet : bitSets) {
      builder.add(Arrays.asList(bitSet, ImmutableBitSet.of()));
    }
    Set<ImmutableBitSet> flattenedBitSets = new LinkedHashSet<>();
    for (List<ImmutableBitSet> o : Linq4j.product(builder)) {
      flattenedBitSets.add(ImmutableBitSet.union(o));
    }
    return ImmutableList.copyOf(flattenedBitSets);
  }

  /**
   * Finds a {@link org.apache.calcite.jdbc.CalciteSchema.TypeEntry} in a
   * given schema whose type has the given name, possibly qualified.
   *
   * @param rootSchema root schema
   * @param typeName name of the type, may be qualified or fully-qualified
   *
   * @return TypeEntry with a table with the given name, or null
   */
  public static CalciteSchema.TypeEntry getTypeEntry(
      CalciteSchema rootSchema, SqlIdentifier typeName) {
    final String name;
    final List<String> path;
    if (typeName.isSimple()) {
      path = ImmutableList.of();
      name = typeName.getSimple();
    } else {
      path = Util.skipLast(typeName.names);
      name = Util.last(typeName.names);
    }
    CalciteSchema schema = rootSchema;
    for (String p : path) {
      if (schema == rootSchema
          && SqlNameMatchers.withCaseSensitive(true).matches(p, schema.getName())) {
        continue;
      }
      schema = schema.getSubSchema(p, true);
    }
    return schema == null ? null : schema.getType(name, false);
  }

  /**
   * Finds a {@link org.apache.calcite.jdbc.CalciteSchema.TableEntry} in a
   * given catalog reader whose table has the given name, possibly qualified.
   *
   * <p>Uses the case-sensitivity policy of the specified catalog reader.
   *
   * <p>If not found, returns null.
   *
   * @param catalogReader accessor to the table metadata
   * @param names Name of table, may be qualified or fully-qualified
   *
   * @return TableEntry with a table with the given name, or null
   */
  public static CalciteSchema.TableEntry getTableEntry(
      SqlValidatorCatalogReader catalogReader, List<String> names) {
    // First look in the default schema, if any.
    // If not found, look in the root schema.
    for (List<String> schemaPath : catalogReader.getSchemaPaths()) {
      CalciteSchema schema =
          getSchema(catalogReader.getRootSchema(),
              Iterables.concat(schemaPath, Util.skipLast(names)),
              catalogReader.nameMatcher());
      if (schema == null) {
        continue;
      }
      CalciteSchema.TableEntry entry =
          getTableEntryFrom(schema, Util.last(names),
              catalogReader.nameMatcher().isCaseSensitive());
      if (entry != null) {
        return entry;
      }
    }
    return null;
  }

  /**
   * Finds and returns {@link CalciteSchema} nested to the given rootSchema
   * with specified schemaPath.
   *
   * <p>Uses the case-sensitivity policy of specified nameMatcher.
   *
   * <p>If not found, returns null.
   *
   * @param rootSchema root schema
   * @param schemaPath full schema path of required schema
   * @param nameMatcher name matcher
   *
   * @return CalciteSchema that corresponds specified schemaPath
   */
  public static CalciteSchema getSchema(CalciteSchema rootSchema,
      Iterable<String> schemaPath, SqlNameMatcher nameMatcher) {
    CalciteSchema schema = rootSchema;
    for (String schemaName : schemaPath) {
      if (schema == rootSchema
          && nameMatcher.matches(schemaName, schema.getName())) {
        continue;
      }
      schema = schema.getSubSchema(schemaName,
          nameMatcher.isCaseSensitive());
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

  private static CalciteSchema.TableEntry getTableEntryFrom(
      CalciteSchema schema, String name, boolean caseSensitive) {
    CalciteSchema.TableEntry entry =
        schema.getTable(name, caseSensitive);
    if (entry == null) {
      entry = schema.getTableBasedOnNullaryFunction(name,
          caseSensitive);
    }
    return entry;
  }

  /**
   * Returns whether there are any input columns that are sorted.
   *
   * <p>If so, it can be the default ORDER BY clause for a WINDOW specification.
   * (This is an extension to the SQL standard for streaming.)
   */
  public static boolean containsMonotonic(SqlValidatorScope scope) {
    for (SqlValidatorNamespace ns : children(scope)) {
      ns = ns.resolve();
      for (String field : ns.getRowType().getFieldNames()) {
        if (!ns.getMonotonicity(field).mayRepeat()) {
          return true;
        }
      }
    }
    return false;
  }

  private static List<SqlValidatorNamespace> children(SqlValidatorScope scope) {
    return scope instanceof ListScope
        ? ((ListScope) scope).getChildren()
        : ImmutableList.of();
  }

  /**
   * Returns whether any of the given expressions are sorted.
   *
   * <p>If so, it can be the default ORDER BY clause for a WINDOW specification.
   * (This is an extension to the SQL standard for streaming.)
   */
  static boolean containsMonotonic(SelectScope scope, SqlNodeList nodes) {
    for (SqlNode node : nodes) {
      if (!scope.getMonotonicity(node).mayRepeat()) {
        return true;
      }
    }
    return false;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, copying every node, and fully-qualifying every
   * identifier.
   */
  @Deprecated // to be removed before 2.0
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
    }

    /** Copies a list of nodes. */
    public static SqlNodeList copy(SqlValidatorScope scope, SqlNodeList list) {
      //noinspection deprecation
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
      return SqlNode.clone(literal);
    }

    public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      final SqlCall call = SqlUtil.makeCall(getScope().getValidator().getOperatorTable(), id);
      if (call != null) {
        return call;
      }

      return getScope().fullyQualify(id).identifier;
    }

    public SqlNode visit(SqlDataTypeSpec type) {
      return SqlNode.clone(type);
    }

    public SqlNode visit(SqlDynamicParam param) {
      return SqlNode.clone(param);
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return SqlNode.clone(intervalQualifier);
    }
  }

  /** Suggests candidates for unique names, given the number of attempts so far
   * and the number of expressions in the project list. */
  public interface Suggester {
    String apply(String original, int attempt, int size);
  }

  public static final Suggester EXPR_SUGGESTER =
      (original, attempt, size) -> Util.first(original, "EXPR$") + attempt;

  public static final Suggester F_SUGGESTER =
      (original, attempt, size) -> Util.first(original, "$f")
          + Math.max(size, attempt);

  /** Builds a list of GROUP BY expressions. */
  static class GroupAnalyzer {
    /** Extra expressions, computed from the input as extra GROUP BY
     * expressions. For example, calls to the {@code TUMBLE} functions. */
    final List<SqlNode> extraExprs = new ArrayList<>();
    final List<SqlNode> groupExprs;
    final Map<Integer, Integer> groupExprProjection = new HashMap<>();
    int groupCount;

    GroupAnalyzer(List<SqlNode> groupExprs) {
      this.groupExprs = groupExprs;
    }

    SqlNode createGroupExpr() {
      // TODO: create an expression that could have no other source
      return SqlLiteral.createCharString("xyz" + groupCount++,
          SqlParserPos.ZERO);
    }
  }
}

// End SqlValidatorUtil.java
