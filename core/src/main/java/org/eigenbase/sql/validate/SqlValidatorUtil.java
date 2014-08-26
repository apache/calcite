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
package org.eigenbase.sql.validate;

import java.nio.charset.*;
import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.prepare.Prepare;

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
          throw new Error(
              type.toString()
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

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, copying every node, and fully-qualifying every
   * identifier.
   */
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
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
          return Util.first(original, "$f") + size;
        }
      };
}

// End SqlValidatorUtil.java
