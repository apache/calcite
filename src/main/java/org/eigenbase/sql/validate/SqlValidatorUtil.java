/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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


/**
 * Utility methods related to validation.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class SqlValidatorUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a {@link SqlValidatorScope} into a {@link RelOptTable}. This is
     * only possible if the scope represents an identifier, such as "sales.emp".
     * Otherwise, returns null.
     *
     * @param namespace Namespace
     * @param schema Schema
     * @param datasetName Name of sample dataset to substitute, or null to use
     * the regular table
     * @param usedDataset Output parameter which is set to true if a sample
     * dataset is found; may be null
     */
    public static RelOptTable getRelOptTable(
        SqlValidatorNamespace namespace,
        RelOptSchema schema,
        String datasetName,
        boolean [] usedDataset)
    {
        if (namespace.isWrapperFor(IdentifierNamespace.class)) {
            IdentifierNamespace identifierNamespace =
                namespace.unwrap(IdentifierNamespace.class);
            final String [] names = identifierNamespace.getId().names;
            if ((datasetName != null)
                && (schema instanceof RelOptSchemaWithSampling))
            {
                return ((RelOptSchemaWithSampling) schema).getTableForMember(
                    names,
                    datasetName,
                    usedDataset);
            } else {
                // Schema does not support substitution. Ignore the dataset,
                // if any.
                return schema.getTableForMember(names);
            }
        } else {
            return null;
        }
    }

    /**
     * Looks up a field with a given name and if found returns its type.
     *
     * @param rowType Row type
     * @param columnName Field name
     *
     * @return Field's type, or null if not found
     */
    static RelDataType lookupFieldType(
        final RelDataType rowType,
        String columnName)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(columnName)) {
                return field.getType();
            }
        }
        return null;
    }

    /**
     * Looks up a field with a given name and if found returns its ordinal.
     *
     * @param rowType Row type
     * @param columnName Field name
     *
     * @return Ordinal of field, or -1 if not found
     */
    public static int lookupField(
        final RelDataType rowType,
        String columnName)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public static void checkCharsetAndCollateConsistentIfCharType(
        RelDataType type)
    {
        //(every charset must have a default collation)
        if (SqlTypeUtil.inCharFamily(type)) {
            Charset strCharset = type.getCharset();
            Charset colCharset = type.getCollation().getCharset();
            assert (null != strCharset);
            assert (null != colCharset);
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
        String alias)
    {
        final SqlParserPos pos = expr.getParserPosition();
        final SqlIdentifier id = new SqlIdentifier(alias, pos);
        return SqlStdOperatorTable.asOperator.createCall(pos, expr, id);
    }

    /**
     * Derives an alias for a node. If it cannot derive an alias, returns null.
     *
     * <p>This method doesn't try very hard. It doesn't invent mangled aliases,
     * and doesn't even recognize an AS clause. (See {@link #getAlias(SqlNode,
     * int)} for that.) It just takes the last part of an identifier.
     */
    public static String getAlias(SqlNode node)
    {
        if (node instanceof SqlIdentifier) {
            String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];
        } else {
            return null;
        }
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
     * "expr$<i>ordinal</i>" if ordinal >= 0; otherwise null
     */
    public static String getAlias(SqlNode node, int ordinal)
    {
        switch (node.getKind()) {
        case AS:
            // E.g. "1 + 2 as foo" --> "foo"
            return ((SqlCall) node).getOperands()[1].toString();

        case OVER:
            // E.g. "bids over w" --> "bids"
            return getAlias(((SqlCall) node).getOperands()[0], ordinal);

        case IDENTIFIER:
            // E.g. "foo.bar" --> "bar"
            final String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];

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
     * @param name Suggested name, may not be unique
     * @param nameList Collection of names already used
     *
     * @return Unique name
     */
    public static String uniquify(String name, Collection<String> nameList)
    {
        if (name == null) {
            name = "EXPR$";
        }
        if (nameList.contains(name)) {
            String aliasBase = name;
            for (int j = 0;; j++) {
                name = aliasBase + j;
                if (!nameList.contains(name)) {
                    break;
                }
            }
        }
        nameList.add(name);
        return name;
    }

    /**
     * Factory method for {@link SqlValidator}.
     */
    public static SqlValidatorWithHints newValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory)
    {
        return new SqlValidatorImpl(
            opTab,
            catalogReader,
            typeFactory,
            SqlConformance.Default);
    }

    /**
     * Makes sure that the names in a list are unique.
     */
    public static void uniquify(List<String> nameList)
    {
        List<String> usedList = new ArrayList<String>();
        for (int i = 0; i < nameList.size(); i++) {
            String name = nameList.get(i);
            String uniqueName = uniquify(name, usedList);
            if (!uniqueName.equals(name)) {
                nameList.set(i, uniqueName);
            }
        }
    }

    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
     * namespace. The returned namespace may represent a schema, table, column,
     * etc.
     *
     * @pre names.size() > 0
     * @post return != null
     */
    public static SqlValidatorNamespace lookup(
        SqlValidatorScope scope,
        List<String> names)
    {
        Util.pre(names.size() > 0, "names.size() > 0");
        SqlValidatorNamespace namespace = null;
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (i == 0) {
                namespace = scope.resolve(name, null, null);
            } else {
                namespace = namespace.lookupChild(name);
            }
        }
        Util.permAssert(namespace != null, "post: namespace != null");
        return namespace;
    }

    public static void getSchemaObjectMonikers(
        SqlValidatorCatalogReader catalogReader,
        List<String> names,
        List<SqlMoniker> hints)
    {
        // Assume that the last name is 'dummy' or similar.
        List<String> subNames = names.subList(0, names.size() - 1);
        hints.addAll(catalogReader.getAllSchemaObjectNames(subNames));

        // If the name has length 0, try prepending the name of the default
        // schema. So, the empty name would yield a list of tables in the
        // default schema, as well as a list of schemas from the above code.
        if (subNames.size() == 0) {
            hints.addAll(
                catalogReader.getAllSchemaObjectNames(
                    Collections.singletonList(
                        catalogReader.getSchemaName())));
        }
    }

    public static SelectScope getEnclosingSelectScope(SqlValidatorScope scope)
    {
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
     * @param leftRowType Row type of left input to the join
     * @param rightRowType Row type of right input to the join
     *
     * @return List of columns that occur once on each side
     */
    public static List<String> deriveNaturalJoinColumnList(
        RelDataType leftRowType,
        RelDataType rightRowType)
    {
        List<String> naturalColumnNames = new ArrayList<String>();
        final String [] leftNames = SqlTypeUtil.getFieldNames(leftRowType);
        final String [] rightNames = SqlTypeUtil.getFieldNames(rightRowType);
        for (String name : leftNames) {
            if ((countOccurrences(name, leftNames) == 1)
                && (countOccurrences(name, rightNames) == 1))
            {
                naturalColumnNames.add(name);
            }
        }
        return naturalColumnNames;
    }

    static int countOccurrences(String name, String [] names)
    {
        int count = 0;
        for (String s : names) {
            if (s.equals(name)) {
                ++count;
            }
        }
        return count;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over an expression, copying every node, and fully-qualifying every
     * identifier.
     */
    public static class DeepCopier
        extends SqlScopedShuttle
    {
        DeepCopier(SqlValidatorScope scope)
        {
            super(scope);
        }

        public SqlNode visit(SqlNodeList list)
        {
            SqlNodeList copy = new SqlNodeList(list.getParserPosition());
            for (SqlNode node : list) {
                copy.add(node.accept(this));
            }
            return copy;
        }

        // Override to copy all arguments regardless of whether visitor changes
        // them.
        protected SqlNode visitScoped(SqlCall call)
        {
            ArgHandler<SqlNode> argHandler =
                new CallCopyingArgHandler(call, true);
            call.getOperator().acceptCall(this, call, false, argHandler);
            return argHandler.result();
        }

        public SqlNode visit(SqlLiteral literal)
        {
            return (SqlNode) literal.clone();
        }

        public SqlNode visit(SqlIdentifier id)
        {
            return getScope().fullyQualify(id);
        }

        public SqlNode visit(SqlDataTypeSpec type)
        {
            return (SqlNode) type.clone();
        }

        public SqlNode visit(SqlDynamicParam param)
        {
            return (SqlNode) param.clone();
        }

        public SqlNode visit(SqlIntervalQualifier intervalQualifier)
        {
            return (SqlNode) intervalQualifier.clone();
        }
    }
}

// End SqlValidatorUtil.java
