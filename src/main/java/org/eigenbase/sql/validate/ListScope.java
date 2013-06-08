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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public abstract class ListScope
    extends DelegatingScope
{
    //~ Instance fields --------------------------------------------------------

    /**
     * List of child {@link SqlValidatorNamespace} objects and their names.
     */
    protected final List<Pair<String, SqlValidatorNamespace>> children =
        new ArrayList<Pair<String, SqlValidatorNamespace>>();

    //~ Constructors -----------------------------------------------------------

    public ListScope(SqlValidatorScope parent)
    {
        super(parent);
    }

    //~ Methods ----------------------------------------------------------------

    public void addChild(SqlValidatorNamespace ns, String alias)
    {
        assert alias != null;
        children.add(Pair.of(alias, ns));
    }

    /**
     * Returns an immutable list of child namespaces.
     *
     * @return list of child namespaces
     */
    public List<SqlValidatorNamespace> getChildren()
    {
        return Pair.right(children);
    }

    protected SqlValidatorNamespace getChild(String alias)
    {
        if (alias == null) {
            if (children.size() != 1) {
                throw Util.newInternal(
                    "no alias specified, but more than one table in from list");
            }
            return children.get(0).right;
        } else {
            for (Pair<String, SqlValidatorNamespace> child : children) {
                if (child.left.equals(alias)) {
                    return child.right;
                }
            }
            return null;
        }
    }

    public void findAllColumnNames(List<SqlMoniker> result)
    {
        for (Pair<String, SqlValidatorNamespace> pair : children) {
            addColumnNames(pair.right, result);
        }
        parent.findAllColumnNames(result);
    }

    public void findAliases(List<SqlMoniker> result)
    {
        for (Pair<String, SqlValidatorNamespace> pair : children) {
            result.add(new SqlMonikerImpl(pair.left, SqlMonikerType.Table));
        }
        parent.findAliases(result);
    }

    public String findQualifyingTableName(
        final String columnName,
        SqlNode ctx)
    {
        int count = 0;
        String tableName = null;
        for (Pair<String, SqlValidatorNamespace> child : children) {
            final RelDataType rowType = child.right.getRowType();
            if (SqlValidatorUtil.lookupField(rowType, columnName) != null) {
                tableName = child.left;
                count++;
            }
        }
        switch (count) {
        case 0:
            return parent.findQualifyingTableName(columnName, ctx);
        case 1:
            return tableName;
        default:
            throw validator.newValidationError(
                ctx,
                EigenbaseResource.instance().ColumnAmbiguous.ex(columnName));
        }
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut)
    {
        // First resolve by looking through the child namespaces.
        final int i = Pair.left(children).indexOf(name);
        if (i >= 0) {
            if (ancestorOut != null) {
                ancestorOut[0] = this;
            }
            if (offsetOut != null) {
                offsetOut[0] = i;
            }
            return children.get(i).right;
        }

        // Then call the base class method, which will delegate to the
        // parent scope.
        return parent.resolve(name, ancestorOut, offsetOut);
    }

    public RelDataType resolveColumn(String columnName, SqlNode ctx)
    {
        int found = 0;
        RelDataType theType = null;
        for (Pair<String, SqlValidatorNamespace> pair : children) {
            SqlValidatorNamespace childNs = pair.right;
            final RelDataType childRowType = childNs.getRowType();
            final RelDataType type =
                SqlValidatorUtil.lookupFieldType(childRowType, columnName);
            if (type != null) {
                found++;
                theType = type;
            }
        }
        if (found == 0) {
            return null;
        } else if (found > 1) {
            throw validator.newValidationError(
                ctx,
                EigenbaseResource.instance().ColumnAmbiguous.ex(columnName));
        } else {
            return theType;
        }
    }
}

// End ListScope.java
