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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;


/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * <p>Examples:
 *
 * <ul>
 * <li><code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code>,
 * <li><code>SELECT * FROM (TABLE getEmpsInDept(30))</code>.
 * </ul>
 *
 * <p>NOTE: jhyde, 2006/4/24: These days, this class seems to be used
 * exclusively for the <code>MULTISET</code> construct.
 *
 * @author wael
 * @version $Id$
 * @see CollectScope
 * @since Mar 25, 2003
 */
public class CollectNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall child;
    private final SqlValidatorScope scope;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CollectNamespace.
     *
     * @param child Parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing parse tree node
     */
    CollectNamespace(
        SqlCall child,
        SqlValidatorScope scope,
        SqlNode enclosingNode)
    {
        super((SqlValidatorImpl) scope.getValidator(), enclosingNode);
        this.child = child;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        final RelDataType type =
            child.getOperator().deriveType(validator, scope, child);

        switch (child.getKind()) {
        case MULTISET_VALUE_CONSTRUCTOR:

            // "MULTISET [<expr>, ...]" needs to be wrapped in a record if
            // <expr> has a scalar type.
            // For example, "MULTISET [8, 9]" has type
            // "RECORD(INTEGER EXPR$0 NOT NULL) NOT NULL MULTISET NOT NULL".
            boolean isNullable = type.isNullable();
            final RelDataType componentType =
                ((MultisetSqlType) type).getComponentType();
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            if (componentType.isStruct()) {
                return type;
            } else {
                final RelDataType structType =
                    typeFactory.createStructType(
                        new RelDataType[] { type },
                        new String[] { validator.deriveAlias(child, 0) });
                final RelDataType multisetType =
                    typeFactory.createMultisetType(structType, -1);
                return typeFactory.createTypeWithNullability(
                    multisetType,
                    isNullable);
            }

        case MULTISET_QUERY_CONSTRUCTOR:

            // "MULTISET(<query>)" is already a record.
            assert (type instanceof MultisetSqlType)
                && ((MultisetSqlType) type).getComponentType().isStruct()
                : type;
            return type;

        default:
            throw Util.unexpected(child.getKind());
        }
    }

    public SqlNode getNode()
    {
        return child;
    }

    public SqlValidatorScope getScope()
    {
        return scope;
    }
}

// End CollectNamespace.java
