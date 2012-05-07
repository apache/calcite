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


/**
 * Namespace for a table constructor <code>VALUES (expr, expr, ...)</code>.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class TableConstructorNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall values;
    private final SqlValidatorScope scope;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator Validator
     * @param values VALUES parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing node
     */
    TableConstructorNamespace(
        SqlValidatorImpl validator,
        SqlCall values,
        SqlValidatorScope scope,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.values = values;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        return validator.getTableConstructorRowType(values, scope);
    }

    public SqlNode getNode()
    {
        return values;
    }

    /**
     * Returns the scope.
     *
     * @return scope
     */
    public SqlValidatorScope getScope()
    {
        return scope;
    }
}

// End TableConstructorNamespace.java
