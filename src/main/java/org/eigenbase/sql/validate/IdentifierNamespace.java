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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * Namespace whose contents are defined by the type of an {@link
 * org.eigenbase.sql.SqlIdentifier identifier}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class IdentifierNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier id;

    /**
     * The underlying table. Set on validate.
     */
    private SqlValidatorTable table;

    /**
     * List of monotonic expressions. Set on validate.
     */
    private List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an IdentifierNamespace.
     *
     * @param validator Validator
     * @param id Identifier node
     * @param enclosingNode Enclosing node
     */
    IdentifierNamespace(
        SqlValidatorImpl validator,
        SqlIdentifier id,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.id = id;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType validateImpl()
    {
        table = validator.catalogReader.getTable(id.names);
        if (table == null) {
            throw validator.newValidationError(
                id,
                EigenbaseResource.instance().TableNameNotFound.ex(
                    id.toString()));
        }
        if (validator.shouldExpandIdentifiers()) {
            // TODO:  expand qualifiers for column references also
            String [] qualifiedNames = table.getQualifiedName();
            if (qualifiedNames != null) {
                // Assign positions to the components of the fully-qualified
                // identifier, as best we can. We assume that qualification
                // adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
                SqlParserPos [] poses = new SqlParserPos[qualifiedNames.length];
                Arrays.fill(
                    poses,
                    id.getParserPosition());
                int offset = qualifiedNames.length - id.names.length;

                // Test offset in case catalog supports fewer
                // qualifiers than catalog reader.
                if (offset >= 0) {
                    for (int i = 0; i < id.names.length; i++) {
                        poses[i + offset] = id.getComponentParserPosition(i);
                    }
                }
                id.setNames(qualifiedNames, poses);
            }
        }

        // Build a list of monotonic expressions.
        monotonicExprs = new ArrayList<Pair<SqlNode, SqlMonotonicity>>();
        RelDataType rowType = table.getRowType();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            final String fieldName = fields[i].getName();
            final SqlMonotonicity monotonicity =
                table.getMonotonicity(fieldName);
            if (monotonicity != SqlMonotonicity.NotMonotonic) {
                monotonicExprs.add(
                    new Pair<SqlNode, SqlMonotonicity>(
                        new SqlIdentifier(fieldName, SqlParserPos.ZERO),
                        monotonicity));
            }
        }

        // Validation successful.
        return rowType;
    }

    public SqlIdentifier getId()
    {
        return id;
    }

    public SqlNode getNode()
    {
        return id;
    }

    public SqlValidatorTable getTable()
    {
        return table;
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut)
    {
        return null;
    }

    public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs()
    {
        return monotonicExprs;
    }

    public SqlMonotonicity getMonotonicity(String columnName)
    {
        final SqlValidatorTable table = getTable();
        return table.getMonotonicity(columnName);
    }
}

// End IdentifierNamespace.java
