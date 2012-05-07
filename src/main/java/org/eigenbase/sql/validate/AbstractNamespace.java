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
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Abstract implementation of {@link SqlValidatorNamespace}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 3, 2005
 */
abstract class AbstractNamespace
    implements SqlValidatorNamespace
{
    //~ Instance fields --------------------------------------------------------

    protected final SqlValidatorImpl validator;

    /**
     * Whether this scope is currently being validated. Used to check for
     * cycles.
     */
    private SqlValidatorImpl.Status status =
        SqlValidatorImpl.Status.Unvalidated;

    /**
     * Type of the output row, which comprises the name and type of each output
     * column. Set on validate.
     */
    protected RelDataType rowType;

    private boolean forceNullable;

    protected final SqlNode enclosingNode;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AbstractNamespace.
     *
     * @param validator Validator
     * @param enclosingNode Enclosing node
     */
    AbstractNamespace(
        SqlValidatorImpl validator,
        SqlNode enclosingNode)
    {
        this.validator = validator;
        this.enclosingNode = enclosingNode;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlValidator getValidator()
    {
        return validator;
    }

    public final void validate()
    {
        switch (status) {
        case Unvalidated:
            try {
                status = SqlValidatorImpl.Status.InProgress;
                Util.permAssert(
                    rowType == null,
                    "Namespace.rowType must be null before validate has been called");
                rowType = validateImpl();
                Util.permAssert(
                    rowType != null,
                    "validateImpl() returned null");
                if (forceNullable) {
                    // REVIEW jvs 10-Oct-2005: This may not be quite right
                    // if it means that nullability will be forced in the
                    // ON clause where it doesn't belong.
                    rowType =
                        validator.getTypeFactory().createTypeWithNullability(
                            rowType,
                            true);
                }
            } finally {
                status = SqlValidatorImpl.Status.Valid;
            }
            break;
        case InProgress:
            throw Util.newInternal("todo: Cycle detected during type-checking");
        case Valid:
            break;
        default:
            throw Util.unexpected(status);
        }
    }

    /**
     * Validates this scope and returns the type of the records it returns.
     * External users should call {@link #validate}, which uses the {@link
     * #status} field to protect against cycles.
     *
     * @return record data type, never null
     *
     * @post return != null
     */
    protected abstract RelDataType validateImpl();

    public RelDataType getRowType()
    {
        if (rowType == null) {
            validator.validateNamespace(this);
            Util.permAssert(rowType != null, "validate must set rowType");
        }
        return rowType;
    }

    public RelDataType getRowTypeSansSystemColumns()
    {
        return getRowType();
    }

    public void setRowType(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    public SqlNode getEnclosingNode()
    {
        return enclosingNode;
    }

    public SqlValidatorTable getTable()
    {
        return null;
    }

    public SqlValidatorNamespace lookupChild(String name)
    {
        return validator.lookupFieldNamespace(
            getRowType(),
            name);
    }

    public boolean fieldExists(String name)
    {
        final RelDataType rowType = getRowType();
        final RelDataType dataType =
            SqlValidatorUtil.lookupFieldType(rowType, name);
        return dataType != null;
    }

    public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs()
    {
        return Collections.emptyList();
    }

    public SqlMonotonicity getMonotonicity(String columnName)
    {
        return SqlMonotonicity.NotMonotonic;
    }

    public void makeNullable()
    {
        forceNullable = true;
    }

    public String translate(String name)
    {
        return name;
    }

    public <T> T unwrap(Class<T> clazz)
    {
        return clazz.cast(this);
    }

    public boolean isWrapperFor(Class<?> clazz)
    {
        return clazz.isInstance(this);
    }
}

// End AbstractNamespace.java
