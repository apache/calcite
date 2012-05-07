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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Namespace representing the row type produced by joining two relations.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
class JoinNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlJoin join;

    //~ Constructors -----------------------------------------------------------

    JoinNamespace(SqlValidatorImpl validator, SqlJoin join)
    {
        super(validator, null);
        this.join = join;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        RelDataType leftType =
            validator.getNamespace(join.getLeft()).getRowType();
        RelDataType rightType =
            validator.getNamespace(join.getRight()).getRowType();
        if (join.getJoinType() == SqlJoinOperator.JoinType.Left) {
            rightType =
                validator.getTypeFactory().createTypeWithNullability(
                    rightType,
                    true);
        }
        if (join.getJoinType() == SqlJoinOperator.JoinType.Right) {
            leftType =
                validator.getTypeFactory().createTypeWithNullability(
                    leftType,
                    true);
        }
        if (join.getJoinType() == SqlJoinOperator.JoinType.Full) {
            leftType =
                validator.getTypeFactory().createTypeWithNullability(
                    leftType,
                    true);
            rightType =
                validator.getTypeFactory().createTypeWithNullability(
                    rightType,
                    true);
        }
        final RelDataType [] types = { leftType, rightType };
        return validator.getTypeFactory().createJoinType(types);
    }

    public SqlNode getNode()
    {
        return join;
    }
}

// End JoinNamespace.java
