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
package org.eigenbase.rex;

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableSet;

/**
 * Expression combined with sort flags (DESCENDING, NULLS LAST).
 */
public class RexFieldCollation extends Pair<RexNode, ImmutableSet<SqlOperator>>
{
    public RexFieldCollation(RexNode left, ImmutableSet<SqlOperator> right) {
        super(left, right);
    }

    @Override
    public String toString() {
        String s = left.toString();
        for (SqlOperator operator : right) {
            if (operator == SqlStdOperatorTable.descendingOperator) {
                s += " DESC";
            } else if (operator == SqlStdOperatorTable.nullsFirstOperator) {
                s += " NULLS FIRST";
            } else if (operator == SqlStdOperatorTable.nullsLastOperator) {
                s += " NULLS LAST";
            } else {
                throw new AssertionError(operator);
            }
        }
        return s;
    }

    public RelFieldCollation.Direction getDirection() {
        return right.contains(SqlStdOperatorTable.descendingOperator)
            ? RelFieldCollation.Direction.Descending
            : RelFieldCollation.Direction.Ascending;
    }

    public RelFieldCollation.NullDirection getNullDirection() {
        return right.contains(SqlStdOperatorTable.nullsLastOperator)
            ? RelFieldCollation.NullDirection.LAST
            : right.contains(SqlStdOperatorTable.nullsFirstOperator)
            ? RelFieldCollation.NullDirection.FIRST
            : RelFieldCollation.NullDirection.UNSPECIFIED;
    }
}

// End RexFieldCollation.java
