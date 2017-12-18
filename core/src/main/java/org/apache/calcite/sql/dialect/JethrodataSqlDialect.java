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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDialect</code> implementation for the Jethrodata database.
 */
public class JethrodataSqlDialect extends SqlDialect {
    public static final SqlDialect DEFAULT =
            new JethrodataSqlDialect(EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.JETHRO).
                    withIdentifierQuoteString("\""));

    /** Creates an InterbaseSqlDialect. */
    public JethrodataSqlDialect(Context context) {
        super(context);
    }

    @Override public boolean hasImplicitTableAlias() {
        return false;
    }

    @Override public boolean supportsCharSet() {
        return false;
    }

    @Override public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst) {
        return node;
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        //TODOY check if STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP are supported
        switch (kind) {
            case COUNT:
            case SUM:
            case AVG:
            case MIN:
            case MAX:
                return true;
        }
        return false;
    }

    @Override public SqlNode getCastSpec(RelDataType type) {
        //TODOY support all hive types
        switch (type.getSqlTypeName()) {
            case VARCHAR:
                // MySQL doesn't have a VARCHAR type, only CHAR.
                return new SqlDataTypeSpec(new SqlIdentifier("CHAR", SqlParserPos.ZERO),
                        type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
            //case INTEGER:
            //  return new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", SqlParserPos.ZERO),
            //      type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
        }
        return super.getCastSpec(type);
    }

    public boolean supportsOffsetFetch() {
        return false;
    }

    public boolean supportsNestedAggregations() {
        return false;
    }
}

// End JethrodataSqlDialect.java
