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
package net.hydromatic.optiq.impl.jdbc;

import org.eigenbase.rel.RelNode;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.util.Util;

/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor {
    final SqlDialect dialect;
    int indent;

    public JdbcImplementor(SqlDialect dialect) {
        this.dialect = dialect;
    }

    public SqlBuilder subquery(SqlBuilder buf, int i, RelNode e, String alias) {
        buf.append("(");
        ++indent;
        newline(buf)
            .append(visitChild(i, e));
        --indent;
        buf.append(") AS ").identifier(alias);
        return buf;
    }

    public SqlString visitChild(int i, RelNode e) {
        return ((JdbcRel) e).implement(this);
    }

    public SqlBuilder newline(SqlBuilder buf) {
        return buf.append("\n")
            .append(Util.spaces(indent * 4));
    }
}

// End JdbcImplementor.java
