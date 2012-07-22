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

import net.hydromatic.linq4j.AbstractQueryable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.ParameterExpression;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Table;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.pretty.SqlPrettyWriter;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * <p>The idea is not to read the whole table, however. The idea is to use
 * this as a building block for a query, by applying Queryable operators
 * such as {@link net.hydromatic.linq4j.Queryable#where(net.hydromatic.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be
 * executed efficiently on the JDBC server.</p>
 *
 * @author jhyde
 */
class JdbcTable<T> extends AbstractQueryable<T> implements Table<T> {
    private final Type elementType;
    private final JdbcSchema dataContext;
    private final String tableName;

    private static final ParameterExpression DC =
        Expressions.parameter(JdbcSchema.class, "dc");

    public JdbcTable(
        Type elementType,
        JdbcSchema dataContext,
        String tableName)
    {
        super();
        this.elementType = elementType;
        this.dataContext = dataContext;
        this.tableName = tableName;
    }

    public String toString() {
        return "JdbcTable {" + tableName + "}";
    }

    public QueryProvider getProvider() {
        return dataContext.queryProvider;
    }

    public DataContext getDataContext() {
        return dataContext;
    }

    public Type getElementType() {
        return elementType;
    }

    public Expression getExpression() {
        return Expressions.lambda(
            Expressions.call(
                DC,
                "getTable",
                Expressions.constant(tableName),
                Expressions.constant(elementType)));
    }

    public Iterator<T> iterator() {
        return Linq4j.enumeratorIterator(enumerator());
    }

    public Enumerator<T> enumerator() {
        SqlWriter writer = new SqlPrettyWriter(dataContext.dialect);
        writer.keyword("select");
        writer.literal("*");
        writer.keyword("from");
        writer.identifier("foodmart");
        writer.literal(".");
        writer.identifier(tableName);
        final String sql = writer.toString();

        return JdbcUtils.sqlEnumerator(sql, dataContext);
    }
}

// End JdbcTable.java
