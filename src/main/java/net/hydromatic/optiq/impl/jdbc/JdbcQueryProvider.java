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

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.QueryProviderImpl;
import net.hydromatic.linq4j.expressions.Expression;

import java.lang.reflect.Type;

/**
 * Implementation of {@link QueryProvider} that talks to JDBC databases.
 *
 * @author jhyde
 */
public final class JdbcQueryProvider extends QueryProviderImpl {
    public static JdbcQueryProvider INSTANCE = new JdbcQueryProvider();

    private JdbcQueryProvider() {
    }

    @Override
    protected <T> Enumerator<T> executeQuery(QueryableImpl<T> queryable) {
        return null;
    }

    /**
     * Binds an expression to this query provider.
     */
    private static class ExpressionQueryable<T> extends QueryableImpl<T> {
        public ExpressionQueryable(
            JdbcQueryProvider jdbcQueryProvider,
            Type elementType,
            Expression expression)
        {
            super(jdbcQueryProvider, elementType, expression);
        }

        @Override
        public JdbcQueryProvider getProvider() {
            return (JdbcQueryProvider) super.getProvider();
        }
    }
}

// End JdbcQueryProvider.java
