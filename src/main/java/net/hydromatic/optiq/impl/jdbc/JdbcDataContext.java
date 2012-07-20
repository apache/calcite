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

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.optiq.DataContext;

import org.eigenbase.sql.SqlDialect;

import java.lang.reflect.Type;
import javax.sql.DataSource;

/**
 * Implementation of {@link DataContext} for a {@link JdbcQueryProvider}.
 *
 * @author jhyde
 */
public class JdbcDataContext implements DataContext {
    final DataSource dataSource;
    final QueryProvider queryProvider;
    final SqlDialect dialect;

    /** Creates a JdbcDataContext. */
    public JdbcDataContext(
        QueryProvider queryProvider,
        DataSource dataSource,
        SqlDialect dialect)
    {
        this.dataSource = dataSource;
        this.queryProvider = queryProvider;
        this.dialect = dialect;
    }

    /** Creates JdbcDataContext using an implicit SQL dialect. */
    public JdbcDataContext(
        DataSource dataSource,
        QueryProvider queryProvider)
    {
        this(
            queryProvider,
            dataSource,
            JdbcUtils.DialectPool.INSTANCE.get(dataSource));
    }

    public Queryable getTable(String name, Type elementType) {
        return new JdbcTableQueryable(elementType, this, name);
    }

    @SuppressWarnings("unchecked")
    public <T> Queryable<T> getTable(String name, Class<T> elementType) {
        return getTable(name, (Type) elementType);
    }
}

// End JdbcDataContext.java
