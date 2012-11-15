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
package net.hydromatic.optiq.impl.clone;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.*;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends MapSchema {
    private final Schema sourceSchema;

    /**
     * Creates a CloneSchema.
     *
     * @param queryProvider Query provider
     * @param typeFactory Type factory
     * @param expression Expression for schema
     * @param sourceSchema JDBC data source
     */
    public CloneSchema(
        QueryProvider queryProvider,
        JavaTypeFactory typeFactory,
        Expression expression,
        Schema sourceSchema)
    {
        super(queryProvider, typeFactory, expression);
        this.sourceSchema = sourceSchema;
    }

    @Override
    public Table getTable(String name) {
        Table table = super.getTable(name);
        if (table != null) {
            return table;
        }
        // TODO: make thread safe!
        Table sourceTable = sourceSchema.getTable(name);
        if (sourceTable != null) {
            table = createCloneTable(sourceTable, name);
            addTable(name, table);
            return table;
        }
        return null;
    }

    private <T> Table<T> createCloneTable(Table<T> sourceTable, String name) {
        final List<T> list = new ArrayList<T>();
        sourceTable.into(list);
        return new ListTable<T>(
            this,
            sourceTable.getElementType(),
            Expressions.call(
                getExpression(),
                BuiltinMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(name)),
            list);
    }

    /**
     * Creates a CloneSchema within another schema.
     *
     *
     * @param optiqConnection Connection to Optiq (also a query provider)
     * @param parentSchema Parent schema
     * @param name Name of new schema
     * @param sourceSchema Source schema
     * @return New CloneSchema
     */
    public static CloneSchema create(
        OptiqConnection optiqConnection,
        MutableSchema parentSchema,
        String name,
        Schema sourceSchema)
    {
        CloneSchema schema =
            new CloneSchema(
                optiqConnection,
                optiqConnection.getTypeFactory(),
                parentSchema.getSubSchemaExpression(name, Object.class),
                sourceSchema);
        parentSchema.addSchema(name, schema);
        return schema;
    }

    private static class ListTable<T>
        extends BaseQueryable<T>
        implements Table<T>
    {
        private final Schema schema;
        private final List<T> list;

        public ListTable(
            Schema schema,
            Type elementType,
            Expression expression,
            List<T> list)
        {
            super(schema.getQueryProvider(), elementType, expression);
            this.schema = schema;
            this.list = list;
        }

        public DataContext getDataContext() {
            return schema;
        }

        @Override
        public Enumerator<T> enumerator() {
            return Linq4j.enumerator(list);
        }
    }
}

// End CloneSchema.java
