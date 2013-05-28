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

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.*;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;

import java.util.Map;

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends MapSchema {
    // TODO: implement 'driver' property
    // TODO: implement 'source' property
    // TODO: test Factory

    private final Schema sourceSchema;

    /**
     * Creates a CloneSchema.
     *
     * @param parentSchema Parent schema
     * @param expression Expression for schema
     * @param sourceSchema JDBC data source
     */
    public CloneSchema(
        Schema parentSchema,
        Expression expression,
        Schema sourceSchema)
    {
        super(parentSchema, expression);
        this.sourceSchema = sourceSchema;
    }

    @Override
    public <E> Table<E> getTable(String name, Class<E> elementType) {
        // TODO: check elementType matches table.elementType
        assert elementType != null;

        Table<E> table = super.getTable(name, elementType);
        if (table != null) {
            return table;
        }
        // TODO: make thread safe!
        Table<E> sourceTable = sourceSchema.getTable(name, elementType);
        if (sourceTable != null) {
            //noinspection unchecked
            table = createCloneTable(sourceTable, name);
            addTable(
                new TableInSchemaImpl(this, name, TableType.TABLE, table));
            return table;
        }
        return null;
    }

    private <T> Table<T> createCloneTable(Table<T> sourceTable, String name) {
        // More efficient: table based on an array per column.
        final ColumnLoader loader =
            new ColumnLoader<T>(
                typeFactory, sourceTable, sourceTable.getRowType());
        return new ArrayTable<T>(
            this,
            sourceTable.getElementType(),
            sourceTable.getRowType(),
            Expressions.call(
                getExpression(),
                BuiltinMethod.DATA_CONTEXT_GET_TABLE.method,
                Expressions.constant(name),
                Expressions.constant(Object.class)),
            loader.representationValues,
            loader.size(),
            loader.sortField);
    }

    /**
     * Creates a CloneSchema within another schema.
     *
     * @param parentSchema Parent schema
     * @param name Name of new schema
     * @param sourceSchema Source schema
     * @return New CloneSchema
     */
    public static CloneSchema create(
        MutableSchema parentSchema,
        String name,
        Schema sourceSchema)
    {
        CloneSchema schema =
            new CloneSchema(
                parentSchema,
                parentSchema.getSubSchemaExpression(name, Object.class),
                sourceSchema);
        parentSchema.addSchema(name, schema);
        return schema;
    }

    /** Schema factory that creates a
     * {@link net.hydromatic.optiq.impl.clone.CloneSchema}.
     * This allows you to create a clone schema inside a model.json file.
     *
     * <pre>{@code
     * {
     *   version: '1.0',
     *   defaultSchema: 'FOODMART_CLONE',
     *   schemas: [
     *     {
     *       name: 'FOODMART_CLONE',
     *       type: 'custom',
     *       factory: 'net.hydromatic.optiq.impl.clone.CloneSchema.Factory',
     *       operand: {
     *         driver: 'com.mysql.jdbc.Driver',
     *         url: 'jdbc:mysql://localhost/foodmart',
     *         user: 'foodmart',
     *         password: 'foodmart'
     *       }
     *     }
     *   ]
     * }
     * }</pre>
     */
    public static class Factory implements SchemaFactory {
        public Schema create(
            MutableSchema parentSchema,
            String name,
            Map<String, Object> operand)
        {
            JdbcSchema jdbcSchema =
                JdbcSchema.create(parentSchema, name + "$source", operand);
            return CloneSchema.create(parentSchema, name, jdbcSchema);
        }
    }
}

// End CloneSchema.java
