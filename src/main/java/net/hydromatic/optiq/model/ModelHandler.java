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
package net.hydromatic.optiq.model;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.apache.commons.dbcp.BasicDataSource;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Reads a model and creates schema objects accordingly.
 */
public class ModelHandler {
    private final OptiqConnection connection;
    private final List<Schema> schemaStack = new ArrayList<Schema>();

    public ModelHandler(OptiqConnection connection, String uri)
        throws IOException
    {
        super();
        this.connection = connection;
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        JsonRoot root;
        if (uri.startsWith("inline:")) {
            root = mapper.readValue(
                uri.substring("inline:".length()), JsonRoot.class);
        } else {
            root = mapper.readValue(new File(uri), JsonRoot.class);
        }
        visit(root);
    }

    public void visit(JsonRoot root) {
        push(schemaStack, connection.getRootSchema());
        for (JsonSchema schema : root.schemas) {
            schema.accept(this);
        }
        pop(schemaStack, connection.getRootSchema());
    }

    public void visit(JsonMapSchema jsonSchema) {
        final MutableSchema parentSchema = (MutableSchema) peek(schemaStack);
        final MapSchema schema =
            MapSchema.create(connection, parentSchema, jsonSchema.name);
        push(schemaStack, schema);
        for (JsonTable jsonTable : jsonSchema.tables) {
            jsonTable.accept(this);
        }
        pop(schemaStack, schema);
    }

    public void visit(JsonJdbcSchema jsonSchema) {
        JdbcSchema.create(
            connection,
            (MutableSchema) peek(schemaStack),
            dataSource(jsonSchema),
            jsonSchema.jdbcCatalog,
            jsonSchema.jdbcSchema,
            jsonSchema.name);
    }

    private DataSource dataSource(JsonJdbcSchema jsonJdbcSchema) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(jsonJdbcSchema.jdbcUrl);
        dataSource.setUsername(jsonJdbcSchema.jdbcUser);
        dataSource.setPassword(jsonJdbcSchema.jdbcPassword);
        return dataSource;
    }

    private <T> T peek(List<T> stack) {
        return stack.get(stack.size() - 1);
    }

    private <T> void push(List<T> stack, T element) {
        stack.add(element);
    }

    private <T> void pop(List<T> stack, T element) {
        assert stack.get(stack.size() - 1) == element;
        stack.remove(stack.size() - 1);
    }

    public void visit(JsonCustomTable jsonTable) {
        try {
            final MutableSchema parentSchema =
                (MutableSchema) peek(schemaStack);
            final Class clazz = Class.forName(jsonTable.factory);
            final TableFactory tableFactory =
                (TableFactory) clazz.newInstance();
            final Table table =
                tableFactory.create(
                    connection.getTypeFactory(),
                    parentSchema,
                    jsonTable.name,
                    jsonTable.operand,
                    null);
            parentSchema.addTable(jsonTable.name, table);
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating " + jsonTable, e);
        }
    }
}

// End ModelHandler.java
