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
package net.hydromatic.optiq.impl;

import net.hydromatic.linq4j.Enumerator;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.Prepare;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/**
 * Table whose contents are defined using an SQL statement.
 *
 * <p>It is not evaluated; it is expanded during query planning.</p>
 */
public class ViewTable<T>
    extends AbstractTable<T>
    implements TranslatableTable<T>
{
    private final String viewSql;
    private final List<String> schemaPath;

    public ViewTable(
        Schema schema,
        Type elementType,
        RelDataType relDataType,
        String tableName,
        String viewSql,
        List<String> schemaPath)
    {
        super(schema, elementType, relDataType, tableName);
        this.viewSql = viewSql;
        this.schemaPath = schemaPath;
    }

    /** Table function that returns a view. */
    public static <T> TableFunction<T> viewFunction(
        final Schema schema,
        final String name,
        final String viewSql,
        final List<String> schemaPath)
    {
        return new ViewTableFunction<T>(schema, name, viewSql, schemaPath);
    }

    public Enumerator<T> enumerator() {
        return schema
            .getQueryProvider()
            .<T>createQuery(getExpression(), elementType)
            .enumerator();
    }

    public RelNode toRel(
        RelOptTable.ToRelContext context,
        RelOptTable relOptTable)
    {
        return expandView(
            context.getPreparingStmt(),
            getRowType(),
            viewSql);
    }

    private RelNode expandView(
        Prepare preparingStmt,
        RelDataType rowType,
        String queryString)
    {
        try {
            RelNode rel =
                preparingStmt.expandView(rowType, queryString, schemaPath);

            rel = RelOptUtil.createCastRel(rel, rowType, true);
            rel = preparingStmt.flattenTypes(rel, false);
            return rel;
        } catch (Throwable e) {
            throw Util.newInternal(
                e, "Error while parsing view definition:  " + queryString);
        }
    }

    private static class ViewTableFunction<T> implements TableFunction<T> {
        private final String viewSql;
        private final Schema schema;
        private final String name;
        private final List<String> schemaPath;

        private ViewTableFunction(
            Schema schema,
            String name,
            String viewSql,
            List<String> schemaPath)
        {
            this.viewSql = viewSql;
            this.schema = schema;
            this.name = name;
            this.schemaPath = schemaPath;
        }

        public List<Parameter> getParameters() {
            return Collections.emptyList();
        }

        public Table<T> apply(List<Object> arguments) {
            OptiqPrepare.ParseResult parsed =
                OptiqPrepare.DEFAULT_FACTORY.apply().parse(
                    new OptiqPrepare.Context() {
                        public JavaTypeFactory getTypeFactory() {
                            return schema.getTypeFactory();
                        }

                        public Schema getRootSchema() {
                            return ((OptiqConnection) schema.getQueryProvider())
                                .getRootSchema();
                        }

                        public List<String> getDefaultSchemaPath() {
                            return schemaPath;
                        }
                    },
                    viewSql);
            return new ViewTable<T>(
                schema, schema.getTypeFactory().getJavaClass(parsed.rowType),
                parsed.rowType, name, viewSql, schemaPath);
        }

        public Type getElementType() {
            return apply(Collections.emptyList()).getElementType();
        }
    }
}

// End ViewTable.java
