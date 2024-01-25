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
package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinRel;
import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GremlinTableBase extends AbstractQueryableTable implements TranslatableTable {
    private final String label;
    private final Boolean isVertex;
    private final Map<String, GremlinProperty> columns;
    public static final String ID = "_ID";
    public static final String IN_ID = "_IN" + ID;
    public static final String OUT_ID = "_OUT" + ID;

    public GremlinTableBase(final String label, final Boolean isVertex,
                            final Map<String, GremlinProperty> columns) {
        super(Object[].class);
        this.label = label;
        this.isVertex = isVertex;
        this.columns = columns;
    }

    public String getLabel() {
      return label;
    }

    public Boolean getIsVertex() {
      return isVertex;
    }

    public Map<String, GremlinProperty> getColumns() {
      return columns;
    }

  public GremlinProperty getColumn(final String column) throws SQLException {
        for (final String key : columns.keySet()) {
            if (key.equalsIgnoreCase(column)) {
                return columns.get(key);
            }
        }
        throw new SQLException(
            String.format(
                "Error: Could not find column '%s' on %s with label '%s'.", column, isVertex ? "vertex" : "edge", label));
    }

    @Override public <T> Queryable<T> asQueryable(final QueryProvider queryProvider, final SchemaPlus schema, final String tableName) {
        return null;
    }

    @Override public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
        final int[] fields = new int[columns.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = i;
        }
        return new GremlinTableScan(context.getCluster(), context.getCluster().traitSetOf(GremlinRel.CONVENTION), relOptTable, fields);
    }

    @Override public RelDataType getRowType(final RelDataTypeFactory relDataTypeFactory) {
        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();
        for (final Map.Entry<String, GremlinProperty> entry : columns.entrySet()) {
            names.add(entry.getKey());
            types.add(relDataTypeFactory.createJavaType(getType(entry.getValue().getType())));
        }
        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }

    private Class<?> getType(final String className) {
        if ("string".equalsIgnoreCase(className)) {
            return String.class;
        } else if ("integer".equalsIgnoreCase(className)) {
            return Integer.class;
        } else if ("float".equalsIgnoreCase(className)) {
            return Float.class;
        } else if ("byte".equalsIgnoreCase(className)) {
            return Byte.class;
        } else if ("short".equalsIgnoreCase(className)) {
            return Short.class;
        } else if ("double".equalsIgnoreCase(className)) {
            return Double.class;
        } else if ("long".equalsIgnoreCase(className)) {
            return Long.class;
        } else if ("boolean".equalsIgnoreCase(className)) {
            return Boolean.class;
        } else if ("date".equalsIgnoreCase(className) || "long_date".equalsIgnoreCase(className)) {
            return java.sql.Date.class;
        } else if ("timestamp".equalsIgnoreCase(className) || "long_timestamp".equalsIgnoreCase(className)) {
            return java.sql.Timestamp.class;
        } else {
            return null;
        }
    }
}
