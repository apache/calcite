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
package net.hydromatic.optiq.impl.java;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.MethodCallExpression;
import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Implementation of {@link Schema} backed by a {@link HashMap}.
 *
 * @author jhyde
 */
public class MapSchema implements MutableSchema {
    private static final Method MAP_GET_METHOD;

    static {
        try {
            MAP_GET_METHOD = Map.class.getMethod("get", Object.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    protected final Map<String, Table<Object>> tableMap =
        new HashMap<String, Table<Object>>();

    protected final Map<String, List<TableFunction>> membersMap =
        new HashMap<String, List<TableFunction>>();

    protected final Map<String, Schema> subSchemaMap =
        new HashMap<String, Schema>();

    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;

    public MapSchema(QueryProvider queryProvider, JavaTypeFactory typeFactory) {
        this.queryProvider = queryProvider;
        this.typeFactory = typeFactory;
    }

    public QueryProvider getQueryProvider() {
        return queryProvider;
    }

    public <T> Table<T> getTable(String name, Class<T> elementType) {
        // TODO: check elementType matches table.elementType
        assert elementType != null;
        return getTable(name);
    }

    public Table getTable(String name) {
        return tableMap.get(name);
    }

    public List<TableFunction> getTableFunctions(String name) {
        List<TableFunction> members = membersMap.get(name);
        if (members != null) {
            return members;
        }
        return Collections.emptyList();
    }

    public Schema getSubSchema(String name) {
        return subSchemaMap.get(name);
    }

    public void addTableFunction(String name, TableFunction tableFunction) {
        putMulti(membersMap, name, tableFunction);
    }

    public void addTable(String name, Table table) {
        tableMap.put(name, table);
    }

    public void add(String name, Schema schema) {
        subSchemaMap.put(name, schema);
    }

    private Expression getMemberExpression(
        Expression schemaExpression, Member member, List<Expression> arguments)
    {
        // (Type) schemaExpression.get("name")
        Expression call =
            Expressions.call(
                schemaExpression,
                MAP_GET_METHOD,
                Collections.<Expression>singletonList(
                    Expressions.constant(member.getName())));
        Type type = deduceType(member);
        if (type != null && type != Object.class) {
            return Expressions.convert_(call, type);
        }
        return call;
    }

    public Object getSubSchemaInstance(
        Object schemaInstance,
        String subSchemaName,
        Schema subSchema)
    {
        throw new UnsupportedOperationException();
    }

    private Expression getSubSchemaExpression(
        Expression schemaExpression, Schema schema, String name)
    {
        MethodCallExpression call =
            Expressions.call(
                schemaExpression,
                MAP_GET_METHOD,
                Expressions.constant(name));
        Object o = schema;
        Type clazz = deduceType(o);
        if (clazz != null && clazz != Object.class) {
            return Expressions.convert_(call, clazz);
        }
        return call;
    }

    private List<RelDataType> convert(List<Type> types) {
        ArrayList<RelDataType> list = new ArrayList<RelDataType>();
        for (Type type : types) {
            list.add(typeFactory.createJavaType((Class) type));
        }
        return list;
    }

    private Type deduceType(Object schemaObject) {
        // REVIEW: Can we remove the dependency on RelDataType and work in
        //   terms of Class?
        if (schemaObject instanceof Member) {
            RelDataType type = ((Member) schemaObject).getType();
            return typeFactory.getJavaClass(type);
        }
        if (schemaObject instanceof Schema) {
            return schemaObject.getClass();
        }
        return null;
    }

    protected static <K, V> void putMulti(
        Map<K, List<V>> map, K k, V v)
    {
        List<V> list = map.put(k, Collections.singletonList(v));
        if (list == null) {
            return;
        }
        if (list.size() == 1) {
            list = new ArrayList<V>(list);
        }
        list.add(v);
        map.put(k, list);
    }
}

// End MapSchema.java
