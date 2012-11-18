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

import org.eigenbase.reltype.*;
import org.eigenbase.util.Pair;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends MapSchema {
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final boolean JDK_17 =
        System.getProperty("java.version").startsWith("1.7");

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
        if (false) {
            // Old behavior: table based on list.
            return new ListTable<T>(
                this,
                sourceTable.getElementType(),
                Expressions.call(
                    getExpression(),
                    BuiltinMethod.SCHEMA_GET_TABLE.method,
                    Expressions.constant(name)),
                list);
        }
        // More efficient: table based on an array per column.
        final RelDataType elementType =
            (RelDataType) sourceTable.getElementType();
        final List<Object> valueArrayList = new ArrayList<Object>();
        final List<Type> types =
            new AbstractList<Type>()
            {
                final List<RelDataTypeField> fields =
                    elementType.getFieldList();
                public Type get(int index) {
                    return typeFactory.getJavaClass(
                        fields.get(index).getType());
                }

                public int size() {
                    return fields.size();
                }
            };
        for (Pair<Integer, Type> pair : Pair.zip(types)) {
            final int i = pair.left;
            final Primitive primitive = Primitive.of(pair.right);
            @SuppressWarnings("unchecked")
            final List<Object> sliceList =
                types.size() == 1
                    ? (List) list
                    : new AbstractList<Object>() {
                        public Object get(int index) {
                            return ((Object[]) list.get(index))[i];
                        }

                        public int size() {
                            return list.size();
                        }
                    };
            if (primitive == null) {
                valueArrayList.add(toArray(sliceList, true));
            } else {
                valueArrayList.add(primitive.toArray(sliceList));
            }
        }
        return new ArrayTable<T>(
            this,
            sourceTable.getElementType(),
            Expressions.call(
                getExpression(),
                BuiltinMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(name)),
            valueArrayList,
            list.size());
    }

    /** Converts a list to an array, optionally canonizing values which are
     * equal. */
    private <T> Object[] toArray(List<T> list, boolean canonize) {
        if (list.isEmpty()) {
            return EMPTY_OBJECT_ARRAY;
        }
        Object[] objects = list.toArray();
        if (canonize) {
            final Map<T, T> map = new HashMap<T, T>();
            for (int i = 0; i < objects.length; i++) {
                @SuppressWarnings("unchecked")
                T o = (T) objects[i];
                if (o != null) {
                    T t = map.get(o);
                    if (t == null) {
                        map.put(o, o);
                    } else {
                        objects[i] = t;
                    }
                }
            }
            // For string lists, optimize by letting all the strings share a
            // backing char array. Optimization does not work on JDK 1.7,
            // where String.substring creates a new char[].
            if (list.get(0) instanceof String
                && !JDK_17)
            {
                @SuppressWarnings("unchecked")
                final Map<String, String> stringMap = (Map<String, String>) map;
                String[] strings =
                    stringMap.keySet().toArray(new String[map.size()]);
                Arrays.sort(strings);
                StringBuilder buf = new StringBuilder(map.size() * 12);
                for (String string : strings) {
                    buf.append(string);
                }
                String bigString = buf.toString();
                int start = 0;
                for (String string : strings) {
                    int end = start + string.length();
                    stringMap.put(string, bigString.substring(start, end));
                    start = end;
                }
                for (int i = 0; i < objects.length; i++) {
                    if (objects[i] != null) {
                        //noinspection SuspiciousMethodCalls
                        objects[i] = map.get(objects[i]);
                    }
                }
                // For debugging, print some stats.
                if (false) {
                    int n = 0;
                    int nullCount = 0;
                    for (Object object : objects) {
                        if (object == null) {
                            ++nullCount;
                        } else {
                            n += ((String) object).length();
                        }
                    }
                    System.out.println(
                        "Strings: count=" + list.size()
                        + ", distinct=" + map.size()
                        + ", nulls=" + nullCount
                        + ", avg len=" + ((float) n / list.size())
                        + ", list=" + map.keySet()
                        + ", bigStringLength=" + bigString.length());
                }
            }
        }
        return objects;
    }

    /**
     * Creates a CloneSchema within another schema.
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
}

// End CloneSchema.java
