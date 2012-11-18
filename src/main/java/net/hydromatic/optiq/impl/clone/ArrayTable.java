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
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelRecordType;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Implementation of table that reads rows from arrays, one per column.
 */
class ArrayTable<T>
    extends BaseQueryable<T>
    implements Table<T>
{
    private final Schema schema;
    private final List<Object> valueArrayList;
    private final int size;

    /** Creates an ArrayTable. */
    public ArrayTable(
        Schema schema,
        Type elementType,
        Expression expression,
        List<Object> valueArrayList,
        int size)
    {
        super(schema.getQueryProvider(), elementType, expression);
        this.schema = schema;
        this.valueArrayList = valueArrayList;
        this.size = size;

        assert ((RelRecordType) elementType).getRecordFields().size()
               == valueArrayList.size();

        // Check that all arrays are arrays (might be primitive arrays) and have
        // the same size.
        for (Object valueArray : valueArrayList) {
            int length = Array.getLength(valueArray);
            assert size == length;
        }
    }

    public DataContext getDataContext() {
        return schema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Enumerator<T> enumerator() {
        return new Enumerator() {
            final int rowCount = size;
            final int columnCount = valueArrayList.size();
            int i = -1;

            public Object[] current() {
                Object[] objects = new Object[columnCount];
                for (int j = 0; j < objects.length; j++) {
                    objects[j] = Array.get(valueArrayList.get(j), i);
                }
                return objects;
            }

            public boolean moveNext() {
                return (++i < rowCount);
            }

            public void reset() {
                i = -1;
            }
        };
    }
}

// End ArrayTable.java
