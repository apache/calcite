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

import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.*;

import java.util.List;

/**
 * Implementation of {@link net.hydromatic.optiq.Schema} that delegates to an
 * underlying schema.
 *
 * @author jhyde
 */
public class DelegatingSchema implements Schema {
    protected final Schema schema;

    /**
     * Creates a DelegatingSchema.
     *
     * @param schema Underlying schema
     */
    public DelegatingSchema(Schema schema) {
        this.schema = schema;
    }

    public Table getTable(String name) {
        return schema.getTable(name);
    }

    public <T> Queryable<T> getTable(String name, Class<T> elementType) {
        return schema.getTable(name, elementType);
    }

    public List<TableFunction> getTableFunctions(String name) {
        return schema.getTableFunctions(name);
    }

    public Schema getSubSchema(String name) {
        return schema.getSubSchema(name);
    }

}

// End DelegatingSchema.java
