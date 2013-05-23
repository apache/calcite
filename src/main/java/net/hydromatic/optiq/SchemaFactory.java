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
package net.hydromatic.optiq;

import java.util.Map;

/**
 * Factory for {@link net.hydromatic.optiq.Schema} objects.
 *
 * <p>A class that implements SchemaFactory specified in a schema must have a
 * public default constructor.</p>
 */
public interface SchemaFactory {
    /** Creates a Schema.
     *
     * <p>The implementation must register the schema in the parent schema,
     * by calling {@link MutableSchema#addSchema(String, Schema)}.</p>
     *
     * @param schema Parent schema
     * @param name Name of this schema
     * @param operand The "operand" JSON property
     */
    Schema create(
        MutableSchema schema, String name, Map<String, Object> operand);
}

// End SchemaFactory.java
