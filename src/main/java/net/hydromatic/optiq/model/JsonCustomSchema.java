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

import java.util.Map;

/**
 * JSON schema element that represents a custom schema.
 *
 * @see net.hydromatic.optiq.model.JsonRoot Description of schema elements
 */
public class JsonCustomSchema extends JsonSchema {
    /** Name of the factory class for this schema. Must implement interface
     * {@link net.hydromatic.optiq.SchemaFactory} and have a public default
     * constructor. */
    public String factory;

    /** Operand. May be a JSON object (represented as Map) or null. */
    public Map<String, Object> operand;

    public void accept(ModelHandler handler) {
        handler.visit(this);
    }
}

// End JsonCustomSchema.java
