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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Schema schema element.
 *
 * @see JsonRoot Description of schema elements
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    defaultImpl = JsonMapSchema.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = JsonMapSchema.class, name = "map"),
    @JsonSubTypes.Type(value = JsonJdbcSchema.class, name = "jdbc"),
    @JsonSubTypes.Type(value = JsonCustomSchema.class, name = "custom")})
public abstract class JsonSchema {
    public String name;

    public abstract void accept(ModelHandler handler);
}

// End JsonSchema.java
