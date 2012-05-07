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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.optiq.MutableSchema;

import org.eigenbase.reltype.RelDataTypeFactory;

import java.sql.Connection;

/**
 * Extension to OPTIQ's implementation of
 * {@link java.sql.Connection JDBC connection} allows schemas to be defined
 * dynamically.
 *
 * <p>You can start off with an empty connection (no schemas), define one
 * or two schemas, and start querying them.</p>
 */
public interface OptiqConnection extends Connection {
    /**
     * Returns the root schema.
     *
     * <p>You can define objects (such as relations) in this schema, and
     * also nested schemas.</p>
     *
     * @return Root schema
     */
    MutableSchema getRootSchema();

    RelDataTypeFactory getTypeFactory();
}

// End OptiqConnection.java
