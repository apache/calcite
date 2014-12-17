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

/**
 * Calcite adapters.
 *
 * <p>An adapter allows Calcite to access data in a particular data source as
 * if it were a collection of tables in a schema. Each adapter typically
 * contains an implementation of {@link org.apache.calcite.schema.SchemaFactory}
 * and some classes that implement other schema SPIs.
 *
 * <p>To use an adapter, include a custom schema in a JSON model file:
 *
 * <blockquote><pre>
 *    schemas: [
 *      {
 *        type: 'custom',
 *        name: 'My Custom Schema',
 *        factory: 'com.acme.MySchemaFactory',
 *        operand: {a: 'foo', b: [1, 3.5] }
 *      }
 *   ]
 * </pre>
 * </blockquote>
 */
@PackageMarker
package org.apache.calcite.adapter;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
