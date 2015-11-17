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
 * Provides model files, in JSON format, defining schemas and other metadata.
 *
 * <p>Models are specified using a <code>model=&lt;uri&gt;</code> parameter on
 * the JDBC connect string. Calcite loads the model while initializing the
 * connection. It first parses the JSON, then uses a
 * {@link org.apache.calcite.model.ModelHandler} as visitor over the parse
 * tree.</p>
 *
 * <p>There are standard implementations of schema and table, but the user can
 * provide their own by implementing the
 * {@link org.apache.calcite.schema.SchemaFactory}
 * or {@link org.apache.calcite.schema.TableFactory}
 * interfaces and including a custom schema in the model.</p>
 *
 * <p>There are several examples of schemas in the
 * <a href="https://calcite.apache.org/docs/tutorial.html">tutorial</a>.
 */
@PackageMarker
package org.apache.calcite.model;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
