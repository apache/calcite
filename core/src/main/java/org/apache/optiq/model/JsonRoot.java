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
package org.apache.optiq.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Root schema element.
 *
 * <p>A POJO with fields of {@link Boolean}, {@link String}, {@link ArrayList},
 * {@link java.util.LinkedHashMap}, per Jackson simple data binding.</p>
 *
 * <p>Schema structure is as follows:</p>
 *
 * <pre>{@code Root}
 *   {@link JsonSchema} (in collection {@link JsonRoot#schemas schemas})
 *     {@link JsonTable} (in collection {@link JsonMapSchema#tables tables})
 *       {@link JsonColumn} (in collection {@link JsonTable#columns column}
 *     {@link JsonView}
 *     {@link JsonFunction}  (in collection {@link JsonMapSchema#functions functions})
 * </pre>
 */
public class JsonRoot {
  public String version;
  public String defaultSchema;
  public final List<JsonSchema> schemas = new ArrayList<JsonSchema>();
}

// End JsonRoot.java
