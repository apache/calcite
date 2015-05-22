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
package org.apache.calcite.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Root schema element.
 *
 * <p>A POJO with fields of {@link Boolean}, {@link String}, {@link ArrayList},
 * {@link java.util.LinkedHashMap LinkedHashMap}, per Jackson simple data
 * binding.</p>
 *
 * <p>Schema structure is as follows:</p>
 *
 * <!-- CHECKSTYLE: OFF -->
 * <pre>{@code Root}
 *   {@link JsonSchema} (in collection {@link JsonRoot#schemas schemas})
 *     {@link JsonTable} (in collection {@link JsonMapSchema#tables tables})
 *       {@link JsonColumn} (in collection {@link JsonTable#columns columns})
 *       {@link JsonStream} (in field {@link JsonTable#stream stream})
 *     {@link JsonView}
 *     {@link JsonFunction} (in collection {@link JsonMapSchema#functions functions})
 *     {@link JsonLattice} (in collection {@link JsonSchema#lattices lattices})
 *       {@link JsonMeasure} (in collection {@link JsonLattice#defaultMeasures defaultMeasures})
 *       {@link JsonTile} (in collection {@link JsonLattice#tiles tiles})
 *         {@link JsonMeasure} (in collection {@link JsonTile#measures measures})
 *     {@link JsonMaterialization} (in collection {@link JsonSchema#materializations materializations})
 * </pre>
 * <!-- CHECKSTYLE: ON -->
 */
public class JsonRoot {
  public String version;
  public String defaultSchema;
  public final List<JsonSchema> schemas = new ArrayList<JsonSchema>();
}

// End JsonRoot.java
