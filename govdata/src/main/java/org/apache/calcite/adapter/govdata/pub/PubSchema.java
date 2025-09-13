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
package org.apache.calcite.adapter.govdata.pub;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Public data schema implementation.
 * 
 * <p>Provides SQL access to ubiquitous public data sources including Wikipedia,
 * OpenStreetMap, Wikidata, and academic research databases. Enables comprehensive
 * entity resolution and knowledge graph analysis across all public information sources.
 */
public class PubSchema extends FileSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSchema.class);

  @SuppressWarnings("unchecked")
  public PubSchema(Schema parentSchema, String name, Map<String, Object> operand) {
    // Cast parentSchema to SchemaPlus which FileSchema expects
    super((SchemaPlus) parentSchema, name,
          getSourceDirectory(operand),
          (List<Map<String, Object>>) operand.get("tables"));
    LOGGER.debug("PubSchema created with name: {}", name);
  }

  private static File getSourceDirectory(Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    if (directory != null) {
      return new File(directory);
    }
    
    // Check for explicit cache directory
    String cacheDirectory = (String) operand.get("cacheDirectory");
    if (cacheDirectory != null) {
      return new File(cacheDirectory);
    }
    
    // Use default public data cache directory
    String cacheHome = System.getenv("PUB_CACHE_HOME");
    if (cacheHome == null) {
      cacheHome = System.getProperty("user.home") + "/.calcite/pub-cache";
    }
    return new File(cacheHome);
  }

  @Override public @Nullable String getComment() {
    return "Ubiquitous public data including Wikipedia encyclopedia content, "
        + "OpenStreetMap detailed geographic data, Wikidata structured knowledge base, "
        + "and academic research publications with comprehensive entity resolution. "
        + "Provides contextual intelligence and background information that enriches "
        + "government data analysis with historical context, biographical information, "
        + "institutional relationships, and precise geographic details. "
        + "Enables natural language queries and cross-domain knowledge discovery "
        + "across the complete spectrum of publicly available information.";
  }
}