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

import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Schema factory for ubiquitous public data sources.
 *
 * <p>Provides access to public knowledge and geographic data from:
 * <ul>
 *   <li>Wikipedia - Encyclopedia articles, entity data, and knowledge extraction</li>
 *   <li>OpenStreetMap - Detailed geographic data, buildings, transportation, amenities</li>
 *   <li>Wikidata - Structured knowledge base with entity relationships</li>
 *   <li>Academic Research - Publications, patents, institutional data</li>
 *   <li>Entity Resolution - Cross-reference linking between all data sources</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "PUB",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.pub.PubSchemaFactory",
 *     "operand": {
 *       "wikipediaLanguages": ["en", "es", "fr"],
 *       "osmRegions": ["us", "canada", "mexico"],
 *       "wikidataEndpoint": "https://query.wikidata.org/sparql",
 *       "openalexApiKey": "${OPENALEX_API_KEY}",
 *       "updateFrequency": "daily",
 *       "entityLinking": {
 *         "enabled": true,
 *         "confidenceThreshold": 0.8
 *       },
 *       "spatialAnalysis": {
 *         "enabled": true,
 *         "bufferAnalysis": ["100m", "500m", "1km"]
 *       },
 *       "cacheDirectory": "${PUB_CACHE_DIR:/tmp/pub-cache}"
 *     }
 *   }]
 * }
 * </pre>
 */
public class PubSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSchemaFactory.class);

  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.info("Creating public data schema: {}", name);
    
    // Extract configuration
    @SuppressWarnings("unchecked")
    List<String> wikipediaLanguages = (List<String>) operand.get("wikipediaLanguages");
    if (wikipediaLanguages == null) {
      wikipediaLanguages = java.util.Arrays.asList("en"); // Default to English
    }
    
    @SuppressWarnings("unchecked")
    List<String> osmRegions = (List<String>) operand.get("osmRegions");
    if (osmRegions == null) {
      osmRegions = java.util.Arrays.asList("us"); // Default to United States
    }
    
    String wikidataEndpoint = (String) operand.getOrDefault("wikidataEndpoint", 
        "https://query.wikidata.org/sparql");
    String openalexApiKey = (String) operand.get("openalexApiKey");
    String updateFrequency = (String) operand.getOrDefault("updateFrequency", "daily");
    String cacheDirectory = (String) operand.get("cacheDirectory");
    
    @SuppressWarnings("unchecked")
    Map<String, Object> entityLinking = (Map<String, Object>) operand.get("entityLinking");
    
    @SuppressWarnings("unchecked")
    Map<String, Object> spatialAnalysis = (Map<String, Object>) operand.get("spatialAnalysis");
    
    LOGGER.debug("Wikipedia languages: {}", wikipediaLanguages);
    LOGGER.debug("OSM regions: {}", osmRegions);
    LOGGER.debug("Wikidata endpoint: {}", wikidataEndpoint);
    LOGGER.debug("Update frequency: {}", updateFrequency);
    
    if (entityLinking != null && Boolean.TRUE.equals(entityLinking.get("enabled"))) {
      Double threshold = (Double) entityLinking.get("confidenceThreshold");
      LOGGER.debug("Entity linking enabled with confidence threshold: {}", 
          threshold != null ? threshold : 0.8);
    }
    
    if (spatialAnalysis != null && Boolean.TRUE.equals(spatialAnalysis.get("enabled"))) {
      @SuppressWarnings("unchecked")
      List<String> buffers = (List<String>) spatialAnalysis.get("bufferAnalysis");
      LOGGER.debug("Spatial analysis enabled with buffer distances: {}", buffers);
    }
    
    // Create the schema with configured sources
    return new PubSchema(parentSchema, name, operand);
  }

  @Override public boolean supportsConstraints() {
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}