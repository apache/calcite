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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.govdata.sec.SecSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Government Data Schema Factory - Uber factory for government data sources.
 *
 * <p>This factory routes to specialized factories based on the 'dataSource' 
 * parameter. Supported data sources:
 * <ul>
 *   <li>sec - Securities and Exchange Commission (EDGAR filings)</li>
 *   <li>census - U.S. Census Bureau data (future)</li>
 *   <li>irs - Internal Revenue Service data (future)</li>
 *   <li>treasury - U.S. Treasury data (future)</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GOV",
 *   "schemas": [{
 *     "name": "GOV",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "sec",
 *       "ciks": ["AAPL", "MSFT"],
 *       "startYear": 2020,
 *       "endYear": 2023
 *     }
 *   }]
 * }
 * </pre>
 */
public class GovDataSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSchemaFactory.class);

  @Override public Schema create(SchemaPlus parentSchema, String name, 
      Map<String, Object> operand) {
    
    String dataSource = (String) operand.get("dataSource");
    
    // Default to SEC for backward compatibility if no dataSource specified
    if (dataSource == null) {
      dataSource = "sec";
      LOGGER.info("No dataSource specified, defaulting to 'sec'");
    }
    
    LOGGER.info("Creating government data schema for source: {}", dataSource);
    
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
        return createSecSchema(parentSchema, name, operand);
      
      case "census":
        throw new UnsupportedOperationException(
            "Census data source not yet implemented. Coming soon!");
        
      case "irs":
        throw new UnsupportedOperationException(
            "IRS data source not yet implemented. Coming soon!");
        
      case "treasury":
        throw new UnsupportedOperationException(
            "Treasury data source not yet implemented. Coming soon!");
        
      default:
        throw new IllegalArgumentException(
            "Unsupported government data source: '" + dataSource + "'. " +
            "Supported sources: sec, census (future), irs (future), treasury (future)");
    }
  }
  
  /**
   * Creates SEC/EDGAR schema using the specialized SEC factory.
   */
  private Schema createSecSchema(SchemaPlus parentSchema, String name, 
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to SecSchemaFactory for SEC/EDGAR data");
    return new SecSchemaFactory().create(parentSchema, name, operand);
  }
}