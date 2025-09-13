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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Table for HUD ZIP Code to CBSA Division crosswalk.
 * 
 * <p>Provides mapping between ZIP codes and CBSA Divisions (Metropolitan
 * Divisions within large Metropolitan Statistical Areas). This crosswalk
 * enables analysis at the sub-metropolitan level for large urban areas.
 */
public class HudZipCbsaDivTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HudZipCbsaDivTable.class);
  
  private final HudCrosswalkFetcher fetcher;
  
  public HudZipCbsaDivTable(HudCrosswalkFetcher fetcher) {
    this.fetcher = fetcher;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("zip", SqlTypeName.VARCHAR)             // 5-digit ZIP code
        .add("cbsadiv", SqlTypeName.VARCHAR)         // CBSA Division code
        .add("cbsadiv_name", SqlTypeName.VARCHAR)    // CBSA Division name
        .add("cbsa", SqlTypeName.VARCHAR)            // Parent CBSA code
        .add("cbsa_name", SqlTypeName.VARCHAR)       // Parent CBSA name
        .add("res_ratio", SqlTypeName.DOUBLE)        // Residential address ratio
        .add("bus_ratio", SqlTypeName.DOUBLE)        // Business address ratio
        .add("oth_ratio", SqlTypeName.DOUBLE)        // Other address ratio
        .add("tot_ratio", SqlTypeName.DOUBLE)        // Total address ratio
        .add("usps_city", SqlTypeName.VARCHAR)       // USPS city name
        .add("state_code", SqlTypeName.VARCHAR)      // 2-letter state code
        .build();
  }
  
  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new Enumerator<Object[]>() {
          private List<Object[]> rows;
          private int currentIndex = -1;
          
          @Override public Object[] current() {
            return rows != null && currentIndex >= 0 && currentIndex < rows.size() 
                ? rows.get(currentIndex) : null;
          }
          
          @Override public boolean moveNext() {
            if (rows == null) {
              rows = loadZipCbsaDivData();
            }
            currentIndex++;
            return currentIndex < rows.size();
          }
          
          @Override public void reset() {
            currentIndex = -1;
          }
          
          @Override public void close() {
            // Nothing to close
          }
          
          private List<Object[]> loadZipCbsaDivData() {
            List<Object[]> data = new ArrayList<>();
            
            try {
              if (fetcher != null) {
                LOGGER.info("Loading HUD ZIP to CBSA crosswalk data for Q2 2024");
                
                // Download ZIP-CBSA crosswalk data from HUD API
                java.io.File csvFile = fetcher.downloadZipToCbsa("2", 2024);
                
                // Load the crosswalk records from the CSV file
                List<HudCrosswalkFetcher.CrosswalkRecord> records = fetcher.loadCrosswalkData(csvFile);
                
                // Convert to table format - filter for CBSA Division data
                for (HudCrosswalkFetcher.CrosswalkRecord record : records) {
                  // CBSAs with divisions have separate division codes
                  // For now, use CBSA code as division code (may need enhancement)
                  data.add(new Object[] {
                      record.zip,              // zip
                      record.geoCode,          // cbsadiv (using CBSA code)
                      "CBSA " + record.geoCode, // cbsadiv_name (constructed name)
                      record.geoCode,          // cbsa (parent CBSA code)
                      "CBSA " + record.geoCode, // cbsa_name (constructed name)
                      record.resRatio,         // res_ratio
                      record.busRatio,         // bus_ratio
                      record.othRatio,         // oth_ratio
                      record.totRatio,         // tot_ratio
                      record.city,             // usps_city
                      record.state             // state_code
                  });
                }
                
                LOGGER.info("Loaded {} ZIP-CBSA crosswalk records", data.size());
                
              } else {
                LOGGER.warn("HUD fetcher not configured. Cannot load ZIP-CBSA Division crosswalk.");
              }
            } catch (Exception e) {
              LOGGER.error("Error loading ZIP-CBSA crosswalk data", e);
            }
            
            return data;
          }
        };
      }
    };
  }
}