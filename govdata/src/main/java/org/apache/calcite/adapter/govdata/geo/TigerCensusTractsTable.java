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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Table for TIGER Census Tracts.
 * 
 * <p>Census tracts are small, relatively permanent statistical subdivisions
 * of a county or statistically equivalent entity. They generally have a
 * population between 1,200 and 8,000 people, with an optimum size of 4,000.
 * Census tracts are key geographic units for detailed demographic analysis.
 */
public class TigerCensusTractsTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerCensusTractsTable.class);
  
  private final TigerDataDownloader downloader;
  
  public TigerCensusTractsTable(TigerDataDownloader downloader) {
    this.downloader = downloader;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("tract_geoid", SqlTypeName.VARCHAR)     // 11-digit census tract GEOID
        .add("state_fips", SqlTypeName.VARCHAR)      // 2-digit state FIPS code
        .add("county_fips", SqlTypeName.VARCHAR)     // 3-digit county FIPS code  
        .add("tract_code", SqlTypeName.VARCHAR)      // 6-digit tract code
        .add("tract_name", SqlTypeName.VARCHAR)      // Tract name (usually numeric)
        .add("namelsad", SqlTypeName.VARCHAR)        // Name and legal/statistical description
        .add("mtfcc", SqlTypeName.VARCHAR)           // MAF/TIGER feature class code
        .add("funcstat", SqlTypeName.VARCHAR)        // Functional status
        .add("land_area", SqlTypeName.DOUBLE)        // Land area in square meters
        .add("water_area", SqlTypeName.DOUBLE)       // Water area in square meters
        .add("intpt_lat", SqlTypeName.DOUBLE)        // Internal point latitude
        .add("intpt_lon", SqlTypeName.DOUBLE)        // Internal point longitude
        .add("population", SqlTypeName.INTEGER)      // Total population
        .add("housing_units", SqlTypeName.INTEGER)   // Total housing units
        .add("aland_sqmi", SqlTypeName.DOUBLE)       // Land area in square miles
        .add("awater_sqmi", SqlTypeName.DOUBLE)      // Water area in square miles
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
              rows = loadCensusTractData();
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
          
          private List<Object[]> loadCensusTractData() {
            List<Object[]> data = new ArrayList<>();
            
            try {
              // Check if Census Tract data exists
              File tractDir = new File(downloader.getCacheDir(), "census_tracts");
              if (!tractDir.exists() && downloader.isAutoDownload()) {
                LOGGER.info("Downloading TIGER Census Tract data...");
                downloader.downloadCensusTracts();
              }
              
              if (tractDir.exists()) {
                LOGGER.info("Loading Census Tract data from {}", tractDir);
                
                // Parse TIGER Census Tract shapefiles (state-level files)
                File[] stateDirectories = tractDir.listFiles(File::isDirectory);
                if (stateDirectories != null) {
                  for (File stateDir : stateDirectories) {
                    LOGGER.debug("Processing census tracts for state directory: {}", stateDir.getName());
                    
                    // Parse tract shapefile for this state (e.g., tl_2024_06_tract for California)
                    String stateCode = stateDir.getName();
                    String expectedPrefix = "tl_2024_" + stateCode + "_tract";
                    
                    List<Object[]> stateTracts = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
                      String geoid = TigerShapefileParser.getStringAttribute(feature, "GEOID");
                      if (geoid.length() != 11) {
                        return null; // Skip invalid tract records
                      }
                      
                      String stateFips = geoid.substring(0, 2);
                      String countyFips = geoid.substring(2, 5);
                      String tractCode = geoid.substring(5, 11);
                      
                      return new Object[] {
                          geoid,                                                           // tract_geoid
                          stateFips,                                                      // state_fips
                          countyFips,                                                     // county_fips
                          tractCode,                                                      // tract_code
                          TigerShapefileParser.getStringAttribute(feature, "NAME"),      // tract_name
                          TigerShapefileParser.getStringAttribute(feature, "NAMELSAD"), // namelsad
                          TigerShapefileParser.getStringAttribute(feature, "MTFCC"),     // mtfcc
                          TigerShapefileParser.getStringAttribute(feature, "FUNCSTAT"),  // funcstat
                          TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),     // land_area
                          TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),    // water_area
                          TigerShapefileParser.getDoubleAttribute(feature, "INTPTLAT"),  // intpt_lat
                          TigerShapefileParser.getDoubleAttribute(feature, "INTPTLON"),  // intpt_lon
                          0,  // population - not in TIGER file, would need separate census data
                          0,  // housing_units - not in TIGER file
                          TigerShapefileParser.getDoubleAttribute(feature, "ALAND") / 2589988.110336,   // aland_sqmi
                          TigerShapefileParser.getDoubleAttribute(feature, "AWATER") / 2589988.110336   // awater_sqmi
                      };
                    });
                    
                    data.addAll(stateTracts);
                  }
                }
              } else {
                LOGGER.warn("Census Tract data not available. Run with autoDownload=true to fetch data.");
              }
            } catch (Exception e) {
              LOGGER.error("Error loading Census Tract data", e);
            }
            
            return data;
          }
        };
      }
    };
  }
}