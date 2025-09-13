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
 * Table for TIGER Census Block Groups.
 * 
 * <p>Block groups are statistical divisions of census tracts, generally containing
 * between 600 and 3,000 people. They are the smallest geographic unit for which
 * the Census Bureau publishes sample data (ACS data). Block groups are crucial
 * for neighborhood-level demographic and economic analysis.
 */
public class TigerBlockGroupsTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerBlockGroupsTable.class);
  
  private final TigerDataDownloader downloader;
  
  public TigerBlockGroupsTable(TigerDataDownloader downloader) {
    this.downloader = downloader;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("bg_geoid", SqlTypeName.VARCHAR)        // 12-digit block group GEOID
        .add("state_fips", SqlTypeName.VARCHAR)      // 2-digit state FIPS code
        .add("county_fips", SqlTypeName.VARCHAR)     // 3-digit county FIPS code
        .add("tract_code", SqlTypeName.VARCHAR)      // 6-digit tract code
        .add("blkgrp", SqlTypeName.VARCHAR)          // 1-digit block group number
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
              rows = loadBlockGroupData();
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
          
          private List<Object[]> loadBlockGroupData() {
            List<Object[]> data = new ArrayList<>();
            
            try {
              // Check if Block Group data exists
              File bgDir = new File(downloader.getCacheDir(), "block_groups");
              if (!bgDir.exists() && downloader.isAutoDownload()) {
                LOGGER.info("Downloading TIGER Block Group data...");
                downloader.downloadBlockGroups();
              }
              
              if (bgDir.exists()) {
                LOGGER.info("Loading Block Group data from {}", bgDir);
                
                // Parse TIGER Block Group shapefiles (state-level files)
                File[] stateDirectories = bgDir.listFiles(File::isDirectory);
                if (stateDirectories != null) {
                  for (File stateDir : stateDirectories) {
                    LOGGER.debug("Processing block groups for state directory: {}", stateDir.getName());
                    
                    // Parse block group shapefile for this state (e.g., tl_2024_06_bg for California)
                    String stateCode = stateDir.getName();
                    String expectedPrefix = "tl_2024_" + stateCode + "_bg";
                    
                    List<Object[]> stateBlockGroups = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
                      String geoid = TigerShapefileParser.getStringAttribute(feature, "GEOID");
                      if (geoid.length() != 12) {
                        return null; // Skip invalid block group records
                      }
                      
                      String stateFips = geoid.substring(0, 2);
                      String countyFips = geoid.substring(2, 5);
                      String tractCode = geoid.substring(5, 11);
                      String blkgrp = geoid.substring(11, 12);
                      
                      return new Object[] {
                          geoid,                                                           // bg_geoid
                          stateFips,                                                      // state_fips
                          countyFips,                                                     // county_fips
                          tractCode,                                                      // tract_code
                          blkgrp,                                                         // blkgrp
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
                    
                    data.addAll(stateBlockGroups);
                  }
                }
              } else {
                LOGGER.warn("Block Group data not available. Run with autoDownload=true to fetch data.");
              }
            } catch (Exception e) {
              LOGGER.error("Error loading Block Group data", e);
            }
            
            return data;
          }
        };
      }
    };
  }
}