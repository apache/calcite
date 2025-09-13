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
 * Table for TIGER Core Based Statistical Areas (CBSAs).
 * 
 * <p>CBSAs are geographic entities associated with at least one core 
 * (urbanized area or urban cluster) of at least 10,000 population, plus
 * adjacent counties having a high degree of social and economic integration
 * with the core. CBSAs include both Metropolitan Statistical Areas (50,000+)
 * and Micropolitan Statistical Areas (10,000-49,999).
 */
public class TigerCbsaTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerCbsaTable.class);
  
  private final TigerDataDownloader downloader;
  
  public TigerCbsaTable(TigerDataDownloader downloader) {
    this.downloader = downloader;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("cbsa_code", SqlTypeName.VARCHAR)       // 5-digit CBSA code
        .add("cbsa_name", SqlTypeName.VARCHAR)       // CBSA name
        .add("namelsad", SqlTypeName.VARCHAR)        // Name and legal/statistical description
        .add("lsad", SqlTypeName.VARCHAR)            // Legal/statistical area description code
        .add("memi", SqlTypeName.VARCHAR)            // Metropolitan/Micropolitan indicator
        .add("mtfcc", SqlTypeName.VARCHAR)           // MAF/TIGER feature class code
        .add("land_area", SqlTypeName.DOUBLE)        // Land area in square meters
        .add("water_area", SqlTypeName.DOUBLE)       // Water area in square meters
        .add("intpt_lat", SqlTypeName.DOUBLE)        // Internal point latitude
        .add("intpt_lon", SqlTypeName.DOUBLE)        // Internal point longitude
        .add("cbsa_type", SqlTypeName.VARCHAR)       // Metropolitan or Micropolitan
        .add("population", SqlTypeName.INTEGER)      // Total population
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
              rows = loadCbsaData();
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
          
          private List<Object[]> loadCbsaData() {
            List<Object[]> data = new ArrayList<>();
            
            try {
              // Check if CBSA data exists
              File cbsaDir = new File(downloader.getCacheDir(), "cbsa");
              if (!cbsaDir.exists() && downloader.isAutoDownload()) {
                LOGGER.info("Downloading TIGER CBSA data...");
                downloader.downloadCbsas();
              }
              
              if (cbsaDir.exists()) {
                LOGGER.info("Loading CBSA data from {}", cbsaDir);
                
                // Parse TIGER CBSA shapefile (national-level file)
                data = TigerShapefileParser.parseShapefile(cbsaDir, "tl_2024_us_cbsa", feature -> {
                  String cbsaCode = TigerShapefileParser.getStringAttribute(feature, "CBSAFP");
                  if (cbsaCode.isEmpty()) {
                    return null; // Skip invalid records
                  }
                  
                  String memi = TigerShapefileParser.getStringAttribute(feature, "MEMI");
                  String cbsaType = "1".equals(memi) ? "Metropolitan" : "Micropolitan";
                  
                  return new Object[] {
                      cbsaCode,                                                        // cbsa_code
                      TigerShapefileParser.getStringAttribute(feature, "NAME"),       // cbsa_name
                      TigerShapefileParser.getStringAttribute(feature, "NAMELSAD"),  // namelsad
                      TigerShapefileParser.getStringAttribute(feature, "LSAD"),       // lsad
                      memi,                                                           // memi
                      TigerShapefileParser.getStringAttribute(feature, "MTFCC"),      // mtfcc
                      TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),      // land_area
                      TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),     // water_area
                      TigerShapefileParser.getDoubleAttribute(feature, "INTPTLAT"),   // intpt_lat
                      TigerShapefileParser.getDoubleAttribute(feature, "INTPTLON"),   // intpt_lon
                      cbsaType,                                                       // cbsa_type
                      0,  // population - not in TIGER file, would need separate census data
                      TigerShapefileParser.getDoubleAttribute(feature, "ALAND") / 2589988.110336,   // aland_sqmi
                      TigerShapefileParser.getDoubleAttribute(feature, "AWATER") / 2589988.110336   // awater_sqmi
                  };
                });
              } else {
                LOGGER.warn("CBSA data not available. Run with autoDownload=true to fetch data.");
              }
            } catch (Exception e) {
              LOGGER.error("Error loading CBSA data", e);
            }
            
            return data;
          }
        };
      }
    };
  }
}