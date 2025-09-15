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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts TIGER/Line shapefiles to Parquet format.
 * 
 * Since shapefiles consist of multiple files (.shp, .dbf, .shx, etc.),
 * this converter focuses on extracting attribute data from the .dbf file
 * and converting it to Parquet. Geometry is stored as WKT strings for now.
 */
public class ShapefileToParquetConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShapefileToParquetConverter.class);
  
  /**
   * Convert shapefiles in a directory to Parquet format.
   */
  public void convertShapefilesToParquet(File sourceDir, File targetDir) throws IOException {
    if (!sourceDir.exists() || !sourceDir.isDirectory()) {
      LOGGER.warn("Source directory does not exist: {}", sourceDir);
      return;
    }
    
    targetDir.mkdirs();
    
    // Process each year directory
    File[] yearDirs = sourceDir.listFiles(f -> f.isDirectory() && f.getName().startsWith("year="));
    if (yearDirs == null || yearDirs.length == 0) {
      LOGGER.info("No year directories found in {}", sourceDir);
      return;
    }
    
    for (File yearDir : yearDirs) {
      String year = yearDir.getName().substring(5); // Remove "year=" prefix
      File targetYearDir = new File(targetDir, yearDir.getName());
      targetYearDir.mkdirs();
      
      // Convert states shapefile
      convertStatesShapefile(yearDir, targetYearDir, year);
      
      // Convert counties shapefile
      convertCountiesShapefile(yearDir, targetYearDir, year);
      
      // Convert places shapefile
      convertPlacesShapefile(yearDir, targetYearDir, year);
      
      // Convert ZCTAs shapefile
      convertZctasShapefile(yearDir, targetYearDir, year);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void convertStatesShapefile(File yearDir, File targetDir, String year) {
    try {
      File statesDir = new File(yearDir, "states");
      if (!statesDir.exists()) {
        LOGGER.debug("No states directory found in {}", yearDir);
        return;
      }
      
      // Create schema with TIGER state attributes
      Schema schema = SchemaBuilder.record("State")
          .fields()
          .name("state_fips").type().stringType().noDefault()
          .name("state_code").type().stringType().noDefault()
          .name("state_name").type().stringType().noDefault()
          .name("state_abbr").type().nullable().stringType().noDefault()
          .name("land_area").type().nullable().doubleType().noDefault()
          .name("water_area").type().nullable().doubleType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      File outputFile = new File(targetDir, "states.parquet");
      
      // Use TigerShapefileParser to parse real shapefile data
      String expectedPrefix = "tl_" + year + "_us_state";
      List<Object[]> statesData = TigerShapefileParser.parseShapefile(statesDir, expectedPrefix, feature -> {
        String stateFips = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
        String stateCode = TigerShapefileParser.getStringAttribute(feature, "GEOID");
        String stateName = TigerShapefileParser.getStringAttribute(feature, "NAME");
        String stateAbbr = TigerShapefileParser.getStringAttribute(feature, "STUSPS");
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");
        
        return new Object[]{
            stateFips,   // state_fips
            stateCode,   // state_code  
            stateName,   // state_name
            stateAbbr,   // state_abbr
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });
      
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
          new Path(outputFile.getAbsolutePath()))
          .withSchema(schema)
          .build()) {
        
        for (Object[] stateData : statesData) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("state_fips", stateData[0]);
          record.put("state_code", stateData[1]);
          record.put("state_name", stateData[2]);
          record.put("state_abbr", stateData[3]);
          record.put("land_area", stateData[4]);
          record.put("water_area", stateData[5]);
          record.put("geometry", stateData[6]);
          writer.write(record);
        }
        
        LOGGER.info("Created states parquet file: {} with {} records from real TIGER data", 
            outputFile, statesData.size());
      }
      
    } catch (Exception e) {
      LOGGER.error("Error converting states shapefile", e);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void convertCountiesShapefile(File yearDir, File targetDir, String year) {
    try {
      File countiesDir = new File(yearDir, "counties");
      if (!countiesDir.exists()) {
        LOGGER.debug("No counties directory found in {}", yearDir);
        return;
      }
      
      // Create schema for counties
      Schema schema = SchemaBuilder.record("County")
          .fields()
          .name("county_fips").type().stringType().noDefault()
          .name("state_fips").type().stringType().noDefault()
          .name("county_name").type().stringType().noDefault()
          .name("county_code").type().nullable().stringType().noDefault()
          .name("land_area").type().nullable().doubleType().noDefault()
          .name("water_area").type().nullable().doubleType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      File outputFile = new File(targetDir, "counties.parquet");
      
      // Use TigerShapefileParser to parse real shapefile data
      String expectedPrefix = "tl_" + year + "_us_county";
      List<Object[]> countiesData = TigerShapefileParser.parseShapefile(countiesDir, expectedPrefix, feature -> {
        String countyFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
        String stateFips = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
        String countyName = TigerShapefileParser.getStringAttribute(feature, "NAME");
        String countyCode = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");
        
        return new Object[]{
            countyFips,  // county_fips
            stateFips,   // state_fips
            countyName,  // county_name
            countyCode,  // county_code
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });
      
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
          new Path(outputFile.getAbsolutePath()))
          .withSchema(schema)
          .build()) {
        
        for (Object[] countyData : countiesData) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("county_fips", countyData[0]);
          record.put("state_fips", countyData[1]);
          record.put("county_name", countyData[2]);
          record.put("county_code", countyData[3]);
          record.put("land_area", countyData[4]);
          record.put("water_area", countyData[5]);
          record.put("geometry", countyData[6]);
          writer.write(record);
        }
        
        LOGGER.info("Created counties parquet file: {} with {} records from real TIGER data", 
            outputFile, countiesData.size());
      }
      
    } catch (Exception e) {
      LOGGER.error("Error converting counties shapefile", e);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void convertPlacesShapefile(File yearDir, File targetDir, String year) {
    try {
      File placesDir = new File(yearDir, "places");
      if (!placesDir.exists()) {
        LOGGER.debug("No places directory found in {}", yearDir);
        return;
      }
      
      // Create schema for places
      Schema schema = SchemaBuilder.record("Place")
          .fields()
          .name("place_fips").type().stringType().noDefault()
          .name("state_fips").type().stringType().noDefault()
          .name("place_name").type().stringType().noDefault()
          .name("place_type").type().nullable().stringType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      File outputFile = new File(targetDir, "places.parquet");
      
      // Process places for each state 
      List<Object[]> allPlacesData = new ArrayList<>();
      
      // Look for state directories within places
      File[] stateDirs = placesDir.listFiles(File::isDirectory);
      if (stateDirs != null) {
        for (File stateDir : stateDirs) {
          String stateFips = stateDir.getName();
          String expectedPrefix = "tl_" + year + "_" + stateFips + "_place";
          
          List<Object[]> statePlaces = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
            String placeFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
            String stateFipsAttr = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
            String placeName = TigerShapefileParser.getStringAttribute(feature, "NAME");
            String placeType = TigerShapefileParser.getStringAttribute(feature, "CLASSFP");
            
            return new Object[]{
                placeFips,      // place_fips
                stateFipsAttr,  // state_fips
                placeName,      // place_name
                placeType,      // place_type
                TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
            };
          });
          
          allPlacesData.addAll(statePlaces);
        }
      }
      
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
          new Path(outputFile.getAbsolutePath()))
          .withSchema(schema)
          .build()) {
        
        for (Object[] placeData : allPlacesData) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("place_fips", placeData[0]);
          record.put("state_fips", placeData[1]);
          record.put("place_name", placeData[2]);
          record.put("place_type", placeData[3]);
          record.put("geometry", placeData[4]);
          writer.write(record);
        }
        
        LOGGER.info("Created places parquet file: {} with {} records from real TIGER data", 
            outputFile, allPlacesData.size());
      }
      
    } catch (Exception e) {
      LOGGER.error("Error converting places shapefile", e);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void convertZctasShapefile(File yearDir, File targetDir, String year) {
    try {
      File zctasDir = new File(yearDir, "zctas");
      if (!zctasDir.exists()) {
        LOGGER.debug("No zctas directory found in {}", yearDir);
        return;
      }
      
      // Create schema for ZCTAs
      Schema schema = SchemaBuilder.record("ZCTA")
          .fields()
          .name("zcta").type().stringType().noDefault()
          .name("land_area").type().nullable().doubleType().noDefault()
          .name("water_area").type().nullable().doubleType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      File outputFile = new File(targetDir, "zctas.parquet");
      
      // Use TigerShapefileParser to parse real ZCTA shapefile data
      String expectedPrefix = "tl_" + year + "_us_zcta520";
      List<Object[]> zctasData = TigerShapefileParser.parseShapefile(zctasDir, expectedPrefix, feature -> {
        // Try different attribute names used across ZCTA years
        String zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE20");
        if (zcta5.isEmpty()) {
          zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE10");
        }
        if (zcta5.isEmpty()) {
          zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE");
        }
        
        // Try different land/water area attribute names
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND20");
        if (landArea == 0.0) {
          landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND10");
        }
        if (landArea == 0.0) {
          landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        }
        
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER20");
        if (waterArea == 0.0) {
          waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER10");
        }
        if (waterArea == 0.0) {
          waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");
        }
        
        return new Object[]{
            zcta5,       // zcta
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });
      
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
          new Path(outputFile.getAbsolutePath()))
          .withSchema(schema)
          .build()) {
        
        for (Object[] zctaData : zctasData) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("zcta", zctaData[0]);
          record.put("land_area", zctaData[1]);
          record.put("water_area", zctaData[2]);
          record.put("geometry", zctaData[3]);
          writer.write(record);
        }
        
        LOGGER.info("Created zctas parquet file: {} with {} records from real TIGER data", 
            outputFile, zctasData.size());
      }
      
    } catch (Exception e) {
      LOGGER.error("Error converting zctas shapefile", e);
    }
  }
  
  @SuppressWarnings("deprecation")
  private void convertStatesCsvToParquet(File csvFile, File outputFile) throws IOException {
    // If a CSV export is available, convert it to Parquet
    LOGGER.info("Converting CSV file {} to Parquet", csvFile);
    
    Schema schema = SchemaBuilder.record("State")
        .fields()
        .name("state_fips").type().stringType().noDefault()
        .name("state_code").type().stringType().noDefault()
        .name("state_name").type().stringType().noDefault()
        .name("state_abbr").type().nullable().stringType().noDefault()
        .name("land_area").type().nullable().doubleType().noDefault()
        .name("water_area").type().nullable().doubleType().noDefault()
        .name("geometry").type().nullable().stringType().noDefault()
        .endRecord();
    
    try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));
         ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
             new Path(outputFile.getAbsolutePath()))
             .withSchema(schema)
             .build()) {
      
      String line = reader.readLine(); // Skip header
      int count = 0;
      
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length >= 3) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("state_fips", parts[0].trim());
          record.put("state_code", parts.length > 1 ? parts[1].trim() : "");
          record.put("state_name", parts.length > 2 ? parts[2].trim() : "");
          record.put("state_abbr", parts.length > 3 ? parts[3].trim() : null);
          record.put("land_area", null);
          record.put("water_area", null);
          record.put("geometry", null);
          writer.write(record);
          count++;
        }
      }
      
      LOGGER.info("Converted {} records from CSV to Parquet", count);
    }
  }
  
}