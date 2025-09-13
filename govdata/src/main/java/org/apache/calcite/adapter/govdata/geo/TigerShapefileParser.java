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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple utility class for parsing TIGER DBF files (without GeoTools dependency).
 * 
 * <p>Focuses on attribute data extraction from the .dbf component of shapefiles.
 * Does not parse geometric data but provides access to the tabular attributes.
 */
public class TigerShapefileParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerShapefileParser.class);
  
  /**
   * Parse a TIGER shapefile and extract features from DBF file.
   * 
   * @param shapefileDir Directory containing shapefile components
   * @param expectedPrefix Expected filename prefix (e.g., "tl_2024_us_zcta520")
   * @param attributeMapper Function to convert feature to Object array
   * @return List of parsed feature records
   */
  public static List<Object[]> parseShapefile(File shapefileDir, String expectedPrefix, 
      AttributeMapper attributeMapper) {
    List<Object[]> records = new ArrayList<>();
    
    try {
      // Find the .dbf file with expected prefix
      File dbfFile = findDbfFileByPrefix(shapefileDir, expectedPrefix);
      if (dbfFile == null) {
        LOGGER.warn("No DBF file found with prefix {} in {}", expectedPrefix, shapefileDir);
        return records;
      }
      
      LOGGER.info("Parsing DBF file: {}", dbfFile.getName());
      
      // Parse DBF file and extract records
      List<Map<String, Object>> dbfRecords = parseDbfFile(dbfFile);
      
      // Convert using attribute mapper
      for (Map<String, Object> record : dbfRecords) {
        Object[] mappedRecord = attributeMapper.mapAttributes(new SimpleDbfFeature(record));
        if (mappedRecord != null) {
          records.add(mappedRecord);
        }
      }
      
      LOGGER.info("Parsed {} features from {}", records.size(), dbfFile.getName());
      
    } catch (Exception e) {
      LOGGER.error("Error parsing shapefile in directory: " + shapefileDir, e);
    }
    
    return records;
  }
  
  /**
   * Find DBF file by prefix in directory.
   */
  private static File findDbfFileByPrefix(File dir, String prefix) {
    if (!dir.exists() || !dir.isDirectory()) {
      return null;
    }
    
    File[] files = dir.listFiles((d, name) -> 
        name.startsWith(prefix) && name.toLowerCase().endsWith(".dbf"));
    
    return (files != null && files.length > 0) ? files[0] : null;
  }
  
  /**
   * Parse DBF file and return list of attribute maps.
   */
  private static List<Map<String, Object>> parseDbfFile(File dbfFile) throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();
    
    try (FileInputStream fis = new FileInputStream(dbfFile)) {
      // Read DBF header
      byte[] headerBytes = new byte[32];
      fis.read(headerBytes);
      
      ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
      
      // Skip version info
      header.position(4);
      
      int recordCount = header.getInt();
      short headerLength = header.getShort();
      short recordLength = header.getShort();
      
      LOGGER.debug("DBF file has {} records, header length {}, record length {}", 
          recordCount, headerLength, recordLength);
      
      // Read field descriptors
      List<DbfField> fields = new ArrayList<>();
      byte[] fieldDescriptor = new byte[32];
      
      // Position at start of field descriptors (after 32-byte header)
      for (int i = 32; i < headerLength - 1; i += 32) {
        fis.read(fieldDescriptor);
        
        // Field name (first 11 bytes, null-terminated)
        String fieldName = new String(fieldDescriptor, 0, 11, StandardCharsets.US_ASCII)
            .split("\0")[0];
        
        // Field type (byte 11)
        char fieldType = (char) fieldDescriptor[11];
        
        // Field length (byte 16)
        int fieldLength = fieldDescriptor[16] & 0xFF;
        
        fields.add(new DbfField(fieldName, fieldType, fieldLength));
      }
      
      // Skip field terminator byte
      fis.read();
      
      LOGGER.debug("Found {} fields in DBF", fields.size());
      
      // Read data records
      byte[] recordData = new byte[recordLength];
      
      for (int i = 0; i < recordCount; i++) {
        int bytesRead = fis.read(recordData);
        if (bytesRead != recordLength) {
          break; // End of file or incomplete record
        }
        
        // Skip deleted records (first byte is '*')
        if (recordData[0] == '*') {
          continue;
        }
        
        Map<String, Object> record = new HashMap<>();
        int fieldOffset = 1; // Skip deletion flag
        
        for (DbfField field : fields) {
          if (fieldOffset + field.length <= recordLength) {
            byte[] fieldData = new byte[field.length];
            System.arraycopy(recordData, fieldOffset, fieldData, 0, field.length);
            
            String fieldValue = new String(fieldData, StandardCharsets.US_ASCII).trim();
            
            // Convert to appropriate type
            Object value = convertDbfValue(fieldValue, field.type);
            record.put(field.name, value);
            
            fieldOffset += field.length;
          }
        }
        
        records.add(record);
      }
    }
    
    return records;
  }
  
  /**
   * Convert DBF field value to appropriate Java type.
   */
  private static Object convertDbfValue(String value, char type) {
    if (value.isEmpty()) {
      return null;
    }
    
    switch (type) {
      case 'N': // Numeric
      case 'F': // Float
        try {
          if (value.contains(".")) {
            return Double.parseDouble(value);
          } else {
            return Long.parseLong(value);
          }
        } catch (NumberFormatException e) {
          return 0.0;
        }
      case 'L': // Logical
        return "T".equalsIgnoreCase(value) || "Y".equalsIgnoreCase(value);
      case 'D': // Date
      case 'C': // Character
      default:
        return value;
    }
  }
  
  /**
   * Interface for mapping shapefile attributes to Object arrays.
   */
  @FunctionalInterface
  public interface AttributeMapper {
    Object[] mapAttributes(SimpleDbfFeature feature);
  }
  
  /**
   * Simple wrapper around DBF record map.
   */
  public static class SimpleDbfFeature {
    private final Map<String, Object> attributes;
    
    public SimpleDbfFeature(Map<String, Object> attributes) {
      this.attributes = attributes;
    }
    
    public Object getAttribute(String name) {
      return attributes.get(name);
    }
  }
  
  /**
   * Get safe string attribute from feature.
   */
  public static String getStringAttribute(SimpleDbfFeature feature, String attributeName) {
    Object value = feature.getAttribute(attributeName);
    return (value != null) ? value.toString().trim() : "";
  }
  
  /**
   * Get safe double attribute from feature.
   */
  public static Double getDoubleAttribute(SimpleDbfFeature feature, String attributeName) {
    Object value = feature.getAttribute(attributeName);
    if (value == null) return 0.0;
    
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    
    try {
      return Double.parseDouble(value.toString());
    } catch (NumberFormatException e) {
      LOGGER.warn("Could not parse {} as double: {}", attributeName, value);
      return 0.0;
    }
  }
  
  /**
   * DBF field descriptor.
   */
  private static class DbfField {
    final String name;
    final char type;
    final int length;
    
    DbfField(String name, char type, int length) {
      this.name = name;
      this.type = type;
      this.length = length;
    }
  }
}