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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
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
 * Utility class for parsing TIGER shapefiles with improved error handling.
 * 
 * <p>Provides access to both attribute data and geometric data from shapefiles.
 * Uses JTS for robust geometry handling and proper WKT conversion.
 */
public class TigerShapefileParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerShapefileParser.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final double SIMPLIFICATION_TOLERANCE = 0.001; // ~100m at equator
  
  /**
   * Parse a TIGER shapefile and extract features with JTS geometry support.
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
      
      // Parse geometry from .shp file if it exists
      File shpFile = findShpFileByPrefix(shapefileDir, expectedPrefix);
      List<Geometry> geometries = new ArrayList<>();
      if (shpFile != null) {
        LOGGER.info("Parsing geometry from SHP file: {}", shpFile.getName());
        geometries = parseShpFileGeometries(shpFile);
      }
      
      // Convert using attribute mapper, adding geometry if available
      for (int i = 0; i < dbfRecords.size(); i++) {
        Map<String, Object> record = dbfRecords.get(i);
        
        // Add geometry to the record if available
        Geometry geometry = (i < geometries.size()) ? geometries.get(i) : null;
        record.put("_GEOMETRY_", geometry);
        
        Object[] mappedRecord = attributeMapper.mapAttributes(new ShapefileFeature(record));
        if (mappedRecord != null) {
          records.add(mappedRecord);
        }
      }
      
      LOGGER.info("Parsed {} features from {} (with {} geometries)", 
          records.size(), dbfFile.getName(), geometries.size());
      
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
   * Find SHP file by prefix in directory.
   */
  private static File findShpFileByPrefix(File dir, String prefix) {
    if (!dir.exists() || !dir.isDirectory()) {
      return null;
    }
    
    File[] files = dir.listFiles((d, name) -> 
        name.startsWith(prefix) && name.toLowerCase().endsWith(".shp"));
    
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
   * Parse geometries from SHP file using JTS and return as JTS Geometry objects.
   */
  private static List<Geometry> parseShpFileGeometries(File shpFile) {
    List<Geometry> geometries = new ArrayList<>();
    
    try (FileInputStream fis = new FileInputStream(shpFile)) {
      // Read SHP header
      byte[] headerBytes = new byte[100];
      fis.read(headerBytes);
      
      ByteBuffer header = ByteBuffer.wrap(headerBytes);
      
      // File code (should be 9994) - big endian
      header.order(ByteOrder.BIG_ENDIAN);
      int fileCode = header.getInt();
      if (fileCode != 9994) {
        LOGGER.warn("Invalid shapefile file code: {}", fileCode);
        return geometries;
      }
      
      // Switch to little endian for remaining header
      header.order(ByteOrder.LITTLE_ENDIAN);
      header.position(32);
      
      // Version and shape type
      int version = header.getInt();
      int shapeType = header.getInt();
      
      LOGGER.debug("SHP file: version={}, shapeType={}", version, shapeType);
      
      // Skip bounding box (8 doubles = 64 bytes)
      header.position(100);
      
      // Read shapes
      while (fis.available() > 0) {
        // Read record header (8 bytes)
        byte[] recordHeader = new byte[8];
        int bytesRead = fis.read(recordHeader);
        if (bytesRead < 8) break;
        
        ByteBuffer recordBuf = ByteBuffer.wrap(recordHeader);
        recordBuf.order(ByteOrder.BIG_ENDIAN);
        int recordNumber = recordBuf.getInt();
        int contentLength = recordBuf.getInt() * 2; // Convert from 16-bit words to bytes
        
        if (contentLength <= 0 || contentLength > 50 * 1024 * 1024) { // 50MB limit
          LOGGER.warn("Skipping record {} with invalid/large size: {} bytes", recordNumber, contentLength);
          if (contentLength > 0 && contentLength < Integer.MAX_VALUE) {
            fis.skip(contentLength);
          }
          geometries.add(null);
          continue;
        }
        
        // Read record content
        byte[] content = new byte[contentLength];
        bytesRead = fis.read(content);
        if (bytesRead < contentLength) {
          LOGGER.warn("Incomplete read for record {}", recordNumber);
          geometries.add(null);
          continue;
        }
        
        // Parse shape using JTS
        Geometry geometry = parseShapeRecordWithJTS(content, shapeType);
        geometries.add(geometry);
      }
      
      LOGGER.info("Extracted {} geometries from {}", geometries.size(), shpFile.getName());
      
    } catch (Exception e) {
      LOGGER.warn("Error parsing SHP file {}: {}", shpFile.getName(), e.getMessage());
    }
    
    return geometries;
  }
  
  /**
   * Parse a single shape record using JTS and return JTS Geometry.
   */
  private static Geometry parseShapeRecordWithJTS(byte[] content, int shapeType) {
    try {
      if (content.length < 4) {
        return null;
      }
      
      ByteBuffer buf = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
      int recordShapeType = buf.getInt();
      
      switch (recordShapeType) {
        case 0: // Null shape
          return null;
          
        case 1: // Point
          if (buf.remaining() < 16) return null;
          double x = buf.getDouble();
          double y = buf.getDouble();
          return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
          
        case 5: // Polygon
          return parsePolygonWithJTS(buf);
          
        case 3: // Polyline
          return parsePolylineWithJTS(buf);
          
        case 8: // MultiPoint
          return parseMultiPointWithJTS(buf);
          
        case 11: // PointZ
          if (buf.remaining() < 24) return null;
          double xz = buf.getDouble();
          double yz = buf.getDouble();
          // Skip Z for now
          return GEOMETRY_FACTORY.createPoint(new Coordinate(xz, yz));
          
        case 13: // PolylineZ
          return parsePolylineWithJTS(buf);
          
        case 15: // PolygonZ
          return parsePolygonWithJTS(buf);
          
        default:
          LOGGER.debug("Unsupported shape type: {}", recordShapeType);
          return null;
      }
      
    } catch (Exception e) {
      LOGGER.debug("Error parsing shape record: {}", e.getMessage());
      return null;
    }
  }
  
  /**
   * Parse polygon using JTS.
   */
  private static Geometry parsePolygonWithJTS(ByteBuffer buf) {
    try {
      if (buf.remaining() < 40) return null;
      
      // Skip bounding box
      buf.position(buf.position() + 32);
      
      int numParts = buf.getInt();
      int numPoints = buf.getInt();
      
      if (numParts <= 0 || numPoints <= 0) return null;
      if (buf.remaining() < numParts * 4 + numPoints * 16) return null;
      
      // Read part indices
      int[] partIndices = new int[numParts];
      for (int i = 0; i < numParts; i++) {
        partIndices[i] = buf.getInt();
      }
      
      // Read all coordinates
      List<Coordinate> allCoords = new ArrayList<>();
      for (int i = 0; i < numPoints && buf.remaining() >= 16; i++) {
        double x = buf.getDouble();
        double y = buf.getDouble();
        allCoords.add(new Coordinate(x, y));
      }
      
      if (allCoords.size() < 4) return null; // Need at least 4 points for polygon
      
      // Create outer ring from first part
      int firstPartStart = partIndices[0];
      int firstPartEnd = (numParts > 1) ? partIndices[1] : numPoints;
      
      if (firstPartEnd <= firstPartStart) return null;
      
      List<Coordinate> ringCoords = new ArrayList<>();
      for (int i = firstPartStart; i < firstPartEnd && i < allCoords.size(); i++) {
        ringCoords.add(allCoords.get(i));
      }
      
      // Simplify if too many points
      if (ringCoords.size() > 1000) {
        int step = ringCoords.size() / 500; // Sample down to ~500 points
        List<Coordinate> simplified = new ArrayList<>();
        for (int i = 0; i < ringCoords.size(); i += step) {
          simplified.add(ringCoords.get(i));
        }
        ringCoords = simplified;
      }
      
      // Ensure ring is closed
      if (ringCoords.size() >= 4 && !ringCoords.get(0).equals(ringCoords.get(ringCoords.size() - 1))) {
        ringCoords.add(new Coordinate(ringCoords.get(0)));
      }
      
      if (ringCoords.size() < 4) return null;
      
      LinearRing shell = GEOMETRY_FACTORY.createLinearRing(ringCoords.toArray(new Coordinate[0]));
      Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell);
      
      // Apply topology-preserving simplification for very complex polygons
      if (polygon.getNumPoints() > 1000) {
        polygon = (Polygon) TopologyPreservingSimplifier.simplify(polygon, SIMPLIFICATION_TOLERANCE);
      }
      
      return polygon;
      
    } catch (Exception e) {
      LOGGER.debug("Error parsing polygon with JTS: {}", e.getMessage());
      return null;
    }
  }
  
  /**
   * Parse polyline using JTS.
   */
  private static Geometry parsePolylineWithJTS(ByteBuffer buf) {
    try {
      if (buf.remaining() < 40) return null;
      
      buf.position(buf.position() + 32); // Skip bbox
      
      int numParts = buf.getInt();
      int numPoints = buf.getInt();
      
      if (numParts <= 0 || numPoints <= 0) return null;
      if (buf.remaining() < numParts * 4 + numPoints * 16) return null;
      
      // Skip part indices for now, just read first linestring
      buf.position(buf.position() + numParts * 4);
      
      List<Coordinate> coords = new ArrayList<>();
      int maxPoints = Math.min(500, numPoints); // Limit for performance
      int step = Math.max(1, numPoints / maxPoints);
      
      for (int i = 0; i < numPoints && buf.remaining() >= 16; i++) {
        double x = buf.getDouble();
        double y = buf.getDouble();
        
        if (i % step == 0 || i == 0 || i == numPoints - 1) {
          coords.add(new Coordinate(x, y));
        }
      }
      
      if (coords.size() < 2) return null;
      
      return GEOMETRY_FACTORY.createLineString(coords.toArray(new Coordinate[0]));
      
    } catch (Exception e) {
      LOGGER.debug("Error parsing polyline with JTS: {}", e.getMessage());
      return null;
    }
  }
  
  /**
   * Parse multipoint using JTS.
   */
  private static Geometry parseMultiPointWithJTS(ByteBuffer buf) {
    try {
      if (buf.remaining() < 36) return null;
      
      buf.position(buf.position() + 32); // Skip bbox
      
      int numPoints = buf.getInt();
      if (numPoints <= 0) return null;
      
      List<Point> points = new ArrayList<>();
      int maxPoints = Math.min(100, numPoints); // Limit for performance
      int step = Math.max(1, numPoints / maxPoints);
      
      for (int i = 0; i < numPoints && buf.remaining() >= 16; i++) {
        double x = buf.getDouble();
        double y = buf.getDouble();
        
        if (i % step == 0) {
          points.add(GEOMETRY_FACTORY.createPoint(new Coordinate(x, y)));
        }
      }
      
      if (points.isEmpty()) return null;
      
      return GEOMETRY_FACTORY.createMultiPoint(points.toArray(new Point[0]));
      
    } catch (Exception e) {
      LOGGER.debug("Error parsing multipoint with JTS: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Interface for mapping shapefile attributes to Object arrays.
   */
  @FunctionalInterface
  public interface AttributeMapper {
    Object[] mapAttributes(ShapefileFeature feature);
  }
  
  /**
   * Wrapper around shapefile feature data.
   */
  public static class ShapefileFeature {
    private final Map<String, Object> attributes;
    
    public ShapefileFeature(Map<String, Object> attributes) {
      this.attributes = attributes;
    }
    
    public Object getAttribute(String name) {
      return attributes.get(name);
    }
  }
  
  /**
   * Get safe string attribute from feature.
   */
  public static String getStringAttribute(ShapefileFeature feature, String attributeName) {
    Object value = feature.getAttribute(attributeName);
    return (value != null) ? value.toString().trim() : "";
  }
  
  /**
   * Get safe double attribute from feature.
   */
  public static Double getDoubleAttribute(ShapefileFeature feature, String attributeName) {
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
   * Get geometry as WKT string from feature with JTS support.
   */
  public static String getGeometryAttribute(ShapefileFeature feature) {
    Object geomObj = feature.getAttribute("_GEOMETRY_");
    if (geomObj == null || !(geomObj instanceof Geometry)) {
      return null;
    }
    
    try {
      Geometry geometry = (Geometry) geomObj;
      return geometry.toText();
    } catch (Exception e) {
      LOGGER.warn("Error extracting geometry: {}", e.getMessage());
      return null;
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