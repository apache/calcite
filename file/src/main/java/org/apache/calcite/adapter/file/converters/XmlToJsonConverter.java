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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Converts XML files to JSON files for processing by the file adapter.
 * 
 * <p>This converter analyzes XML structure to find repeating element patterns
 * that can be treated as tables, then converts each pattern to a separate JSON file.
 * 
 * <p>Features:
 * <ul>
 *   <li>Automatic detection of repeating XML element patterns</li>
 *   <li>XPath support for targeted element extraction</li>
 *   <li>Attribute handling (prefixed with @)</li>
 *   <li>Nested element flattening with configurable separators</li>
 *   <li>Multiple table generation from single XML file</li>
 * </ul>
 * 
 * <p>XML Attributes are converted to JSON properties with @ prefix:
 * {@code <product id="123">} becomes {@code {"@id": "123"}}
 * 
 * <p>Nested elements are flattened with __ separator by default:
 * {@code <address><city>NYC</city></address>} becomes {@code {"address__city": "NYC"}}
 */
public class XmlToJsonConverter {
  private static final Logger LOGGER = Logger.getLogger(XmlToJsonConverter.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  // Configuration constants
  private static final String DEFAULT_FLATTEN_SEPARATOR = "__";
  private static final String ATTRIBUTE_PREFIX = "@";
  private static final int MAX_NESTING_DEPTH = 5;
  
  private XmlToJsonConverter() {
    // Utility class should not be instantiated
  }
  
  /**
   * Converts XML file to JSON files using automatic pattern detection.
   * 
   * @param xmlFile The XML file to convert
   * @param outputDir The directory to write JSON files to
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File xmlFile, File outputDir, File baseDirectory) throws IOException {
    return convert(xmlFile, outputDir, null, baseDirectory);
  }
  
  /**
   * Converts XML file to JSON files with optional XPath targeting.
   * 
   * @param xmlFile The XML file to convert
   * @param outputDir The directory to write JSON files to
   * @param xpath Optional XPath expression to target specific elements
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File xmlFile, File outputDir, String xpath, File baseDirectory) throws IOException {
    return convert(xmlFile, outputDir, xpath, "UNCHANGED", baseDirectory);
  }
  
  /**
   * Converts XML file to JSON files with column name casing control.
   * 
   * @param xmlFile The XML file to convert
   * @param outputDir The directory to write JSON files to
   * @param xpath Optional XPath expression to target specific elements
   * @param columnNameCasing Casing strategy for column names ("UPPER", "LOWER", "UNCHANGED")
   * @return List of generated JSON files
   * @throws IOException if conversion fails
   */
  public static List<File> convert(File xmlFile, File outputDir, String xpath, String columnNameCasing, File baseDirectory) 
      throws IOException {
    return convert(xmlFile, outputDir, xpath, columnNameCasing, baseDirectory, null);
  }
  
  public static List<File> convert(File xmlFile, File outputDir, String xpath, String columnNameCasing, File baseDirectory, String relativePath) 
      throws IOException {
    List<File> jsonFiles = new ArrayList<>();
    
    // Use XmlTableScanner to find table patterns
    Source source = Sources.of(xmlFile);
    List<XmlTableScanner.TableInfo> tableInfos = XmlTableScanner.scanTables(source, xpath);
    
    LOGGER.info("Found " + tableInfos.size() + " table patterns in " + xmlFile.getName());
    
    // Ensure output directory exists
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    
    // Get base filename for JSON file naming
    String baseFileName = ConverterUtils.getBaseFileName(xmlFile.getName(), ".xml", ".XML");
    
    // Include directory structure in the filename if relativePath is provided
    if (relativePath != null && relativePath.contains(File.separator)) {
      String dirPrefix = relativePath.substring(0, relativePath.lastIndexOf(File.separator))
          .replace(File.separator, "_");
      baseFileName = dirPrefix + "_" + baseFileName;
    }
    
    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(xmlFile);
      LOGGER.fine("Acquired read lock on XML file: " + xmlFile.getPath());
    } catch (IOException e) {
      LOGGER.warning("Could not acquire lock on file: " + xmlFile.getPath() + " - proceeding without lock");
    }
    
    try {
      // Convert each table pattern to a JSON file
      for (int i = 0; i < tableInfos.size(); i++) {
        XmlTableScanner.TableInfo tableInfo = tableInfos.get(i);
        String tableName = tableInfo.name;
        
        // Create filename: basename__tablename.json
        File jsonFile = new File(outputDir, baseFileName + "__" + tableName + ".json");
        
        try {
          convertElementsToJson(tableInfo.elements, jsonFile, columnNameCasing);
          jsonFiles.add(jsonFile);
          
          // Record the conversion for refresh tracking
          ConversionRecorder.recordConversion(xmlFile, jsonFile, "XML_TO_JSON", baseDirectory);
          
          LOGGER.fine("Wrote table '" + tableName + "' to " + jsonFile.getAbsolutePath());
          
        } catch (IOException e) {
          LOGGER.log(Level.WARNING, "Failed to write table " + tableName + " to JSON", e);
          // Continue with other tables
        }
      }
      
    } finally {
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.fine("Released read lock on XML file");
      }
    }
    
    return jsonFiles;
  }
  
  /**
   * Converts a list of XML elements to a JSON array file.
   */
  private static void convertElementsToJson(List<Element> elements, File jsonFile, String columnNameCasing) 
      throws IOException {
    ArrayNode jsonArray = MAPPER.createArrayNode();
    
    for (Element element : elements) {
      ObjectNode jsonObject = convertElementToJson(element, columnNameCasing, 0);
      if (jsonObject.size() > 0) {
        jsonArray.add(jsonObject);
      }
    }
    
    // Write to file
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(writer, jsonArray);
    }
  }
  
  /**
   * Converts a single XML element to a JSON object.
   */
  private static ObjectNode convertElementToJson(Element element, String columnNameCasing, int depth) {
    ObjectNode jsonObject = MAPPER.createObjectNode();
    
    if (depth > MAX_NESTING_DEPTH) {
      // Avoid infinite recursion - just return text content
      String textContent = element.getTextContent();
      if (textContent != null && !textContent.trim().isEmpty()) {
        jsonObject.put("_content", textContent.trim());
      }
      return jsonObject;
    }
    
    // Process attributes first
    NamedNodeMap attributes = element.getAttributes();
    for (int i = 0; i < attributes.getLength(); i++) {
      Attr attr = (Attr) attributes.item(i);
      String attrName = ATTRIBUTE_PREFIX + attr.getName();
      String attrValue = attr.getValue();
      
      String columnName = ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(attrName, columnNameCasing));
      ConverterUtils.setJsonValueWithTypeInference(jsonObject, columnName, attrValue);
    }
    
    // Process child elements
    NodeList children = element.getChildNodes();
    boolean hasChildElements = false;
    StringBuilder textContent = new StringBuilder();
    
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      
      if (child instanceof Element) {
        hasChildElements = true;
        Element childElement = (Element) child;
        String childName = childElement.getLocalName() != null ? 
            childElement.getLocalName() : childElement.getTagName();
        
        // Check if this child appears multiple times (array case)
        List<Element> similarChildren = getSimilarChildElements(element, childName);
        
        if (similarChildren.size() > 1) {
          // Multiple similar children - create array
          ArrayNode childArray = MAPPER.createArrayNode();
          for (Element similarChild : similarChildren) {
            ObjectNode childJson = convertElementToJson(similarChild, columnNameCasing, depth + 1);
            if (childJson.size() > 0) {
              childArray.add(childJson);
            }
          }
          String columnName = ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(childName, columnNameCasing));
          jsonObject.set(columnName, childArray);
          
        } else {
          // Single child element
          ObjectNode childJson = convertElementToJson(childElement, columnNameCasing, depth + 1);
          
          if (childJson.size() == 1 && childJson.has("_content")) {
            // Child has only text content - flatten it
            String value = childJson.get("_content").asText();
            String columnName = ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(childName, columnNameCasing));
            ConverterUtils.setJsonValueWithTypeInference(jsonObject, columnName, value);
          } else if (childJson.size() > 0) {
            // Child has complex content - use flattened naming
            flattenChildObject(jsonObject, childJson, childName, columnNameCasing);
          }
        }
        
      } else if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
        String text = child.getTextContent();
        if (text != null) {
          textContent.append(text);
        }
      }
    }
    
    // If element has text content and no child elements, add it
    if (!hasChildElements && textContent.length() > 0) {
      String text = textContent.toString().trim();
      if (!text.isEmpty()) {
        jsonObject.put("_content", text);
      }
    }
    
    return jsonObject;
  }
  
  /**
   * Get all child elements with the same name.
   */
  private static List<Element> getSimilarChildElements(Element parent, String childName) {
    List<Element> similar = new ArrayList<>();
    NodeList children = parent.getChildNodes();
    
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child instanceof Element) {
        Element childElement = (Element) child;
        String name = childElement.getLocalName() != null ? 
            childElement.getLocalName() : childElement.getTagName();
        if (childName.equals(name)) {
          similar.add(childElement);
        }
      }
    }
    
    return similar;
  }
  
  /**
   * Flatten a child object into the parent with prefixed property names.
   */
  private static void flattenChildObject(ObjectNode parent, ObjectNode child, String prefix, String columnNameCasing) {
    child.fields().forEachRemaining(entry -> {
      String childKey = entry.getKey();
      String flattenedKey = prefix + DEFAULT_FLATTEN_SEPARATOR + childKey;
      String columnName = ConverterUtils.sanitizeIdentifier(org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(flattenedKey, columnNameCasing));
      parent.set(columnName, entry.getValue());
    });
  }
  
  /**
   * Checks if extracted JSON files already exist for an XML file.
   */
  public static boolean hasExtractedFiles(File xmlFile, File outputDir) {
    String baseFileName = ConverterUtils.getBaseFileName(xmlFile.getName(), ".xml", ".XML");
    
    // Check if any files matching the pattern exist
    final String finalBaseFileName = baseFileName;
    File[] files = outputDir.listFiles((dir, name) ->
        name.startsWith(finalBaseFileName + "__") && name.endsWith(".json"));
    
    return files != null && files.length > 0;
  }
}