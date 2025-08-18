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
package org.apache.calcite.adapter.file.xml;

import org.apache.calcite.adapter.file.converters.XmlToJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for XML to JSON converter.
 */
@Tag("unit")
class XmlToJsonConverterTest {
  
  @TempDir
  Path tempDir;
  
  private ObjectMapper mapper;
  private File outputDir;
  
  @BeforeEach
  void setUp() {
    mapper = new ObjectMapper();
    outputDir = tempDir.resolve("output").toFile();
    outputDir.mkdirs();
  }
  
  @Test
  void testSimpleProductCatalog() throws IOException {
    // Create test XML
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog>\n"
        + "  <product id=\"1\">\n"
        + "    <name>Widget</name>\n"
        + "    <price>9.99</price>\n"
        + "    <category>Electronics</category>\n"
        + "  </product>\n"
        + "  <product id=\"2\">\n"
        + "    <name>Gadget</name>\n"
        + "    <price>19.99</price>\n"
        + "    <category>Electronics</category>\n"
        + "  </product>\n"
        + "  <product id=\"3\">\n"
        + "    <name>Tool</name>\n"
        + "    <price>29.99</price>\n"
        + "    <category>Hardware</category>\n"
        + "  </product>\n"
        + "</catalog>";
    
    File xmlFile = createTempXmlFile("catalog.xml", xmlContent);
    
    // Convert
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir);
    
    // Verify
    assertEquals(1, jsonFiles.size());
    
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    assertEquals("catalog__product.json", jsonFile.getName());
    
    // Parse and verify JSON content
    JsonNode jsonArray = mapper.readTree(jsonFile);
    assertTrue(jsonArray.isArray());
    assertEquals(3, jsonArray.size());
    
    // Verify first product
    JsonNode firstProduct = jsonArray.get(0);
    assertEquals("1", firstProduct.get("id").asText());
    assertEquals("Widget", firstProduct.get("name").asText());
    assertEquals("9.99", firstProduct.get("price").asText());
    assertEquals("Electronics", firstProduct.get("category").asText());
    
    // Verify second product
    JsonNode secondProduct = jsonArray.get(1);
    assertEquals("2", secondProduct.get("id").asText());
    assertEquals("Gadget", secondProduct.get("name").asText());
  }
  
  @Test
  void testNestedElements() throws IOException {
    // Create test XML with nested structure
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<customers>\n"
        + "  <customer id=\"1\">\n"
        + "    <name>John Doe</name>\n"
        + "    <address>\n"
        + "      <street>123 Main St</street>\n"
        + "      <city>New York</city>\n"
        + "      <state>NY</state>\n"
        + "      <zip>10001</zip>\n"
        + "    </address>\n"
        + "    <phone>555-1234</phone>\n"
        + "  </customer>\n"
        + "  <customer id=\"2\">\n"
        + "    <name>Jane Smith</name>\n"
        + "    <address>\n"
        + "      <street>456 Oak Ave</street>\n"
        + "      <city>Los Angeles</city>\n"
        + "      <state>CA</state>\n"
        + "      <zip>90210</zip>\n"
        + "    </address>\n"
        + "    <phone>555-5678</phone>\n"
        + "  </customer>\n"
        + "</customers>";
    
    File xmlFile = createTempXmlFile("customers.xml", xmlContent);
    
    // Convert
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir);
    
    // Verify
    assertEquals(1, jsonFiles.size());
    
    File jsonFile = jsonFiles.get(0);
    assertEquals("customers__customer.json", jsonFile.getName());
    
    // Parse and verify JSON content
    JsonNode jsonArray = mapper.readTree(jsonFile);
    assertEquals(2, jsonArray.size());
    
    // Verify flattened structure
    JsonNode firstCustomer = jsonArray.get(0);
    assertEquals("1", firstCustomer.get("id").asText());
    assertEquals("John Doe", firstCustomer.get("name").asText());
    assertEquals("123 Main St", firstCustomer.get("address__street").asText());
    assertEquals("New York", firstCustomer.get("address__city").asText());
    assertEquals("NY", firstCustomer.get("address__state").asText());
    assertEquals("10001", firstCustomer.get("address__zip").asText());
    assertEquals("555-1234", firstCustomer.get("phone").asText());
  }
  
  @Test
  void testMultipleTablePatterns() throws IOException {
    // Create test XML with multiple repeating patterns
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<company>\n"
        + "  <departments>\n"
        + "    <department id=\"1\">\n"
        + "      <name>Engineering</name>\n"
        + "      <budget>100000</budget>\n"
        + "    </department>\n"
        + "    <department id=\"2\">\n"
        + "      <name>Sales</name>\n"
        + "      <budget>75000</budget>\n"
        + "    </department>\n"
        + "  </departments>\n"
        + "  <employees>\n"
        + "    <employee id=\"101\">\n"
        + "      <name>Alice Johnson</name>\n"
        + "      <department>Engineering</department>\n"
        + "      <salary>80000</salary>\n"
        + "    </employee>\n"
        + "    <employee id=\"102\">\n"
        + "      <name>Bob Wilson</name>\n"
        + "      <department>Sales</department>\n"
        + "      <salary>65000</salary>\n"
        + "    </employee>\n"
        + "    <employee id=\"103\">\n"
        + "      <name>Carol Davis</name>\n"
        + "      <department>Engineering</department>\n"
        + "      <salary>90000</salary>\n"
        + "    </employee>\n"
        + "  </employees>\n"
        + "</company>";
    
    File xmlFile = createTempXmlFile("company.xml", xmlContent);
    
    // Convert
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir);
    
    // Verify - should create 2 tables
    assertEquals(2, jsonFiles.size());
    
    // Check file names
    boolean foundDepartments = false;
    boolean foundEmployees = false;
    
    for (File jsonFile : jsonFiles) {
      if (jsonFile.getName().equals("company__department.json")) {
        foundDepartments = true;
        
        // Verify departments content
        JsonNode jsonArray = mapper.readTree(jsonFile);
        assertEquals(2, jsonArray.size());
        assertEquals("Engineering", jsonArray.get(0).get("name").asText());
        assertEquals("100000", jsonArray.get(0).get("budget").asText());
        
      } else if (jsonFile.getName().equals("company__employee.json")) {
        foundEmployees = true;
        
        // Verify employees content
        JsonNode jsonArray = mapper.readTree(jsonFile);
        assertEquals(3, jsonArray.size());
        assertEquals("Alice Johnson", jsonArray.get(0).get("name").asText());
        assertEquals("Engineering", jsonArray.get(0).get("department").asText());
      }
    }
    
    assertTrue(foundDepartments, "Should create departments table");
    assertTrue(foundEmployees, "Should create employees table");
  }
  
  @Test
  void testXPathTargeting() throws IOException {
    // Create test XML
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<data>\n"
        + "  <metadata>\n"
        + "    <version>1.0</version>\n"
        + "    <created>2023-01-01</created>\n"
        + "  </metadata>\n"
        + "  <products>\n"
        + "    <product id=\"1\">\n"
        + "      <name>Item A</name>\n"
        + "      <price>10.00</price>\n"
        + "    </product>\n"
        + "    <product id=\"2\">\n"
        + "      <name>Item B</name>\n"
        + "      <price>20.00</price>\n"
        + "    </product>\n"
        + "  </products>\n"
        + "  <categories>\n"
        + "    <category id=\"cat1\">\n"
        + "      <name>Category 1</name>\n"
        + "    </category>\n"
        + "  </categories>\n"
        + "</data>";
    
    File xmlFile = createTempXmlFile("data.xml", xmlContent);
    
    // Convert with XPath targeting only products
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir, "//product");
    
    // Verify - should only create products table
    assertEquals(1, jsonFiles.size());
    
    File jsonFile = jsonFiles.get(0);
    assertEquals("data__product.json", jsonFile.getName());
    
    // Verify content
    JsonNode jsonArray = mapper.readTree(jsonFile);
    assertEquals(2, jsonArray.size());
    assertEquals("Item A", jsonArray.get(0).get("name").asText());
  }
  
  @Test
  void testColumnNameCasing() throws IOException {
    // Create test XML with mixed case elements
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<root>\n"
        + "  <testItem id=\"1\">\n"
        + "    <itemName>Test Item</itemName>\n"
        + "    <itemPrice>99.99</itemPrice>\n"
        + "    <isAvailable>true</isAvailable>\n"
        + "  </testItem>\n"
        + "  <testItem id=\"2\">\n"
        + "    <itemName>Another Item</itemName>\n"
        + "    <itemPrice>199.99</itemPrice>\n"
        + "    <isAvailable>false</isAvailable>\n"
        + "  </testItem>\n"
        + "</root>";
    
    File xmlFile = createTempXmlFile("test.xml", xmlContent);
    
    // Convert with LOWER casing
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir, null, "LOWER");
    
    assertEquals(1, jsonFiles.size());
    File jsonFile = jsonFiles.get(0);
    
    // Verify column names are lowercase
    JsonNode jsonArray = mapper.readTree(jsonFile);
    JsonNode firstItem = jsonArray.get(0);
    
    assertTrue(firstItem.has("id"));
    assertTrue(firstItem.has("itemname"));
    assertTrue(firstItem.has("itemprice"));
    assertTrue(firstItem.has("isavailable"));
    
    // Verify values are preserved
    assertEquals("1", firstItem.get("id").asText());
    assertEquals("Test Item", firstItem.get("itemname").asText());
  }
  
  @Test
  void testArrayElements() throws IOException {
    // Create test XML with array-like structures
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<library>\n"
        + "  <book id=\"1\">\n"
        + "    <title>Book One</title>\n"
        + "    <authors>\n"
        + "      <author>Author A</author>\n"
        + "      <author>Author B</author>\n"
        + "      <author>Author C</author>\n"
        + "    </authors>\n"
        + "    <isbn>123456789</isbn>\n"
        + "  </book>\n"
        + "  <book id=\"2\">\n"
        + "    <title>Book Two</title>\n"
        + "    <authors>\n"
        + "      <author>Author X</author>\n"
        + "      <author>Author Y</author>\n"
        + "    </authors>\n"
        + "    <isbn>987654321</isbn>\n"
        + "  </book>\n"
        + "</library>";
    
    File xmlFile = createTempXmlFile("library.xml", xmlContent);
    
    // Convert
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir);
    
    assertEquals(1, jsonFiles.size());
    File jsonFile = jsonFiles.get(0);
    
    // Verify array handling
    JsonNode jsonArray = mapper.readTree(jsonFile);
    JsonNode firstBook = jsonArray.get(0);
    
    assertEquals("Book One", firstBook.get("title").asText());
    assertEquals("123456789", firstBook.get("isbn").asText());
    
    // Verify authors array (flattened as authors__author)
    assertTrue(firstBook.has("authors__author"));
    JsonNode authors = firstBook.get("authors__author");
    assertTrue(authors.isArray());
    assertEquals(3, authors.size());
    assertEquals("Author A", authors.get(0).get("_content").asText());
    assertEquals("Author B", authors.get(1).get("_content").asText());
  }
  
  @Test
  void testEmptyAndMissingElements() throws IOException {
    // Create test XML with empty and missing elements
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<records>\n"
        + "  <record id=\"1\">\n"
        + "    <name>Complete Record</name>\n"
        + "    <description>Has all fields</description>\n"
        + "    <status>active</status>\n"
        + "  </record>\n"
        + "  <record id=\"2\">\n"
        + "    <name>Partial Record</name>\n"
        + "    <description></description>\n"
        + "  </record>\n"
        + "  <record id=\"3\">\n"
        + "    <name>Minimal Record</name>\n"
        + "  </record>\n"
        + "</records>";
    
    File xmlFile = createTempXmlFile("records.xml", xmlContent);
    
    // Convert
    List<File> jsonFiles = XmlToJsonConverter.convert(xmlFile, outputDir);
    
    assertEquals(1, jsonFiles.size());
    File jsonFile = jsonFiles.get(0);
    
    // Verify handling of empty/missing elements
    JsonNode jsonArray = mapper.readTree(jsonFile);
    assertEquals(3, jsonArray.size());
    
    // First record - complete
    JsonNode record1 = jsonArray.get(0);
    assertEquals("Complete Record", record1.get("name").asText());
    assertEquals("Has all fields", record1.get("description").asText());
    assertEquals("active", record1.get("status").asText());
    
    // Second record - empty description
    JsonNode record2 = jsonArray.get(1);
    assertEquals("Partial Record", record2.get("name").asText());
    assertFalse(record2.has("description") || (record2.get("description") != null && !record2.get("description").asText().isEmpty()));
    
    // Third record - minimal
    JsonNode record3 = jsonArray.get(2);
    assertEquals("Minimal Record", record3.get("name").asText());
    assertFalse(record3.has("description"));
    assertFalse(record3.has("status"));
  }
  
  @Test
  void testHasExtractedFiles() throws IOException {
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<root>"
        + "<item>test1</item>"
        + "<item>test2</item>"
        + "</root>";
    
    File xmlFile = createTempXmlFile("test.xml", xmlContent);
    
    // Initially no extracted files
    assertFalse(XmlToJsonConverter.hasExtractedFiles(xmlFile, outputDir));
    
    // Convert
    XmlToJsonConverter.convert(xmlFile, outputDir);
    
    // Now should have extracted files
    assertTrue(XmlToJsonConverter.hasExtractedFiles(xmlFile, outputDir));
  }
  
  @Test
  void testInvalidXml() throws IOException {
    // Create invalid XML
    String invalidXml = "<?xml version=\"1.0\"?>\n"
        + "<root>\n"
        + "  <unclosed>This tag is not closed\n"
        + "  <item>Valid item</item>\n"
        + "</root>";
    
    File xmlFile = createTempXmlFile("invalid.xml", invalidXml);
    
    // Should throw IOException
    try {
      XmlToJsonConverter.convert(xmlFile, outputDir);
      // If we get here, the test failed
      assertTrue(false, "Expected IOException for invalid XML");
    } catch (IOException e) {
      // Expected
      assertNotNull(e.getMessage());
    }
  }
  
  /**
   * Helper method to create temporary XML file.
   */
  private File createTempXmlFile(String fileName, String content) throws IOException {
    File file = tempDir.resolve(fileName).toFile();
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }
}