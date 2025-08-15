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

import org.apache.calcite.adapter.file.converters.XmlTableScanner;
import org.apache.calcite.util.Sources;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for XML table scanner.
 */
@Tag("unit")
class XmlTableScannerTest {
  
  @TempDir
  Path tempDir;
  
  @BeforeEach
  void setUp() {
    // Setup if needed
  }
  
  @Test
  void testScanSimpleRepeatingElements() throws IOException {
    // Create test XML with repeating elements
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog>\n"
        + "  <product id=\"1\">\n"
        + "    <name>Widget</name>\n"
        + "    <price>9.99</price>\n"
        + "  </product>\n"
        + "  <product id=\"2\">\n"
        + "    <name>Gadget</name>\n"
        + "    <price>19.99</price>\n"
        + "  </product>\n"
        + "  <product id=\"3\">\n"
        + "    <name>Tool</name>\n"
        + "    <price>29.99</price>\n"
        + "  </product>\n"
        + "</catalog>";
    
    File xmlFile = createTempXmlFile("catalog.xml", xmlContent);
    
    // Scan for tables
    List<XmlTableScanner.TableInfo> tables = XmlTableScanner.scanTables(Sources.of(xmlFile));
    
    // Verify results
    assertEquals(1, tables.size());
    
    XmlTableScanner.TableInfo productTable = tables.get(0);
    assertEquals("product", productTable.name);
    assertEquals(3, productTable.count);
    assertEquals("//product", productTable.xpath);
    assertNotNull(productTable.elements);
    assertEquals(3, productTable.elements.size());
  }
  
  @Test
  void testScanMultipleRepeatingPatterns() throws IOException {
    // Create test XML with multiple repeating patterns
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<company>\n"
        + "  <metadata>\n"
        + "    <version>1.0</version>\n"
        + "    <created>2023-01-01</created>\n"
        + "  </metadata>\n"
        + "  <departments>\n"
        + "    <department id=\"1\">\n"
        + "      <name>Engineering</name>\n"
        + "    </department>\n"
        + "    <department id=\"2\">\n"
        + "      <name>Sales</name>\n"
        + "    </department>\n"
        + "    <department id=\"3\">\n"
        + "      <name>Marketing</name>\n"
        + "    </department>\n"
        + "  </departments>\n"
        + "  <employees>\n"
        + "    <employee id=\"101\">\n"
        + "      <name>Alice</name>\n"
        + "      <dept>Engineering</dept>\n"
        + "    </employee>\n"
        + "    <employee id=\"102\">\n"
        + "      <name>Bob</name>\n"
        + "      <dept>Sales</dept>\n"
        + "    </employee>\n"
        + "  </employees>\n"
        + "</company>";
    
    File xmlFile = createTempXmlFile("company.xml", xmlContent);
    
    // Scan for tables
    List<XmlTableScanner.TableInfo> tables = XmlTableScanner.scanTables(Sources.of(xmlFile));
    
    // Should find both department and employee patterns
    assertEquals(2, tables.size());
    
    // Find department table
    XmlTableScanner.TableInfo deptTable = tables.stream()
        .filter(t -> "department".equals(t.name))
        .findFirst()
        .orElse(null);
    
    assertNotNull(deptTable);
    assertEquals(3, deptTable.count);
    assertEquals("//department", deptTable.xpath);
    
    // Find employee table
    XmlTableScanner.TableInfo empTable = tables.stream()
        .filter(t -> "employee".equals(t.name))
        .findFirst()
        .orElse(null);
    
    assertNotNull(empTable);
    assertEquals(2, empTable.count);
    assertEquals("//employee", empTable.xpath);
  }
  
  @Test
  void testScanWithXPath() throws IOException {
    // Create test XML
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<data>\n"
        + "  <section1>\n"
        + "    <item id=\"1\">\n"
        + "      <value>A</value>\n"
        + "    </item>\n"
        + "    <item id=\"2\">\n"
        + "      <value>B</value>\n"
        + "    </item>\n"
        + "  </section1>\n"
        + "  <section2>\n"
        + "    <item id=\"3\">\n"
        + "      <value>C</value>\n"
        + "    </item>\n"
        + "    <item id=\"4\">\n"
        + "      <value>D</value>\n"
        + "    </item>\n"
        + "  </section2>\n"
        + "  <other>\n"
        + "    <record>Single record</record>\n"
        + "  </other>\n"
        + "</data>";
    
    File xmlFile = createTempXmlFile("data.xml", xmlContent);
    
    // Scan with XPath targeting only section1 items
    List<XmlTableScanner.TableInfo> tables = 
        XmlTableScanner.scanTables(Sources.of(xmlFile), "//section1/item");
    
    // Should find only section1 items
    assertEquals(1, tables.size());
    
    XmlTableScanner.TableInfo table = tables.get(0);
    assertEquals("item", table.name);
    assertEquals(2, table.count);
    assertEquals("//section1/item", table.xpath);
    
    // Verify elements are from section1
    assertEquals("1", table.elements.get(0).getAttribute("id"));
    assertEquals("2", table.elements.get(1).getAttribute("id"));
  }
  
  @Test
  void testScanWithComplexXPath() throws IOException {
    // Create test XML with namespaces and complex structure
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<root>\n"
        + "  <books>\n"
        + "    <book category=\"fiction\">\n"
        + "      <title>Book 1</title>\n"
        + "      <author>Author 1</author>\n"
        + "      <price>10.99</price>\n"
        + "    </book>\n"
        + "    <book category=\"non-fiction\">\n"
        + "      <title>Book 2</title>\n"
        + "      <author>Author 2</author>\n"
        + "      <price>15.99</price>\n"
        + "    </book>\n"
        + "    <book category=\"fiction\">\n"
        + "      <title>Book 3</title>\n"
        + "      <author>Author 3</author>\n"
        + "      <price>12.99</price>\n"
        + "    </book>\n"
        + "  </books>\n"
        + "</root>";
    
    File xmlFile = createTempXmlFile("books.xml", xmlContent);
    
    // Scan with XPath for all books
    List<XmlTableScanner.TableInfo> tables = 
        XmlTableScanner.scanTables(Sources.of(xmlFile), "//book");
    
    assertEquals(1, tables.size());
    
    XmlTableScanner.TableInfo table = tables.get(0);
    assertEquals("book", table.name);
    assertEquals(3, table.count);
    assertEquals("//book", table.xpath);
    
    // Verify all books found
    assertEquals("fiction", table.elements.get(0).getAttribute("category"));
    assertEquals("non-fiction", table.elements.get(1).getAttribute("category"));
    assertEquals("fiction", table.elements.get(2).getAttribute("category"));
  }
  
  @Test
  void testScanIgnoresSingleElements() throws IOException {
    // Create test XML with mostly single elements
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<config>\n"
        + "  <database>\n"
        + "    <host>localhost</host>\n"
        + "    <port>5432</port>\n"
        + "    <username>user</username>\n"
        + "    <password>secret</password>\n"
        + "  </database>\n"
        + "  <application>\n"
        + "    <name>MyApp</name>\n"
        + "    <version>1.0</version>\n"
        + "  </application>\n"
        + "  <feature>\n"
        + "    <enabled>true</enabled>\n"
        + "  </feature>\n"
        + "</config>";
    
    File xmlFile = createTempXmlFile("config.xml", xmlContent);
    
    // Scan for tables - should find no repeating patterns
    List<XmlTableScanner.TableInfo> tables = XmlTableScanner.scanTables(Sources.of(xmlFile));
    
    assertEquals(0, tables.size());
  }
  
  @Test
  void testScanWithMixedContent() throws IOException {
    // Create test XML with mixed content types
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<mixed>\n"
        + "  <users>\n"
        + "    <user id=\"1\">\n"
        + "      <name>Alice</name>\n"
        + "      <email>alice@example.com</email>\n"
        + "    </user>\n"
        + "    <user id=\"2\">\n"
        + "      <name>Bob</name>\n"
        + "      <email>bob@example.com</email>\n"
        + "    </user>\n"
        + "  </users>\n"
        + "  <settings>\n"
        + "    <theme>dark</theme>\n"
        + "    <language>en</language>\n"
        + "  </settings>\n"
        + "  <logs>\n"
        + "    <entry timestamp=\"2023-01-01T10:00:00\">\n"
        + "      <level>INFO</level>\n"
        + "      <message>Application started</message>\n"
        + "    </entry>\n"
        + "    <entry timestamp=\"2023-01-01T10:01:00\">\n"
        + "      <level>DEBUG</level>\n"
        + "      <message>Debug message</message>\n"
        + "    </entry>\n"
        + "    <entry timestamp=\"2023-01-01T10:02:00\">\n"
        + "      <level>ERROR</level>\n"
        + "      <message>Error occurred</message>\n"
        + "    </entry>\n"
        + "  </logs>\n"
        + "</mixed>";
    
    File xmlFile = createTempXmlFile("mixed.xml", xmlContent);
    
    // Scan for tables
    List<XmlTableScanner.TableInfo> tables = XmlTableScanner.scanTables(Sources.of(xmlFile));
    
    // Should find users and log entries (both have 2+ similar elements)
    assertEquals(2, tables.size());
    
    boolean foundUsers = false;
    boolean foundEntries = false;
    
    for (XmlTableScanner.TableInfo table : tables) {
      if ("user".equals(table.name)) {
        foundUsers = true;
        assertEquals(2, table.count);
      } else if ("entry".equals(table.name)) {
        foundEntries = true;
        assertEquals(3, table.count);
      }
    }
    
    assertTrue(foundUsers, "Should find user table");
    assertTrue(foundEntries, "Should find entry table");
  }
  
  @Test
  void testNameSanitization() throws IOException {
    // Create test XML with names requiring sanitization
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<root>\n"
        + "  <product-item id=\"1\">\n"
        + "    <item-name>Item 1</item-name>\n"
        + "  </product-item>\n"
        + "  <product-item id=\"2\">\n"
        + "    <item-name>Item 2</item-name>\n"
        + "  </product-item>\n"
        + "</root>";
    
    File xmlFile = createTempXmlFile("products.xml", xmlContent);
    
    // Scan for tables
    List<XmlTableScanner.TableInfo> tables = XmlTableScanner.scanTables(Sources.of(xmlFile));
    
    assertEquals(1, tables.size());
    
    XmlTableScanner.TableInfo table = tables.get(0);
    // Hyphens should be converted to underscores
    assertEquals("product_item", table.name);
    assertEquals(2, table.count);
  }
  
  @Test
  void testInvalidXmlHandling() throws IOException {
    // Create invalid XML
    String invalidXml = "<?xml version=\"1.0\"?>\n"
        + "<root>\n"
        + "  <item>Unclosed tag\n"
        + "  <other>Valid</other>\n"
        + "</root>";
    
    File xmlFile = createTempXmlFile("invalid.xml", invalidXml);
    
    // Should throw IOException
    try {
      XmlTableScanner.scanTables(Sources.of(xmlFile));
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