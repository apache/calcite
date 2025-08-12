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

import org.apache.calcite.util.Source;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Scans XML files to discover repeating element patterns that can be treated as tables.
 * Supports both automatic pattern detection and XPath-based element selection.
 */
public class XmlTableScanner {
  private static final Pattern INVALID_NAME_CHARS = Pattern.compile("[^a-zA-Z0-9_]");
  private static final int MIN_REPEATING_ELEMENTS = 2;
  private static final int MAX_DISCOVERY_DEPTH = 5;
  
  private XmlTableScanner() {
    // Utility class should not be instantiated
  }
  
  /**
   * Information about a discovered XML table (repeating element pattern).
   */
  public static class TableInfo {
    public final String name;
    public final String xpath;
    public final List<Element> elements;
    public final int count;
    
    TableInfo(String name, String xpath, List<Element> elements) {
      this.name = name;
      this.xpath = xpath;
      this.elements = new ArrayList<>(elements);
      this.count = elements.size();
    }
  }
  
  /**
   * Scans an XML source and returns information about all repeating element patterns.
   * Uses automatic pattern detection.
   *
   * @param source The XML source to scan
   * @return List of table information
   * @throws IOException If the source cannot be read
   */
  public static List<TableInfo> scanTables(Source source) throws IOException {
    return scanTables(source, null);
  }
  
  /**
   * Scans an XML source for tables using optional XPath expression.
   *
   * @param source The XML source to scan
   * @param xpath Optional XPath expression to target specific elements
   * @return List of table information
   * @throws IOException If the source cannot be read
   */
  public static List<TableInfo> scanTables(Source source, String xpath) throws IOException {
    try {
      Document document = parseXmlDocument(source);
      
      if (xpath != null && !xpath.trim().isEmpty()) {
        return scanWithXPath(document, xpath);
      } else {
        return scanForRepeatingPatterns(document);
      }
      
    } catch (Exception e) {
      throw new IOException("Failed to parse XML document: " + source.path(), e);
    }
  }
  
  /**
   * Parse XML document from source.
   */
  private static Document parseXmlDocument(Source source) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    // Security settings
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    
    DocumentBuilder builder = factory.newDocumentBuilder();
    
    try (InputStream inputStream = source.openStream()) {
      return builder.parse(inputStream);
    }
  }
  
  /**
   * Scan for tables using XPath expression.
   */
  private static List<TableInfo> scanWithXPath(Document document, String xpathExpression) 
      throws Exception {
    XPath xpath = XPathFactory.newInstance().newXPath();
    NodeList nodes = (NodeList) xpath.evaluate(xpathExpression, document, XPathConstants.NODESET);
    
    List<TableInfo> tables = new ArrayList<>();
    
    if (nodes.getLength() >= MIN_REPEATING_ELEMENTS) {
      List<Element> elements = new ArrayList<>();
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        if (node instanceof Element) {
          elements.add((Element) node);
        }
      }
      
      if (!elements.isEmpty()) {
        String tableName = deriveTableNameFromXPath(xpathExpression);
        tables.add(new TableInfo(tableName, xpathExpression, elements));
      }
    }
    
    return tables;
  }
  
  /**
   * Scan document for repeating element patterns automatically.
   */
  private static List<TableInfo> scanForRepeatingPatterns(Document document) {
    List<TableInfo> tables = new ArrayList<>();
    Map<String, Integer> nameConflicts = new HashMap<>();
    
    // Find patterns by looking at sibling groups, preferring top-level patterns
    findSiblingPatternsWithPriority(document.getDocumentElement(), tables, nameConflicts, 0);
    
    return tables;
  }
  
  /**
   * Find repeating patterns prioritizing larger, top-level patterns over smaller nested ones.
   */
  private static void findSiblingPatternsWithPriority(Element parent, 
                                                       List<TableInfo> tables,
                                                       Map<String, Integer> nameConflicts,
                                                       int depth) {
    if (depth > MAX_DISCOVERY_DEPTH) {
      return;
    }
    
    // Group child elements by tag name
    Map<String, List<Element>> childGroups = new LinkedHashMap<>();
    NodeList children = parent.getChildNodes();
    
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child instanceof Element) {
        Element childElement = (Element) child;
        String elementName = childElement.getLocalName() != null ? 
            childElement.getLocalName() : childElement.getTagName();
        
        if (hasDataContent(childElement)) {
          childGroups.computeIfAbsent(elementName, k -> new ArrayList<>()).add(childElement);
        }
      }
    }
    
    // Check for repeating patterns in this level
    List<String> processedNames = new ArrayList<>();
    for (Map.Entry<String, List<Element>> entry : childGroups.entrySet()) {
      List<Element> elements = entry.getValue();
      
      if (elements.size() >= MIN_REPEATING_ELEMENTS && areElementsSimilar(elements)) {
        String elementName = entry.getKey();
        String tableName = resolveNameConflict(elementName, nameConflicts);
        String xpath = generateXPathForElements(elements);
        
        tables.add(new TableInfo(tableName, xpath, elements));
        processedNames.add(elementName);
      }
    }
    
    // Recursively process child elements, but skip those that became tables
    for (Map.Entry<String, List<Element>> entry : childGroups.entrySet()) {
      if (!processedNames.contains(entry.getKey())) {
        for (Element element : entry.getValue()) {
          findSiblingPatternsWithPriority(element, tables, nameConflicts, depth + 1);
        }
      }
    }
  }
  
  /**
   * Check if element has data content (text or child elements).
   */
  private static boolean hasDataContent(Element element) {
    // Check for text content
    String textContent = element.getTextContent();
    if (textContent != null && !textContent.trim().isEmpty()) {
      return true;
    }
    
    // Check for attributes
    if (element.getAttributes().getLength() > 0) {
      return true;
    }
    
    // Check for child elements
    NodeList children = element.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child instanceof Element) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Check if elements in the group have similar structure.
   */
  private static boolean areElementsSimilar(List<Element> elements) {
    if (elements.size() < 2) {
      return false;
    }
    
    // Compare first two elements to determine similarity
    Element first = elements.get(0);
    Element second = elements.get(1);
    
    return haveSimilarStructure(first, second);
  }
  
  /**
   * Check if two elements have similar structure.
   */
  private static boolean haveSimilarStructure(Element e1, Element e2) {
    // Compare number of attributes
    int attrs1 = e1.getAttributes().getLength();
    int attrs2 = e2.getAttributes().getLength();
    
    // Allow some flexibility in attribute count
    if (Math.abs(attrs1 - attrs2) > 2) {
      return false;
    }
    
    // Compare child element names
    List<String> childNames1 = getChildElementNames(e1);
    List<String> childNames2 = getChildElementNames(e2);
    
    // Calculate similarity - at least 50% overlap in child element names
    long commonNames = childNames1.stream()
        .filter(childNames2::contains)
        .count();
    
    int maxChildCount = Math.max(childNames1.size(), childNames2.size());
    if (maxChildCount == 0) {
      return true; // Both have no children
    }
    
    double similarity = (double) commonNames / maxChildCount;
    return similarity >= 0.5;
  }
  
  /**
   * Get child element names for structure comparison.
   */
  private static List<String> getChildElementNames(Element element) {
    List<String> names = new ArrayList<>();
    NodeList children = element.getChildNodes();
    
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child instanceof Element) {
        Element childElement = (Element) child;
        String name = childElement.getLocalName() != null ? 
            childElement.getLocalName() : childElement.getTagName();
        names.add(name);
      }
    }
    
    return names;
  }
  
  /**
   * Generate XPath expression for a group of similar elements.
   */
  private static String generateXPathForElements(List<Element> elements) {
    if (elements.isEmpty()) {
      return "";
    }
    
    Element firstElement = elements.get(0);
    String elementName = firstElement.getLocalName() != null ? 
        firstElement.getLocalName() : firstElement.getTagName();
    
    // Simple XPath - could be enhanced to be more specific
    return "//" + elementName;
  }
  
  /**
   * Derive table name from XPath expression.
   */
  private static String deriveTableNameFromXPath(String xpath) {
    // Extract element name from XPath
    // Handle patterns like: //product, /catalog/product, //ns:product
    String[] segments = xpath.split("/");
    String lastSegment = segments[segments.length - 1];
    
    // Remove namespace prefix if present
    if (lastSegment.contains(":")) {
      lastSegment = lastSegment.substring(lastSegment.lastIndexOf(":") + 1);
    }
    
    // Remove array notation or predicates
    if (lastSegment.contains("[")) {
      lastSegment = lastSegment.substring(0, lastSegment.indexOf("["));
    }
    
    return sanitizeName(lastSegment);
  }
  
  /**
   * Resolve naming conflicts by adding index suffix.
   */
  private static String resolveNameConflict(String baseName, Map<String, Integer> nameConflicts) {
    String sanitized = sanitizeName(baseName);
    
    if (!nameConflicts.containsKey(sanitized)) {
      nameConflicts.put(sanitized, 0);
      return sanitized;
    }
    
    int count = nameConflicts.get(sanitized) + 1;
    nameConflicts.put(sanitized, count);
    return sanitized + "_" + count;
  }
  
  /**
   * Sanitizes a string to be a valid table name.
   */
  private static String sanitizeName(String name) {
    if (name == null || name.isEmpty()) {
      return "Table";
    }
    
    // Remove invalid characters
    name = INVALID_NAME_CHARS.matcher(name).replaceAll("_");
    
    // Remove consecutive underscores
    while (name.contains("__")) {
      name = name.replace("__", "_");
    }
    
    // Remove leading/trailing underscores
    name = name.replaceAll("^_+|_+$", "");
    
    // Ensure it starts with a letter or underscore
    if (!name.isEmpty() && !Character.isLetter(name.charAt(0)) && name.charAt(0) != '_') {
      name = "_" + name;
    }
    
    // Limit length
    if (name.length() > 50) {
      name = name.substring(0, 50);
    }
    
    return name.isEmpty() ? "Table" : name;
  }
}