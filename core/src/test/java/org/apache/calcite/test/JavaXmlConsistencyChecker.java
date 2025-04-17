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
package org.apache.calcite.test;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * This class provides utility methods to validate the consistency between
 * test cases defined in the XML file and the corresponding test methods
 * implemented in the Java file. It ensures that all test cases in the XML
 * file have matching test methods in the Java class.
 */
public class JavaXmlConsistencyChecker {
  final String javaFilePrefix = "src/test/java/org/apache/calcite/test/";
  final String xmlFilePrefix = "src/test/resources/org/apache/calcite/test/";
  final List<String> fileNames = ImmutableList.of(
      "RelOptRulesTest",
      "HepPlannerTest",
      "RuleMatchVisualizerTest",
      "SqlHintsConverterTest",
      "SqlLimitsTest",
      "SqlToRelConverterTest",
      "TopDownOptTest",
      "TypeCoercionConverterTest");

  private static Set<String> parseTestCaseNamesFromXml(String filePath) throws Exception {
    Set<String> testCaseNames = new HashSet<>();
    File xmlFile = new File(filePath);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document document = builder.parse(xmlFile);

    NodeList testCaseNodes = document.getElementsByTagName("TestCase");
    for (int i = 0; i < testCaseNodes.getLength(); i++) {
      Element testCaseElement = (Element) testCaseNodes.item(i);
      String name = testCaseElement.getAttribute("name");
      if (!name.isEmpty()) {
        testCaseNames.add(name);
      }
    }
    return testCaseNames;
  }

  private static Set<String> parseMethodNamesFromJava(String filePath) throws Exception {
    Set<String> methodNames = new HashSet<>();
    final String content = Files.readAllLines(Paths.get(filePath)).toString();

    Pattern pattern = Pattern.compile("@Test\\s+(?:public\\s+)?void\\s+(\\w+)\\s*\\(");
    Matcher matcher = pattern.matcher(content);

    while (matcher.find()) {
      methodNames.add(matcher.group(1));
    }
    return methodNames;
  }

  private static void check(String testName, String xmlFilePath, String javaFilePath)
      throws Exception {
    Set<String> testCaseNames = parseTestCaseNamesFromXml(xmlFilePath);
    Set<String> javaMethodNames = parseMethodNamesFromJava(javaFilePath);

    List<String> missingMethods = new ArrayList<>();
    for (String testCase : testCaseNames) {
      if (!javaMethodNames.contains(testCase)) {
        missingMethods.add(testCase);
      }
    }

    if (!missingMethods.isEmpty()) {
      throw new AssertionError(
          "The following test methods are missing in " + testName + ": " + missingMethods);
    }
  }

  @Test void validateFiles() throws Exception {
    for (String fileName : fileNames) {
      String xmlFilePath = xmlFilePrefix + fileName + ".xml";
      String javaFilePath = javaFilePrefix + fileName + ".java";
      check(fileName, xmlFilePath, javaFilePath);
    }
  }
}
