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
package org.apache.calcite.runtime;

import org.apache.calcite.util.SimpleNamespaceContext;

import org.apache.commons.lang3.StringUtils;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of functions used in Xml processing.
 */
public class XmlFunctions {

  private static final ThreadLocal<XPathFactory> XPATH_FACTORY =
      ThreadLocal.withInitial(XPathFactory::newInstance);
  private static final ThreadLocal<TransformerFactory> TRANSFORMER_FACTORY =
      ThreadLocal.withInitial(TransformerFactory::newInstance);

  private static final Pattern VALID_NAMESPACE_PATTERN = Pattern
      .compile("^(([0-9a-zA-Z:_-]+=\"[^\"]*\")( [0-9a-zA-Z:_-]+=\"[^\"]*\")*)$");
  private static final Pattern EXTRACT_NAMESPACE_PATTERN = Pattern
      .compile("([0-9a-zA-Z:_-]+)=(['\"])((?!\\2).+?)\\2");

  private XmlFunctions() {
  }

  public static String extractValue(String input, String xpath) {
    if (input == null || xpath == null) {
      return null;
    }
    try {
      XPathExpression xpathExpression = XPATH_FACTORY.get().newXPath().compile(xpath);
      try {
        NodeList nodes = (NodeList) xpathExpression
            .evaluate(new InputSource(new StringReader(input)), XPathConstants.NODESET);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
          result.add(nodes.item(i).getFirstChild().getTextContent());
        }
        return StringUtils.join(result, " ");
      } catch (XPathExpressionException e) {
        return xpathExpression.evaluate(new InputSource(new StringReader(input)));
      }
    } catch (XPathExpressionException ex) {
      throw RESOURCE.invalidInputForExtractValue(input, xpath).ex();
    }
  }

  public static String xmlTransform(String xml, String xslt) {
    if (xml == null || xslt == null) {
      return null;
    }
    try {
      final Source xsltSource = new StreamSource(new StringReader(xslt));
      final Source xmlSource = new StreamSource(new StringReader(xml));
      final Transformer transformer = TRANSFORMER_FACTORY.get().newTransformer(xsltSource);
      final StringWriter writer = new StringWriter();
      final StreamResult result = new StreamResult(writer);
      transformer.transform(xmlSource, result);
      return writer.toString();
    } catch (TransformerConfigurationException e) {
      throw RESOURCE.illegalXslt(xslt).ex();
    } catch (TransformerException e) {
      throw RESOURCE.invalidInputForXmlTransform(xml).ex();
    }
  }

  public static String extractXml(String xml, String xpath) {
    return extractXml(xml, xpath, null);
  }

  public static String extractXml(String xml, String xpath, String namespace) {
    if (xml == null || xpath == null) {
      return null;
    }
    try {
      XPath xPath = XPATH_FACTORY.get().newXPath();

      if (namespace != null) {
        if (!VALID_NAMESPACE_PATTERN.matcher(namespace).find()) {
          throw new IllegalArgumentException("Invalid namespace " + namespace);
        }
        Map<String, String> namespaceMap = new HashMap<>();
        Matcher matcher = EXTRACT_NAMESPACE_PATTERN.matcher(namespace);
        while (matcher.find()) {
          namespaceMap.put(matcher.group(1), matcher.group(3));
        }
        xPath.setNamespaceContext(new SimpleNamespaceContext(namespaceMap));
      }

      XPathExpression xpathExpression = xPath.compile(xpath);

      try {
        List<String> result = new ArrayList<>();
        NodeList nodes = (NodeList) xpathExpression
            .evaluate(new InputSource(new StringReader(xml)), XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          result.add(convertNodeToString(nodes.item(i)));
        }
        return StringUtils.join(result, "");
      } catch (XPathExpressionException e) {
        Node node = (Node) xpathExpression
            .evaluate(new InputSource(new StringReader(xml)), XPathConstants.NODE);
        return convertNodeToString(node);
      }
    } catch (IllegalArgumentException | XPathExpressionException | TransformerException ex) {
      throw RESOURCE.invalidInputForExtractXml(xpath, namespace).ex();
    }
  }

  private static String convertNodeToString(Node node) throws TransformerException {
    StringWriter writer = new StringWriter();
    Transformer transformer = TRANSFORMER_FACTORY.get().newTransformer();
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    transformer.transform(new DOMSource(node), new StreamResult(writer));
    return writer.toString();
  }
}
