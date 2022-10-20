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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.ErrorListener;
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
import javax.xml.xpath.XPathFactoryConfigurationException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * A collection of functions used in Xml processing.
 */
public class XmlFunctions {

  private static final ThreadLocal<@Nullable XPathFactory> XPATH_FACTORY =
      ThreadLocal.withInitial(() -> {
        final XPathFactory xPathFactory = XPathFactory.newInstance();
        try {
          xPathFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (XPathFactoryConfigurationException e) {
          throw new IllegalStateException("XPath Factory configuration failed", e);
        }
        return xPathFactory;
      });
  private static final ThreadLocal<@Nullable TransformerFactory> TRANSFORMER_FACTORY =
      ThreadLocal.withInitial(() -> {
        final TransformerFactory transformerFactory = TransformerFactory.newInstance();
        transformerFactory.setErrorListener(new InternalErrorListener());
        try {
          transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (TransformerConfigurationException e) {
          throw new IllegalStateException("Transformer Factory configuration failed", e);
        }
        return transformerFactory;
      });
  private static final ThreadLocal<@Nullable DocumentBuilderFactory> DOCUMENT_BUILDER_FACTORY =
      ThreadLocal.withInitial(() -> {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);
        documentBuilderFactory.setNamespaceAware(true);
        try {
          documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
          documentBuilderFactory
              .setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (final ParserConfigurationException e) {
          throw new IllegalStateException("Document Builder configuration failed", e);
        }
        return documentBuilderFactory;
      });

  private static final Pattern VALID_NAMESPACE_PATTERN = Pattern
      .compile("^(([0-9a-zA-Z:_-]+=\"[^\"]*\")( [0-9a-zA-Z:_-]+=\"[^\"]*\")*)$");
  private static final Pattern EXTRACT_NAMESPACE_PATTERN = Pattern
      .compile("([0-9a-zA-Z:_-]+)=(['\"])((?!\\2).+?)\\2");

  private XmlFunctions() {
  }

  public static @Nullable String extractValue(@Nullable String input, @Nullable String xpath) {
    if (input == null || xpath == null) {
      return null;
    }
    try {
      final Node documentNode = getDocumentNode(input);
      XPathExpression xpathExpression = castNonNull(XPATH_FACTORY.get()).newXPath().compile(xpath);
      try {
        NodeList nodes = (NodeList) xpathExpression
            .evaluate(documentNode, XPathConstants.NODESET);
        List<@Nullable String> result = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
          Node item = castNonNull(nodes.item(i));
          Node firstChild = requireNonNull(item.getFirstChild(),
              () -> "firstChild of node " + item);
          result.add(firstChild.getTextContent());
        }
        return StringUtils.join(result, " ");
      } catch (XPathExpressionException e) {
        return xpathExpression.evaluate(documentNode);
      }
    } catch (IllegalArgumentException | XPathExpressionException ex) {
      throw RESOURCE.invalidInputForExtractValue(input, xpath).ex();
    }
  }

  public static @Nullable String xmlTransform(@Nullable String xml, @Nullable String xslt) {
    if (xml == null || xslt == null) {
      return null;
    }
    try {
      final Source xsltSource = new StreamSource(new StringReader(xslt));
      final Source xmlSource = new StreamSource(new StringReader(xml));
      final Transformer transformer = castNonNull(TRANSFORMER_FACTORY.get())
          .newTransformer(xsltSource);
      final StringWriter writer = new StringWriter();
      final StreamResult result = new StreamResult(writer);
      transformer.setErrorListener(new InternalErrorListener());
      transformer.transform(xmlSource, result);
      return writer.toString();
    } catch (TransformerConfigurationException e) {
      throw RESOURCE.illegalXslt(xslt).ex();
    } catch (TransformerException e) {
      throw RESOURCE.invalidInputForXmlTransform(xml).ex();
    }
  }

  public static @Nullable String extractXml(@Nullable String xml, @Nullable String xpath) {
    return extractXml(xml, xpath, null);
  }

  public static @Nullable String extractXml(@Nullable String xml, @Nullable String xpath,
      @Nullable String namespace) {
    if (xml == null || xpath == null) {
      return null;
    }
    try {
      XPath xPath = castNonNull(XPATH_FACTORY.get()).newXPath();

      if (namespace != null) {
        xPath.setNamespaceContext(extractNamespaceContext(namespace));
      }

      XPathExpression xpathExpression = xPath.compile(xpath);

      final Node documentNode = getDocumentNode(xml);
      try {
        List<String> result = new ArrayList<>();
        NodeList nodes = (NodeList) xpathExpression
            .evaluate(documentNode, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          result.add(convertNodeToString(castNonNull(nodes.item(i))));
        }
        return StringUtils.join(result, "");
      } catch (XPathExpressionException e) {
        Node node = (Node) xpathExpression
            .evaluate(documentNode, XPathConstants.NODE);
        return convertNodeToString(node);
      }
    } catch (IllegalArgumentException | XPathExpressionException | TransformerException ex) {
      throw RESOURCE.invalidInputForExtractXml(xpath, namespace).ex();
    }
  }

  public static @Nullable Integer existsNode(@Nullable String xml, @Nullable String xpath) {
    return existsNode(xml, xpath, null);
  }

  public static @Nullable Integer existsNode(@Nullable String xml, @Nullable String xpath,
      @Nullable String namespace) {
    if (xml == null || xpath == null) {
      return null;
    }
    try {
      XPath xPath = castNonNull(XPATH_FACTORY.get()).newXPath();
      if (namespace != null) {
        xPath.setNamespaceContext(extractNamespaceContext(namespace));
      }

      XPathExpression xpathExpression = xPath.compile(xpath);
      final Node documentNode = getDocumentNode(xml);
      try {
        NodeList nodes = (NodeList) xpathExpression
            .evaluate(documentNode, XPathConstants.NODESET);
        if (nodes != null && nodes.getLength() > 0) {
          return 1;
        }
        return 0;
      } catch (XPathExpressionException e) {
        Node node = (Node) xpathExpression
            .evaluate(documentNode, XPathConstants.NODE);
        if (node != null) {
          return 1;
        }
        return 0;
      }
    } catch (IllegalArgumentException | XPathExpressionException ex) {
      throw RESOURCE.invalidInputForExistsNode(xpath, namespace).ex();
    }
  }

  private static SimpleNamespaceContext extractNamespaceContext(String namespace) {
    if (!VALID_NAMESPACE_PATTERN.matcher(namespace).find()) {
      throw new IllegalArgumentException("Invalid namespace " + namespace);
    }
    Map<String, String> namespaceMap = new HashMap<>();
    Matcher matcher = EXTRACT_NAMESPACE_PATTERN.matcher(namespace);
    while (matcher.find()) {
      namespaceMap.put(castNonNull(matcher.group(1)), castNonNull(matcher.group(3)));
    }
    return new SimpleNamespaceContext(namespaceMap);
  }

  private static String convertNodeToString(Node node) throws TransformerException {
    StringWriter writer = new StringWriter();
    Transformer transformer = castNonNull(TRANSFORMER_FACTORY.get()).newTransformer();
    transformer.setErrorListener(new InternalErrorListener());
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    transformer.transform(new DOMSource(node), new StreamResult(writer));
    return writer.toString();
  }

  private static Node getDocumentNode(final String xml) {
    try {
      final DocumentBuilder documentBuilder =
          castNonNull(DOCUMENT_BUILDER_FACTORY.get()).newDocumentBuilder();
      final InputSource inputSource = new InputSource(new StringReader(xml));
      return documentBuilder.parse(inputSource);
    } catch (final ParserConfigurationException | SAXException | IOException e) {
      throw new IllegalArgumentException("XML parsing failed", e);
    }
  }

  /** The internal default ErrorListener for Transformer. Just rethrows errors to
   * discontinue the XML transformation. */
  private static class InternalErrorListener implements ErrorListener {

    @Override public void warning(TransformerException exception) throws TransformerException {
      throw exception;
    }

    @Override public void error(TransformerException exception) throws TransformerException {
      throw exception;
    }

    @Override public void fatalError(TransformerException exception) throws TransformerException {
      throw exception;
    }
  }
}
