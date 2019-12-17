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

import org.apache.commons.lang3.StringUtils;

import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
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
      throw RESOURCE.illegalBehaviorInExtractValueFunc(input, xpath).ex();
    }
  }
}
