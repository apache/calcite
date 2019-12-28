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
package org.apache.calcite.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;

public class SimpleNamespaceContext implements NamespaceContext {

  private final Map<String, String> prefixToNamespaceUri = new HashMap<>();
  private final Map<String, Set<String>> namespaceUriToPrefixes = new HashMap<>();

  public SimpleNamespaceContext(Map<String, String> bindings) {
    bindNamespaceUri(XMLConstants.XML_NS_PREFIX, XMLConstants.XML_NS_URI);
    bindNamespaceUri(XMLConstants.XMLNS_ATTRIBUTE, XMLConstants.XMLNS_ATTRIBUTE_NS_URI);
    bindNamespaceUri(XMLConstants.DEFAULT_NS_PREFIX, "");
    bindings.forEach(this::bindNamespaceUri);
  }

  @Override public String getNamespaceURI(String prefix) {
    if (this.prefixToNamespaceUri.containsKey(prefix)) {
      return this.prefixToNamespaceUri.get(prefix);
    }
    return "";
  }

  @Override public String getPrefix(String namespaceUri) {
    Set<String> prefixes = getPrefixesSet(namespaceUri);
    return !prefixes.isEmpty() ? prefixes.iterator().next() : null;
  }

  @Override public Iterator<String> getPrefixes(String namespaceUri) {
    return getPrefixesSet(namespaceUri).iterator();
  }

  private Set<String> getPrefixesSet(String namespaceUri) {
    Set<String> prefixes = this.namespaceUriToPrefixes.get(namespaceUri);
    return prefixes != null ? Collections.unmodifiableSet(prefixes) : Collections.emptySet();
  }

  private void bindNamespaceUri(String prefix, String namespaceUri) {
    this.prefixToNamespaceUri.put(prefix, namespaceUri);
    Set<String> prefixes = this.namespaceUriToPrefixes
        .computeIfAbsent(namespaceUri, k -> new LinkedHashSet<>());
    prefixes.add(prefix);
  }
}
