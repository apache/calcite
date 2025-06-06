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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * Query against Splunk.
 *
 * @param <T> Element type
 */
public class SplunkQuery<T> extends AbstractEnumerable<T> {
  private final SplunkConnection splunkConnection;
  private final String search;
  private final String earliest;
  private final String latest;
  private final List<String> fieldList;
  private final Set<String> explicitFields;
  private final Map<String, String> fieldMapping;

  /** Creates a SplunkQuery. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList) {
    this(splunkConnection, search, earliest, latest, fieldList,
        Collections.emptySet(), Collections.emptyMap());
  }

  /** Creates a SplunkQuery with explicit field information. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields) {
    this(splunkConnection, search, earliest, latest, fieldList,
        explicitFields, Collections.emptyMap());
  }

  /** Creates a SplunkQuery with explicit field information and field mapping. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields,
      Map<String, String> fieldMapping) {
    this.splunkConnection = splunkConnection;
    this.search = search;
    this.earliest = earliest;
    this.latest = latest;
    this.fieldList = fieldList;
    this.explicitFields = explicitFields;
    this.fieldMapping = fieldMapping != null ? fieldMapping : Collections.emptyMap();
  }

  @Override public String toString() {
    return "SplunkQuery {" + search + "}";
  }

  @SuppressWarnings("unchecked")
  @Override public Enumerator<T> enumerator() {
    // Map schema field names to Splunk field names for the query
    List<String> mappedFieldList = mapFieldList(fieldList);

    // Create a reverse mapping for result processing (Splunk field -> schema field)
    Map<String, String> reverseMapping = createReverseMapping();

    // Use the 5-parameter method with field mapping support
    return (Enumerator<T>) splunkConnection.getSearchResultEnumerator(
        search, getArgs(), mappedFieldList, explicitFields, reverseMapping);
  }

  /**
   * Maps schema field names to Splunk field names using the field mapping.
   */
  private List<String> mapFieldList(List<String> schemaFieldList) {
    return schemaFieldList.stream()
        .map(field -> fieldMapping.getOrDefault(field, field))
        .collect(Collectors.toList());
  }

  /**
   * Creates a reverse mapping from Splunk field names to schema field names.
   * This is used when processing results to map back to schema field names.
   */
  private Map<String, String> createReverseMapping() {
    Map<String, String> reverse = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
      reverse.put(entry.getValue(), entry.getKey());
    }
    return reverse;
  }

  private Map<String, String> getArgs() {
    Map<String, String> args = new HashMap<>();

    // Map the field list to Splunk field names for the field_list parameter
    List<String> mappedFieldList = mapFieldList(fieldList);

    // If _extra is requested, we need both the mapped fields AND additional fields
    if (fieldList.contains("_extra")) {
      // Keep the mapped fields for CIM extraction, but add additional raw fields
      mappedFieldList = new ArrayList<>(mappedFieldList);
      mappedFieldList.remove("_extra"); // Remove _extra from the field list
      // Add common extra fields that aren't in CIM
//      mappedFieldList.add("_raw");
//      mappedFieldList.add("_serial");
//      mappedFieldList.add("_time");
//      enhancedFieldList.add("_indextime");
//      enhancedFieldList.add("_kv");
//      enhancedFieldList.add("_meta");
//      enhancedFieldList.add("_sourcetype");
//      enhancedFieldList.add("splunk_server");
//      enhancedFieldList.add("punct");
      enhancedFieldList.add("*");
      // Don't add "*" as it might interfere with CIM field extraction
    }

    String fields = StringUtils.encodeList(mappedFieldList, ',').toString();
    args.put("field_list", fields);

    args.put("earliest_time", earliest);
    args.put("latest_time", latest);
    return args;
  }

  /**
   * Returns the field mapping for external use.
   */
  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }
}
