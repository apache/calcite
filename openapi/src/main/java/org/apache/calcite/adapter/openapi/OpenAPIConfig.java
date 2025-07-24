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
package org.apache.calcite.adapter.openapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for OpenAPI table variants and pushdown capabilities.
 */
public class OpenAPIConfig {

  private final List<Variant> variants;
  private final Authentication authentication;
  private final Map<String, Object> defaultParams;

  @JsonCreator
  public OpenAPIConfig(
      @JsonProperty("variants") List<Variant> variants,
      @JsonProperty("authentication") Authentication authentication,
      @JsonProperty("defaultParams") Map<String, Object> defaultParams) {
    this.variants = Objects.requireNonNull(variants, "variants");
    this.authentication = authentication;
    this.defaultParams = defaultParams;
  }

  public List<Variant> getVariants() { return variants; }
  public Authentication getAuthentication() { return authentication; }
  public Map<String, Object> getDefaultParams() { return defaultParams; }

  /**
   * Configuration for a specific API endpoint variant.
   */
  public static class Variant {
    private final String name;
    private final String path;
    private final String httpMethod;
    private final List<String> requiredFilters;
    private final List<String> optionalFilters;
    private final String arrayPath;
    private final Map<String, Object> defaultParams;
    private final ProjectionPushdown projectionPushdown;
    private final SortPushdown sortPushdown;

    @JsonCreator
    public Variant(
        @JsonProperty("name") String name,
        @JsonProperty("path") String path,
        @JsonProperty("httpMethod") String httpMethod,
        @JsonProperty("requiredFilters") List<String> requiredFilters,
        @JsonProperty("optionalFilters") List<String> optionalFilters,
        @JsonProperty("arrayPath") String arrayPath,
        @JsonProperty("defaultParams") Map<String, Object> defaultParams,
        @JsonProperty("projectionPushdown") ProjectionPushdown projectionPushdown,
        @JsonProperty("sortPushdown") SortPushdown sortPushdown) {
      this.name = Objects.requireNonNull(name, "name");
      this.path = Objects.requireNonNull(path, "path");
      this.httpMethod = httpMethod != null ? httpMethod : "GET";
      this.requiredFilters = requiredFilters != null ? requiredFilters : List.of();
      this.optionalFilters = optionalFilters != null ? optionalFilters : List.of();
      this.arrayPath = arrayPath;
      this.defaultParams = defaultParams;
      this.projectionPushdown = projectionPushdown;
      this.sortPushdown = sortPushdown;
    }

    public String getName() { return name; }
    public String getPath() { return path; }
    public String getHttpMethod() { return httpMethod; }
    public List<String> getRequiredFilters() { return requiredFilters; }
    public List<String> getOptionalFilters() { return optionalFilters; }
    public String getArrayPath() { return arrayPath; }
    public Map<String, Object> getDefaultParams() { return defaultParams; }
    public ProjectionPushdown getProjectionPushdown() { return projectionPushdown; }
    public SortPushdown getSortPushdown() { return sortPushdown; }
  }

  /**
   * Configuration for projection pushdown (field selection).
   */
  public static class ProjectionPushdown {
    private final String paramName;
    private final ParameterLocation location;
    private final ProjectionFormat format;
    private final String template;

    @JsonCreator
    public ProjectionPushdown(
        @JsonProperty("paramName") String paramName,
        @JsonProperty("location") ParameterLocation location,
        @JsonProperty("format") ProjectionFormat format,
        @JsonProperty("template") String template) {
      this.paramName = Objects.requireNonNull(paramName, "paramName");
      this.location = location != null ? location : ParameterLocation.QUERY;
      this.format = format != null ? format : ProjectionFormat.COMMA;
      this.template = template != null ? template : "{fields}";
    }

    public String getParamName() { return paramName; }
    public ParameterLocation getLocation() { return location; }
    public ProjectionFormat getFormat() { return format; }
    public String getTemplate() { return template; }
  }

  /**
   * Configuration for sort pushdown.
   */
  public static class SortPushdown {
    private final String paramName;
    private final ParameterLocation location;
    private final String directionParam;
    private final SortFormat format;
    private final Map<String, String> directionValues;
    private final List<String> supportedColumns;

    @JsonCreator
    public SortPushdown(
        @JsonProperty("paramName") String paramName,
        @JsonProperty("location") ParameterLocation location,
        @JsonProperty("directionParam") String directionParam,
        @JsonProperty("format") SortFormat format,
        @JsonProperty("directionValues") Map<String, String> directionValues,
        @JsonProperty("supportedColumns") List<String> supportedColumns) {
      this.paramName = Objects.requireNonNull(paramName, "paramName");
      this.location = location != null ? location : ParameterLocation.QUERY;
      this.directionParam = directionParam;
      this.format = format != null ? format : SortFormat.SEPARATE_PARAMS;
      this.directionValues = directionValues != null ? directionValues :
          Map.of("asc", "asc", "desc", "desc");
      this.supportedColumns = supportedColumns;
    }

    public String getParamName() { return paramName; }
    public ParameterLocation getLocation() { return location; }
    public String getDirectionParam() { return directionParam; }
    public SortFormat getFormat() { return format; }
    public Map<String, String> getDirectionValues() { return directionValues; }
    public List<String> getSupportedColumns() { return supportedColumns; }
  }

  /**
   * Authentication configuration.
   */
  public static class Authentication {
    private final AuthType type;
    private final ParameterLocation location;
    private final String paramName;
    private final String template;
    private final Map<String, String> credentials;

    @JsonCreator
    public Authentication(
        @JsonProperty("type") AuthType type,
        @JsonProperty("location") ParameterLocation location,
        @JsonProperty("paramName") String paramName,
        @JsonProperty("template") String template,
        @JsonProperty("credentials") Map<String, String> credentials) {
      this.type = Objects.requireNonNull(type, "type");
      this.location = location != null ? location : ParameterLocation.HEADER;
      this.paramName = paramName;
      this.template = template;
      this.credentials = credentials;
    }

    public AuthType getType() { return type; }
    public ParameterLocation getLocation() { return location; }
    public String getParamName() { return paramName; }
    public String getTemplate() { return template; }
    public Map<String, String> getCredentials() { return credentials; }
  }

  public enum ParameterLocation {
    QUERY, HEADER, PATH, BODY
  }

  public enum ProjectionFormat {
    COMMA,        // "field1,field2,field3"
    JSON_ARRAY,   // ["field1","field2","field3"]
    SPACE,        // "field1 field2 field3"
    PIPE          // "field1|field2|field3"
  }

  public enum SortFormat {
    SEPARATE_PARAMS,  // ?sort=name&order=desc
    PREFIX,           // ?sort=-name
    COMBINED,         // ?sort=name,desc
    COLON,            // ?sort=name:desc
    JQL_ORDER_BY,     // ?jql=...ORDER BY name DESC (Jira-style)
    SIMPLE            // ?orderBy=name or ?orderBy=-name
  }

  public enum AuthType {
    API_KEY, BASIC, OAUTH2, BEARER
  }
}
