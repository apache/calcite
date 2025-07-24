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

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a request to be made to an OpenAPI endpoint.
 * Built using the Builder pattern to handle various parameter types and configurations.
 */
public class OpenAPIRequest {

  private final org.apache.calcite.adapter.openapi.OpenAPIConfig.Variant variant;
  private final Map<String, Object> pathParams;
  private final List<NameValuePair> queryParams;
  private final Map<String, String> headers;
  private final Map<String, Object> bodyParams;

  private OpenAPIRequest(Builder builder) {
    this.variant = builder.variant;
    this.pathParams = Map.copyOf(builder.pathParams);
    this.queryParams = List.copyOf(builder.queryParams);
    this.headers = Map.copyOf(builder.headers);
    this.bodyParams = builder.bodyParams != null ? Map.copyOf(builder.bodyParams) : null;
  }

  public OpenAPIConfig.Variant getVariant() { return variant; }
  public Map<String, Object> getPathParams() { return pathParams; }
  public List<NameValuePair> getQueryParams() { return queryParams; }
  public Map<String, String> getHeaders() { return headers; }
  public Map<String, Object> getBodyParams() { return bodyParams; }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private OpenAPIConfig.Variant variant;
    private OpenAPITransport transport;
    private final Map<String, Object> pathParams = new HashMap<>();
    private final List<NameValuePair> queryParams = new ArrayList<>();
    private final Map<String, String> headers = new HashMap<>();
    private Map<String, Object> bodyParams;

    public Builder variant(OpenAPIConfig.Variant variant) {
      this.variant = variant;
      return this;
    }

    /**
     * Helper method to get existing JQL parameter for Jira-style sorting.
     */
    private String getExistingJqlParam() {
      return queryParams.stream()
          .filter(param -> "jql".equals(param.getName()))
          .map(NameValuePair::getValue)
          .findFirst()
          .orElse("");
    }

    public Builder transport(OpenAPITransport transport) {
      this.transport = transport;
      return this;
    }

    public Builder pathParam(String name, Object value) {
      pathParams.put(name, value);
      return this;
    }

    public Builder queryParam(String name, Object value) {
      queryParams.add(new BasicNameValuePair(name, String.valueOf(value)));
      return this;
    }

    public Builder header(String name, String value) {
      headers.put(name, value);
      return this;
    }

    public Builder bodyParam(String name, Object value) {
      if (bodyParams == null) {
        bodyParams = new HashMap<>();
      }
      bodyParams.put(name, value);
      return this;
    }

    /**
     * Add projection pushdown parameters based on configuration.
     */
    public Builder projection(List<Map.Entry<String, Class>> fields,
        OpenAPIConfig.ProjectionPushdown config) {

      // Extract field names
      List<String> fieldNames = fields.stream()
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());

      // Format according to configuration
      String formattedFields = formatFields(fieldNames, config.getFormat());

      // Apply template
      String paramValue = config.getTemplate().replace("{fields}", formattedFields);

      // Add to appropriate location
      switch (config.getLocation()) {
      case QUERY:
        queryParam(config.getParamName(), paramValue);
        break;
      case HEADER:
        header(config.getParamName(), paramValue);
        break;
      case BODY:
        bodyParam(config.getParamName(), paramValue);
        break;
      default:
        throw new IllegalArgumentException("Invalid location for projection: " + config.getLocation());
      }

      return this;
    }

    /**
     * Add sort pushdown parameters based on configuration.
     */
    public Builder sort(Map.Entry<String, RelFieldCollation.Direction> sortField,
        OpenAPIConfig.SortPushdown config) {

      String columnName = sortField.getKey();
      String direction = sortField.getValue().isDescending() ? "desc" : "asc";

      // Check if column is supported
      if (config.getSupportedColumns() != null &&
          !config.getSupportedColumns().contains(columnName)) {
        return this; // Skip unsupported columns
      }

      // Get API direction value
      String apiDirection = config.getDirectionValues().get(direction);

      // Format according to configuration
      String sortValue = formatSort(columnName, apiDirection, config.getFormat(), config);

      // Add to appropriate location
      switch (config.getLocation()) {
      case QUERY:
        if (config.getFormat() == OpenAPIConfig.SortFormat.SEPARATE_PARAMS) {
          queryParam(config.getParamName(), columnName);
          if (config.getDirectionParam() != null) {
            queryParam(config.getDirectionParam(), apiDirection);
          }
        } else if (config.getFormat() == OpenAPIConfig.SortFormat.JQL_ORDER_BY) {
          // Special handling for Jira JQL - append to existing JQL or create new
          String orderByClause = formatSort(columnName, apiDirection, config.getFormat(), config);
          // This is a simplified approach - in reality you'd need to parse existing JQL
          // and append the ORDER BY clause properly
          String existingJql = getExistingJqlParam();
          String newJql = existingJql.isEmpty() ? orderByClause : existingJql + " " + orderByClause;
          queryParam(config.getParamName(), newJql);
        } else {
          queryParam(config.getParamName(), sortValue);
        }
        break;
      case HEADER:
        header(config.getParamName(), sortValue);
        break;
      case BODY:
        if (config.getFormat() == OpenAPIConfig.SortFormat.SEPARATE_PARAMS) {
          bodyParam(config.getParamName(), columnName);
          if (config.getDirectionParam() != null) {
            bodyParam(config.getDirectionParam(), apiDirection);
          }
        } else {
          bodyParam(config.getParamName(), sortValue);
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid location for sort: " + config.getLocation());
      }

      return this;
    }

    /**
     * Add pagination parameters.
     */
    public Builder pagination(Long offset, Long fetch) {
      if (offset != null) {
        queryParam("offset", offset);
      }
      if (fetch != null) {
        queryParam("limit", fetch);
      }
      return this;
    }

    public OpenAPIRequest build() {
      if (variant == null) {
        throw new IllegalStateException("Variant must be set");
      }

      // Add default parameters from variant
      if (variant.getDefaultParams() != null) {
        variant.getDefaultParams().forEach((key, value) -> {
          queryParam(key, value);
        });
      }

      return new OpenAPIRequest(this);
    }

    private String formatFields(List<String> fields, OpenAPIConfig.ProjectionFormat format) {
      switch (format) {
      case COMMA:
        return String.join(",", fields);
      case JSON_ARRAY:
        return "[" + fields.stream()
            .map(f -> "\"" + f + "\"")
            .collect(Collectors.joining(",")) + "]";
      case SPACE:
        return String.join(" ", fields);
      case PIPE:
        return String.join("|", fields);
      default:
        throw new IllegalArgumentException("Unknown projection format: " + format);
      }
    }

    private String formatSort(String column, String direction,
        OpenAPIConfig.SortFormat format, OpenAPIConfig.SortPushdown config) {

      switch (format) {
      case SEPARATE_PARAMS:
        return column; // Direction handled separately
      case PREFIX:
        return "desc".equals(direction) ? "-" + column : column;
      case COMBINED:
        return column + "," + direction;
      case COLON:
        return column + ":" + direction;
      case JQL_ORDER_BY:
        // Special format for Jira JQL: "ORDER BY field DESC"
        return "ORDER BY " + column + " " + direction.toUpperCase();
      case SIMPLE:
        // Simple format: just the column name with optional prefix
        return "desc".equals(direction) ? "-" + column : column;
      default:
        throw new IllegalArgumentException("Unknown sort format: " + format);
      }
    }
  }
}
