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
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import graphql.GraphQL;
import graphql.schema.*;

/**
 * Represents a Calcite schema that generates tables based on the types defined in a GraphQL schema.
 */
public class GraphQLCalciteSchema extends AbstractSchema {
  final GraphQL graphQL;
  private final SchemaPlus parentSchema;
  private final String name;
  private final String endpoint;
  @Nullable public final String role;
  @Nullable public final String user;
  @Nullable public final String auth;
  @Nullable private Map<String, Table> tableMap;
  @Nullable private final Map<String, Object> cacheConfig;
  private final Integer objectDepth;
  private final Boolean pseudoKeys;

  private static final List<String> excludedNames =
      Arrays.asList("Subscription", "Mutation", "Query", "__EnumValue", "__Field", "__InputValue",
      "__Schema", "__Type", "__Directive");

  public GraphQLCalciteSchema(GraphQL graphQL, SchemaPlus parentSchema,
      String name, String endpoint, @Nullable String role,
      @Nullable String auth, @Nullable String user,
      @Nullable Map<String, Object> cacheConfig, @Nullable Integer objectDepth, @Nullable Boolean pseudoKeys) {
    this.graphQL = graphQL;
    this.parentSchema = parentSchema;
    this.name = name;
    this.endpoint = endpoint;
    this.role = role;
    this.auth = auth;
    this.user = user;
    this.cacheConfig = cacheConfig;
    this.objectDepth = (objectDepth != null) ? objectDepth : 2;
    this.pseudoKeys = (pseudoKeys != null) ? pseudoKeys : false;
  }

  public @Nullable Map<String, Object> getCacheConfig() {
    return cacheConfig;
  }

  /**
   * Retrieves a map of table names to Table objects representing the tables based on types defined in the GraphQL schema.
   *
   * @return a map of table names to Table objects
   */
  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = new HashMap<>();
      GraphQLSchema schema = graphQL.getGraphQLSchema();
      schema.getTypeMap().values().stream()
          .filter(type -> type instanceof GraphQLObjectType &&
              !excludedNames.contains(type.getName()) &&
              !type.getName().endsWith("AggExp"))
          .forEach(type -> {
            GraphQLObjectType objectType = (GraphQLObjectType) type;

            // Find all types that have a list reference to this type
            List<GraphQLObjectType> referencingTypes = schema.getTypeMap().values().stream()
                .filter(t -> t instanceof GraphQLObjectType &&
                    !excludedNames.contains(t.getName()) &&
                    !t.getName().endsWith("AggExp"))
                .map(t -> (GraphQLObjectType) t)
                .filter(t -> hasListReferenceToType(t, objectType))
                .collect(Collectors.toList());

            GraphQLTable proposedTable = new GraphQLTable(this, objectType, graphQL, endpoint, referencingTypes);
            tableMap.put(objectType.getName(), proposedTable);
          });
    }
    return tableMap;
  }

  private boolean hasListReferenceToType(GraphQLObjectType sourceType, GraphQLObjectType targetType) {
    return sourceType.getFieldDefinitions().stream()
        .map(GraphQLFieldDefinition::getType)
        .anyMatch(fieldType -> {
          // Unwrap from GraphQLNonNull if present
          if (fieldType instanceof GraphQLNonNull) {
            fieldType = (GraphQLOutputType) ((GraphQLNonNull) fieldType).getWrappedType();
          }

          // Check if it's a List
          if (fieldType instanceof GraphQLList) {
            GraphQLType listType = ((GraphQLList) fieldType).getWrappedType();

            // Unwrap the list type from GraphQLNonNull if present
            if (listType instanceof GraphQLNonNull) {
              listType = ((GraphQLNonNull) listType).getWrappedType();
            }

            // Compare the unwrapped type with our target type
            return listType == targetType;
          }
          return false;
        });
  }

  @Override public String toString() {
    return "CalciteGraphQLSchema {name=" + name + "}";
  }

  public Integer getObjectDepth() {
    return objectDepth;
  }

  public Boolean getPseudoKeys() {
    return pseudoKeys;
  }
}
