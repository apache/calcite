package org.apache.calcite.adapter.graphql;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.SchemaPlus;
import graphql.GraphQL;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Represents a Calcite schema that generates tables based on the types defined in a GraphQL schema.
 */
public class GraphQLCalciteSchema extends AbstractSchema {
  private final GraphQL graphQL;
  private final SchemaPlus parentSchema;
  private final String name;
  private final String endpoint;
  @Nullable public final String role;
  @Nullable public final String user;
  @Nullable public final String auth;
  @Nullable private Map<String, Table> tableMap;
  private static final List<String> excludedNames = Arrays.asList(
      "Mutation", "Query", "__EnumValue", "__Field", "__InputValue",
      "__Schema", "__Type", "__Directive");

  public GraphQLCalciteSchema(GraphQL graphQL, SchemaPlus parentSchema, String name, String endpoint, @Nullable String role, @Nullable String auth, @Nullable String user) {
    this.graphQL = graphQL;
    this.parentSchema = parentSchema;
    this.name = name;
    this.endpoint = endpoint;
    this.role = role;
    this.auth = auth;
    this.user = user;
  }

  /**
   * Retrieves a map of table names to Table objects representing the tables based on types defined in the GraphQL schema.
   *
   * @return a map of table names to Table objects
   */
  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = new HashMap<>();
      GraphQLSchema schema = graphQL.getGraphQLSchema();
      schema.getTypeMap().values().stream()
          .filter(type -> type instanceof GraphQLObjectType &&
              !excludedNames.contains(type.getName()) &&
              !type.getName().endsWith("AggExp"))
          .forEach(type -> {
            GraphQLObjectType objectType = (GraphQLObjectType) type;
            tableMap.put(objectType.getName(),
                new GraphQLTable(this, objectType, graphQL, endpoint));
          });
    }
    return tableMap;
  }

  @Override
  public String toString() {
    return "CalciteGraphQLSchema {name=" + name + "}";
  }
}
