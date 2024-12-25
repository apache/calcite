package org.apache.calcite.adapter.graphql;

import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.trace.CalciteTrace;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.*;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating GraphQL schemas based on provided endpoint and configuration.
 */
public class GraphQLSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();
  private static final Map<String, GraphQL> ENDPOINT_TO_GRAPHQL = new ConcurrentHashMap<>();
  private static final Map<String, GraphQLCalciteSchema> CREATED_SCHEMAS = new ConcurrentHashMap<>();

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String endpoint = (String) operand.get("endpoint");
    @Nullable String user = (String) operand.get("user");
    @Nullable String role = (String) operand.get("role");
    @Nullable String auth = (String) operand.get("auth");
    @Nullable Integer objectDepth = (Integer) operand.get("objectDepth");
    @Nullable Boolean pseudoKeys = (Boolean) operand.get("pseudoKeys");

    // Extract cache configuration from operands
    @Nullable Map<String, Object> cacheConfig = null;
    @Nullable Object possibleMap = operand.get("cache");
    if (possibleMap instanceof Map) {
      cacheConfig = (Map<String, Object>) possibleMap;
    }


    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Creating GraphQL schema for endpoint: {}", endpoint);
    }

    GraphQL graphQL = ENDPOINT_TO_GRAPHQL.computeIfAbsent(endpoint, this::createGraphQLClient);
    GraphQLCalciteSchema schema = new GraphQLCalciteSchema(
        graphQL,
        parentSchema,
        name,
        endpoint,
        role,
        auth,
        user,
        cacheConfig,
        objectDepth,
        pseudoKeys
    );
    CREATED_SCHEMAS.put(name, schema);
    return schema;
  }

  private GraphQL createGraphQLClient(String endpoint) {
    try {
      String sdl = GraphQLSDLRetriever.retrieveSDL(endpoint);
      SchemaParser schemaParser = new SchemaParser();
      TypeDefinitionRegistry typeRegistry = schemaParser.parse(sdl);

      Set<String> customScalars = new HashSet<>();
      typeRegistry.scalars().forEach((name, definition) -> {
        if (!isBuiltInScalar(name)) {
          customScalars.add(name);
        }
      });

      RuntimeWiring.Builder runtimeWiringBuilder = RuntimeWiring.newRuntimeWiring();
      for (String scalarName : customScalars) {
        runtimeWiringBuilder.scalar(createFakeScalar(scalarName));
      }

      RuntimeWiring runtimeWiring = runtimeWiringBuilder.build();
      SchemaGenerator schemaGenerator = new SchemaGenerator();
      GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);

      return GraphQL.newGraphQL(graphQLSchema).build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GraphQL client", e);
    }
  }

  private boolean isBuiltInScalar(String name) {
    return name.equals("String") || name.equals("Int") || name.equals("Float") ||
        name.equals("Boolean") || name.equals("ID");
  }

  @SuppressWarnings("deprecation")
  private GraphQLScalarType createFakeScalar(String name) {
    return GraphQLScalarType.newScalar()
        .name(name)
        .description("Fake scalar implementation for analysis purposes")
        .coercing(new Coercing<Object, Object>() {
          @Override
          public Object serialize(Object dataFetcherResult) {
            return dataFetcherResult;
          }

          @Override
          public Object parseValue(Object input) {
            return input;
          }

          @Override
          public Object parseLiteral(Object input) {
            return input;
          }
        })
        .build();
  }
}
