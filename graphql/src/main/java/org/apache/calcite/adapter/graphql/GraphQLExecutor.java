package org.apache.calcite.adapter.graphql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import graphql.schema.*;
import okhttp3.*;

/**
 * Executes GraphQL queries with caching support. The cache can be configured through
 * environment variables:
 * - GRAPHQL_CACHE_TYPE: "memory" or "redis" (if not set, caching is disabled)
 * - GRAPHQL_CACHE_TTL: Time to live in seconds (defaults to 300)
 * - REDIS_URL: Redis connection URL (required if cache type is "redis")
 */
public class GraphQLExecutor implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLExecutor.class);
  private final String query;
  private final String endpoint;
  private final GraphQLCalciteSchema schema;
  private static volatile @Nullable GraphQLCacheModule cacheModule = null;
  private final ObjectMapper objectMapper;
  private final OkHttpClient client;
  private static final Object CACHE_LOCK = new Object();

  /**
   * Creates a new GraphQLExecutor instance.
   *
   * @param query    The GraphQL query to execute
   * @param endpoint The GraphQL endpoint URL
   * @param schema   The schema configuration containing role and authentication details
   */
  public GraphQLExecutor(String query, String endpoint, GraphQLCalciteSchema schema) {
    this.query = query;
    this.endpoint = endpoint;
    this.schema = schema;
    Map<String, GraphQLOutputType> fieldTypes = extractFieldTypes();

    // Initialize singleton cache if not already created
    if (cacheModule == null) {
      synchronized (CACHE_LOCK) {
        if (cacheModule == null) {
          cacheModule = new GraphQLCacheModule.Builder()
              .withOperandConfig(schema.getCacheConfig())  // Operand config takes precedence
              .withEnvironmentConfig()                     // Environment variables as fallback
              .build();
          LOGGER.info("Initialized singleton cache module");
        }
      }
    }

    // Initialize ObjectMapper with JsonFlatteningDeserializer
    this.objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Object.class, new JsonFlatteningDeserializer(fieldTypes));
    this.objectMapper.registerModule(module);

    this.client = new OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build();
  }

  private Map<String, GraphQLOutputType> extractFieldTypes() {
    Map<String, GraphQLOutputType> types = new HashMap<>();
    try {
      Document document = new Parser().parseDocument(query);
      document.getDefinitions().forEach(definition -> {
        if (definition instanceof OperationDefinition) {
          OperationDefinition opDef = (OperationDefinition) definition;
          opDef.getSelectionSet().getSelections().forEach(selection -> {
            if (selection instanceof Field) {
              Field queryField = (Field) selection;
              GraphQLObjectType queryType = schema.graphQL.getGraphQLSchema().getQueryType();
              GraphQLFieldDefinition fieldDef = queryType.getFieldDefinition(queryField.getName());

              if (fieldDef != null) {
                GraphQLType unwrappedType = GraphQLTypeUtil.unwrapAll(fieldDef.getType());
                if (unwrappedType instanceof GraphQLObjectType) {
                  GraphQLObjectType resultType = (GraphQLObjectType) unwrappedType;
                  if (queryField.getSelectionSet() != null) {
                    queryField.getSelectionSet().getSelections().forEach(subSelection -> {
                      if (subSelection instanceof Field) {
                        Field subField = (Field) subSelection;
                        GraphQLFieldDefinition subFieldDef = resultType.getFieldDefinition(subField.getName());
                        if (subFieldDef != null) {
                          types.put(subField.getName(), subFieldDef.getType());
                        }
                      }
                    });
                  }
                }
              }
            }
          });
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to parse GraphQL query", e);
    }
    return types;
  }

  /**
   * Executes the GraphQL query, using cached results if available.
   *
   * @return The execution result, or null if the execution fails
   */
  public @Nullable ExecutionResult executeQuery() {
    @Nullable String cacheKey = Objects.requireNonNull(cacheModule).generateKey(query, schema.role);

    // Try to get from cache first
    assert cacheKey != null;
    @Nullable ExecutionResult cachedResult = Objects.requireNonNull(cacheModule).get(cacheKey);
    if (cachedResult != null) {
      LOGGER.debug("Returning cached result for query");
      return cachedResult;
    }

    // Cache miss - execute actual query
    LOGGER.debug("Cache miss - executing actual query");
    ExecutionResult queryResult = executeActualQuery();

    // Store successful results in cache
    if (queryResult != null) {
      LOGGER.debug("Storing query result in cache");
      Objects.requireNonNull(cacheModule).put(cacheKey, queryResult);
    }

    return queryResult;
  }

  private @Nullable ExecutionResult executeActualQuery() {
    LOGGER.debug("Preparing to execute the GraphQL query");

    try {
      ObjectNode jsonBody = objectMapper.createObjectNode();
      jsonBody.put("query", this.query);
      LOGGER.debug("JSON body created with query: {}", this.query);

      @SuppressWarnings("deprecation")
      RequestBody body = RequestBody.create(
          MediaType.parse("application/json"),
          objectMapper.writeValueAsString(jsonBody)
      );

      Request.Builder requestBuilder = new Request.Builder()
          .url(this.endpoint)
          .post(body);

      if (schema.role != null && !schema.role.isEmpty()) {
        requestBuilder.header("X-Hasura-Role", schema.role);
      }
      if (schema.user != null && !schema.user.isEmpty()) {
        requestBuilder.header("X-Hasura-User", schema.user);
      }
      if (schema.auth != null && !schema.auth.isEmpty()) {
        requestBuilder.header("Authorization", schema.auth);
      }

      Request request = requestBuilder.build();
      LOGGER.debug("Request built and ready to be sent");

      try (Response response = client.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          throw new IOException("Unexpected code " + response);
        }

        ResponseBody responseBody = response.body();
        if (responseBody == null) {
          throw new IOException("Empty response body");
        }

        String responseBodyString = responseBody.string();
        LOGGER.debug("Received response: {}", responseBodyString);

        // Deserialize with our flattening deserializer
        @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) objectMapper.readValue(responseBodyString, Object.class);

        // After flattening, there should be one key "data.<datasetName>" with the list value
        for (Map.Entry<String, Object> entry : result.entrySet()) {
          if (entry.getKey().startsWith("data.")) {
            Object value = entry.getValue();
            if (value instanceof List) {
              @SuppressWarnings("unchecked")
              List<Map<String, ?>> dataList = (List<Map<String, ?>>) value;
              return new ExecutionResultImpl(dataList, null);
            }
          }
        }

        LOGGER.debug("Query executed successfully");
        return new ExecutionResultImpl(null, null);
      }
    } catch (IOException e) {
      LOGGER.error("An error occurred while executing the query", e);
      return null;
    }
  }

  @Override
  public void close() {
    // Close HTTP client resources
    client.dispatcher().executorService().shutdown();
    client.connectionPool().evictAll();
    // Cache module is now static and shared, so don't close it here
  }

  /**
   * Shuts down the cache module and cleans up resources.
   * This should be called when the application is shutting down.
   */
  public static void shutdownCache() {
    synchronized (CACHE_LOCK) {
      if (cacheModule != null) {
        Objects.requireNonNull(cacheModule).close();
        cacheModule = null;
        LOGGER.info("Cache module shut down");
      }
    }
  }
}
