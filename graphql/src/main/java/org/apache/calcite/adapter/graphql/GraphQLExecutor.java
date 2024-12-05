package org.apache.calcite.adapter.graphql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  private final Map<String, GraphQLOutputType> fieldTypes;
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
    this.fieldTypes = extractFieldTypes();

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

    this.objectMapper = configureObjectMapper();
    this.client = new OkHttpClient();
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

  private boolean isDateType(GraphQLNamedOutputType type) {
    return GraphQLTypeUtil.unwrapAll(type) instanceof GraphQLScalarType && "Date".equals(type.getName());
  }

  private boolean isTimestampType(GraphQLNamedOutputType type) {
    return GraphQLTypeUtil.unwrapAll(type) instanceof GraphQLScalarType && "Timestamp".equals(type.getName());
  }

  private boolean isTimestamptzType(GraphQLNamedOutputType type) {
    return GraphQLTypeUtil.unwrapAll(type) instanceof GraphQLScalarType && "Timestamptz".equals(type.getName());
  }

  private ObjectMapper configureObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();

    module.addDeserializer(Object.class, new StdDeserializer<Object>(Object.class) {
      @Override
      public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String fieldName = p.getCurrentName();
        GraphQLOutputType type = fieldTypes.get(fieldName);

        try {
          if (type != null && isDateType((GraphQLNamedOutputType) type)) {
            String dateStr = p.getValueAsString();
            if (dateStr != null && !dateStr.isEmpty()) {
              try {
                return Date.valueOf(dateStr);
              } catch (IllegalArgumentException e) {
                LOGGER.warn("Failed to parse date value: {}", dateStr);
              }
            }
          }

          if (type != null && isTimestampType((GraphQLNamedOutputType) type)) {
            String dateStr = p.getValueAsString();
            if (dateStr != null && !dateStr.isEmpty()) {
              try {
                return Timestamp.valueOf(dateStr);
              } catch (IllegalArgumentException e) {
                LOGGER.warn("Failed to parse timestamp value: {}", dateStr);
              }
            }
          }
          if (type != null && isTimestamptzType((GraphQLNamedOutputType) type)) {
            String dateStr = p.getValueAsString();
            if (dateStr != null && !dateStr.isEmpty()) {
              try {
                // Keep as OffsetDateTime to preserve precision
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(dateStr);
                return Timestamp.from(offsetDateTime.toInstant());
              } catch (DateTimeParseException e) {
                LOGGER.warn("Failed to parse timestamptz value: {}", dateStr, e);
              }
            }
          }
        } catch(Exception ignored) {}

        JsonNode node = p.getCodec().readTree(p);
        if (node.isTextual()) {
          return node.asText();
        } else if (node.isNumber()) {
          return node.numberValue();
        } else if (node.isBoolean()) {
          return node.booleanValue();
        } else if (node.isNull()) {
          return null;
        } else {
          return mapper.treeToValue(node, Object.class);
        }
      }
    });

    mapper.registerModule(module);
    return mapper;
  }

  /**
   * Executes the GraphQL query, using cached results if available.
   *
   * @return The execution result, or null if the execution fails
   */
  public @Nullable ExecutionResult executeQuery() {
    assert cacheModule != null;
    String cacheKey = cacheModule.generateKey(query, schema.role);

    // Try to get from cache first
    assert cacheKey != null;
    @Nullable ExecutionResult cachedResult = cacheModule.get(cacheKey);
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
      cacheModule.put(cacheKey, queryResult);
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
        JsonNode jsonResponse = objectMapper.readTree(responseBodyString);
        JsonNode dataNode = jsonResponse.get("data");
        List<Map<String, ?>> dataList = null;

        if (dataNode != null && dataNode.isObject()) {
          Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
          if (fields.hasNext()) {
            JsonNode actualDataNode = fields.next().getValue();
            if (actualDataNode.isArray()) {
              dataList = objectMapper.convertValue(actualDataNode,
                  new TypeReference<List<Map<String, ?>>>() {
                  });
            }
          }
        }

        LOGGER.debug("Query executed successfully");
        return new ExecutionResultImpl(dataList, null);
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
        cacheModule.close();
        cacheModule = null;
        LOGGER.info("Cache module shut down");
      }
    }
  }
}
