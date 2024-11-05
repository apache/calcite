package org.apache.calcite.adapter.graphql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.language.*;
import graphql.parser.Parser;
import graphql.schema.*;
import okhttp3.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class GraphQLExecutor {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLExecutor.class);
  private final String query;
  private final String endpoint;
  private final GraphQLCalciteSchema schema;
  private final Map<String, GraphQLOutputType> fieldTypes;

  public GraphQLExecutor(String query, String endpoint, GraphQLCalciteSchema schema) {
    this.query = query;
    this.endpoint = endpoint;
    this.schema = schema;
    this.fieldTypes = extractFieldTypes();
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

  public @Nullable ExecutionResult executeQuery() {
    OkHttpClient client = new OkHttpClient();
    ObjectMapper objectMapper = configureObjectMapper();
    LOGGER.debug("Preparing to execute the GraphQL query");

    try {
      ObjectNode jsonBody = objectMapper.createObjectNode();
      jsonBody.put("query", this.query);
      LOGGER.debug("JSON body created with query: {}", this.query);

      RequestBody body = RequestBody.create(
          objectMapper.writeValueAsString(jsonBody),
          MediaType.parse("application/json")
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

        assert response.body() != null;
        String responseBody = response.body().string();
        JsonNode jsonResponse = objectMapper.readTree(responseBody);
        JsonNode dataNode = jsonResponse.get("data");
        List<Map<String, ?>> dataList = null;

        if (dataNode != null && dataNode.isObject()) {
          Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
          if (fields.hasNext()) {
            JsonNode actualDataNode = fields.next().getValue();
            if (actualDataNode.isArray()) {
              dataList = objectMapper.convertValue(actualDataNode,
                  new TypeReference<List<Map<String, ?>>>() {});
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

  private ObjectMapper configureObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();

    module.addDeserializer(Object.class, new StdDeserializer<Object>(Object.class) {
      @Override
      public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String fieldName = p.getCurrentName();
        GraphQLOutputType type = fieldTypes.get(fieldName);

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

        // Default deserialization for non-date fields
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
          return objectMapper.treeToValue(node, Object.class);
        }
      }
    });

    objectMapper.registerModule(module);
    return objectMapper;
  }
}
