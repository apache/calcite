package org.apache.calcite.adapter.graphql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import okhttp3.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents a class responsible for executing GraphQL queries by sending an HTTP POST request to a specified endpoint.
 * Upon successful execution, it processes the response to extract data from the JSON structure.
 */
public class GraphQLExecutor {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLExecutor.class);
  private final String query;
  private final String endpoint;
  private final GraphQLCalciteSchema schema;

  public GraphQLExecutor(String query, String endpoint, GraphQLCalciteSchema schema) {
    this.query = query;
    this.endpoint = endpoint;
    this.schema = schema;
  }

  /**
   * Executes a GraphQL query by sending an HTTP POST request to the specified endpoint.
   * It processes the response and extracts the data from the JSON structure.
   * The retrieved data is converted to a list of maps and encapsulated in an ExecutionResult object.
   *
   * @return An ExecutionResult object containing the extracted data.
   */
  public @Nullable ExecutionResult executeQuery() {
    OkHttpClient client = new OkHttpClient();
    ObjectMapper objectMapper = new ObjectMapper();

    LOGGER.debug("Preparing to execute the GraphQL query");

    try {
      // Create JSON body
      ObjectNode jsonBody = objectMapper.createObjectNode();
      jsonBody.put("query", query);

      LOGGER.debug("JSON body created with query: {}", query);

      RequestBody body = RequestBody.create(
          objectMapper.writeValueAsString(jsonBody),
          MediaType.parse("application/json")
      );

      Request.Builder requestBuilder = new Request.Builder().url(endpoint).post(body);

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
        if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

        assert response.body() != null;
        String responseBody = response.body().string();

        JsonNode jsonResponse = objectMapper.readTree(responseBody);

        JsonNode dataNode = jsonResponse.get("data");
        List<Map<String, ?>> dataList = null;
        if (dataNode != null && dataNode.isObject()) {
          // Get the first (and presumably only) field in the data object
          Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
          if (fields.hasNext()) {
            JsonNode actualDataNode = fields.next().getValue();
            if (actualDataNode.isArray()) {
              dataList = objectMapper.convertValue(actualDataNode, new TypeReference<List<Map<String, ?>>>() {});
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
}
