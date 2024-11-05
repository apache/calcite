package org.apache.calcite.adapter.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.type.TypeReference;
import graphql.introspection.IntrospectionResultToSchema;
import graphql.language.Document;
import graphql.schema.idl.SchemaPrinter;
import okhttp3.*;
import java.io.IOException;
import java.util.Map;

/**
 * This class represents a utility for retrieving the GraphQL Schema Definition Language (SDL) from a given GraphQL endpoint.
 * The class contains a method retrieveSDL that sends an introspection query to the specified endpoint and converts the introspection result into SDL format.
 * The retrieveSDL method requires the endpoint URL as a parameter and returns the SDL representation of the GraphQL schema.
 *
 * It uses the OkHttpClient library to send HTTP requests, ObjectMapper for JSON serialization and deserialization,
 * and utilizes a predefined introspection query to retrieve schema information from the GraphQL server.
 *
 * Note: This class is designed for internal use to fetch the SDL of a GraphQL schema from a server.
 */
public class GraphQLSDLRetriever {
  private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
  private static final String INTROSPECTION_QUERY = "query IntrospectionQuery {\n" +
      "  __schema {\n" +
      "    queryType { name }\n" +
      "    mutationType { name }\n" +
      "    subscriptionType { name }\n" +
      "    types {\n" +
      "      ...FullType\n" +
      "    }\n" +
      "    directives {\n" +
      "      name\n" +
      "      description\n" +
      "      locations\n" +
      "      args {\n" +
      "        ...InputValue\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}\n" +
      "\n" +
      "fragment FullType on __Type {\n" +
      "  kind\n" +
      "  name\n" +
      "  description\n" +
      "  fields(includeDeprecated: true) {\n" +
      "    name\n" +
      "    description\n" +
      "    args {\n" +
      "      ...InputValue\n" +
      "    }\n" +
      "    type {\n" +
      "      ...TypeRef\n" +
      "    }\n" +
      "    isDeprecated\n" +
      "    deprecationReason\n" +
      "  }\n" +
      "  inputFields {\n" +
      "    ...InputValue\n" +
      "  }\n" +
      "  interfaces {\n" +
      "    ...TypeRef\n" +
      "  }\n" +
      "  enumValues(includeDeprecated: true) {\n" +
      "    name\n" +
      "    description\n" +
      "    isDeprecated\n" +
      "    deprecationReason\n" +
      "  }\n" +
      "  possibleTypes {\n" +
      "    ...TypeRef\n" +
      "  }\n" +
      "}\n" +
      "\n" +
      "fragment InputValue on __InputValue {\n" +
      "  name\n" +
      "  description\n" +
      "  type { ...TypeRef }\n" +
      "  defaultValue\n" +
      "}\n" +
      "\n" +
      "fragment TypeRef on __Type {\n" +
      "  kind\n" +
      "  name\n" +
      "  ofType {\n" +
      "    kind\n" +
      "    name\n" +
      "    ofType {\n" +
      "      kind\n" +
      "      name\n" +
      "      ofType {\n" +
      "        kind\n" +
      "        name\n" +
      "        ofType {\n" +
      "          kind\n" +
      "          name\n" +
      "          ofType {\n" +
      "            kind\n" +
      "            name\n" +
      "            ofType {\n" +
      "              kind\n" +
      "              name\n" +
      "              ofType {\n" +
      "                kind\n" +
      "                name\n" +
      "              }\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}\n";

  public static String retrieveSDL(String graphqlEndpoint) throws IOException {
    // Create OkHttpClient
    OkHttpClient client = new OkHttpClient();

    // Create ObjectMapper
    ObjectMapper objectMapper = new ObjectMapper();

    // Create the request body
    JsonNode jsonBody = objectMapper.createObjectNode()
        .put("query", INTROSPECTION_QUERY);
    RequestBody body = RequestBody.create(objectMapper.writeValueAsString(jsonBody), JSON);

    // Build the request
    Request request = new Request.Builder()
        .url(graphqlEndpoint)
        .post(body)
        .build();

    // Execute the request
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

      // Parse the response
      assert response.body() != null;
      String responseBody = response.body().string();
      JsonNode jsonResponse = objectMapper.readTree(responseBody);
      Map<String, Object> resultAsMap = objectMapper.convertValue(jsonResponse.get("data"), new TypeReference<Map<String, Object>>() {});

      // Convert the introspection result to a Document
      Document schemaDocument = new IntrospectionResultToSchema().createSchemaDefinition(resultAsMap);

      // Print the schema as SDL
      return new SchemaPrinter().print(schemaDocument);
    }
  }
}
