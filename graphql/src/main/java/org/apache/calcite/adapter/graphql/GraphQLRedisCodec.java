package org.apache.calcite.adapter.graphql;

import graphql.ExecutionResult;
import io.lettuce.core.codec.RedisCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class GraphQLRedisCodec implements RedisCodec<String, ExecutionResult> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String decodeKey(ByteBuffer bytes) {
    return StandardCharsets.UTF_8.decode(bytes).toString();
  }

  @Override
  public ExecutionResult decodeValue(ByteBuffer bytes) {
    try {
      byte[] array = new byte[bytes.remaining()];
      bytes.get(array);
      return objectMapper.readValue(array, ExecutionResult.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode ExecutionResult", e);
    }
  }

  @Override
  public ByteBuffer encodeKey(String key) {
    return StandardCharsets.UTF_8.encode(key);
  }

  @Override
  public ByteBuffer encodeValue(ExecutionResult value) {
    try {
      byte[] jsonBytes = objectMapper.writeValueAsBytes(value);
      return ByteBuffer.wrap(jsonBytes);
    } catch (IOException e) {
      throw new RuntimeException("Failed to encode ExecutionResult", e);
    }
  }
}
