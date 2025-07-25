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
import org.apache.calcite.adapter.graphql.JsonFlatteningDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonFlatteningDeserializerTest {
  @Test void testDeserialize() throws IOException {
    String json = "{\n"
  +
        "  \"albumId\": 1,\n"
  +
        "  \"title\": \"For Those About To Rock We Salute You\",\n"
  +
        "  \"artist\": {\n"
  +
        "    \"name\": \"AC/DC\"\n"
  +
        "  }\n"
  +
        "}";

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Object.class, new JsonFlatteningDeserializer(new HashMap<>()));
    mapper.registerModule(module);

    Map<String, Object> expected = new HashMap<>();
    expected.put("albumId", 1);
    expected.put("title", "For Those About To Rock We Salute You");
    expected.put("artist.name", "AC/DC");

    Object actual = mapper.readValue(json, Object.class);
    assertEquals(expected, actual);
  }

  @Test void testDeserializeNestedObjects() throws IOException {
    String jsonArray = "["
        + "{"
        + "  \"a\": { \"e\": 1 }, \"b\": \"test\""
        + "},"
        + "{"
        + "  \"b\": {"
        + "    \"c\": 1,"
        + "    \"d\": 2"
        + "  }"
        + "}"
        + "]";

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Object.class, new JsonFlatteningDeserializer(new HashMap<>()));
    mapper.registerModule(module);

    Map<String, Object> expected1 = new HashMap<>();
    expected1.put("a.e", 1);
    expected1.put("b", "test");

    Map<String, Object> expected2 = new HashMap<>();
    expected2.put("b.c", 1);
    expected2.put("b.d", 2);

    List<Map<String, Object>> expected = new ArrayList<>();
    expected.add(expected1);
    expected.add(expected2);

    Object actual = mapper.readValue(jsonArray, Object.class);
    assertEquals(expected, actual);
  }

  @Test void testDeserializeWithArray() throws IOException {
    String json = "{\n"
  +
        "  \"albumId\": 1,\n"
  +
        "  \"tracks\": [\n"
  +
        "    {\"name\": \"Track 1\"},\n"
  +
        "    {\"name\": \"Track 2\"}\n"
  +
        "  ]\n"
  +
        "}";

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Object.class, new JsonFlatteningDeserializer(new HashMap<>()));
    mapper.registerModule(module);

    HashMap<String, String> track1 = new HashMap<>();
    track1.put("name", "Track 1");

    HashMap<String, String> track2 = new HashMap<>();
    track2.put("name", "Track 2");

    Map<String, Object> expected = new HashMap<>();
    ArrayList<Object> list = new ArrayList<>();
    list.add(track1);
    list.add(track2);
    expected.put("albumId", 1);
    expected.put("tracks", list);

    Object actual = mapper.readValue(json, Object.class);
    assertEquals(expected, actual);
  }
}
