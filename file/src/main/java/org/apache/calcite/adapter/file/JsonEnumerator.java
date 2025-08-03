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
package org.apache.calcite.adapter.file;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.trace.CalciteLogger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Enumerator that reads from a Object List.
 */
public class JsonEnumerator implements Enumerator<@Nullable Object[]> {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(JsonEnumerator.class));

  private final Enumerator<@Nullable Object[]> enumerator;

  public JsonEnumerator(List<? extends @Nullable Object> list) {
    List<@Nullable Object[]> objs = new ArrayList<>();
    for (Object obj : list) {
      if (obj instanceof Collection) {
        //noinspection unchecked
        List<Object> tmp = (List<Object>) obj;
        objs.add(tmp.toArray());
      } else if (obj instanceof Map) {
        objs.add(((LinkedHashMap) obj).values().toArray());
      } else {
        objs.add(new Object[]{obj});
      }
    }
    enumerator = Linq4j.enumerator(objs);
  }

  public static void replaceArrayLists(Map<String, Object> map) throws IllegalAccessException {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (value instanceof ArrayList) {
        ArrayList listValue = (ArrayList) value;
        ComparableArrayList comparableList = new ComparableArrayList();
        comparableList.addAll(listValue);
        map.put(key, comparableList);
      } else if (value instanceof LinkedHashMap) {
        LinkedHashMap listValue = (LinkedHashMap) value;
        ComparableLinkedHashMap comparableList = new ComparableLinkedHashMap();
        comparableList.putAll(listValue);
        map.put(key, comparableList);
      } else if (value instanceof Map) {
        replaceArrayLists((Map<String, Object>) value);
      }
    }
  }

  static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source) {
    return deduceRowType(typeFactory, source, (Map<String, Object>) null);
  }

  static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
      Map<String, Object> options) {
    Source sourceSansGz = source.trim(".gz");
    Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    Source sourceSansYaml = sourceSansGz.trimOrNull(".yaml");
    if (sourceSansYaml == null) {
      sourceSansYaml = sourceSansGz.trimOrNull(".yml");
    }
    if (sourceSansYaml == null) {
      sourceSansYaml = sourceSansGz.trimOrNull(".hml");
    }
    if (sourceSansJson != null) {
      return deduceRowType(typeFactory, source, "json", options);
    } else if (sourceSansYaml != null) {
      return deduceRowType(typeFactory, source, "yaml", options);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + source);
    }
  }

  static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType) {
    return deduceRowType(typeFactory, source, dataType, null);
  }

  /**
   * Deduces the names and types of a table's columns by reading the first line
   * of a JSON file.
   */
  static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType, Map<String, Object> options) {
    final ObjectMapper objectMapper = new ObjectMapper();
    ObjectMapper jsonMapper = new ObjectMapper();
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
//    yamlMapper.findAndRegisterModules();
    List<Object> list;
    LinkedHashMap<String, Object> jsonFieldMap = new LinkedHashMap<>(1);
    Object jsonObj = null;
    try {
      jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

      ObjectMapper selectedMapper;
      if ("json".equalsIgnoreCase(dataType)) {
        selectedMapper = jsonMapper;
      } else if ("yaml".equalsIgnoreCase(dataType)) {
        selectedMapper = yamlMapper;
      } else {
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }

      if ("file".equals(source.protocol()) && source.file().exists()) {
        // Acquire read lock on source file
        SourceFileLockManager.LockHandle lockHandle = null;
        try {
          lockHandle = SourceFileLockManager.acquireReadLock(source.file());
          LOGGER.debug("Acquired read lock on JSON file: " + source.path());
          //noinspection unchecked
          jsonObj = selectedMapper.readValue(source.reader(), Object.class);
        } catch (IOException lockException) {
          LOGGER.warn("Could not acquire lock on file: "
              + source.path()
              + " - proceeding without lock");
          // Proceed without lock
          //noinspection unchecked
          jsonObj = selectedMapper.readValue(source.reader(), Object.class);
        } finally {
          if (lockHandle != null) {
            lockHandle.close();
            LOGGER.debug("Released read lock on JSON file");
          }
        }
      } else if (Arrays.asList("http", "https", "ftp").contains(source.protocol())) {
        //noinspection unchecked
        jsonObj = selectedMapper.readValue(source.url(), Object.class);
      } else {
        jsonObj = selectedMapper.readValue(source.reader(), Object.class);
      }

      if (jsonObj instanceof ArrayList) {
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) jsonObj;
        for (Map<String, Object> item : l) {
          replaceArrayLists(item);
        }
      }

    } catch (MismatchedInputException e) {
      if (!e.getMessage().contains("No content")) {
        throw new RuntimeException("Couldn't read " + source, e);
      }
    } catch (Exception e) {
      throw new RuntimeException("Couldn't read " + source, e);
    }

    if (jsonObj == null) {
      list = new ArrayList<>();
      jsonFieldMap.put("EmptyFileHasNoColumns", Boolean.TRUE);
    } else if (jsonObj instanceof Collection) {
      //noinspection unchecked
      list = (List<Object>) jsonObj;
      //noinspection unchecked
      jsonFieldMap = (LinkedHashMap) list.get(0);
      // Apply flattening if requested
      if (options != null && Boolean.TRUE.equals(options.get("flatten"))) {
        String flattenSeparator = options.containsKey("flattenSeparator") 
            ? (String) options.get("flattenSeparator") : ".";
        JsonFlattener flattener = new JsonFlattener(",", 3, "", flattenSeparator);
        jsonFieldMap = new LinkedHashMap<>(flattener.flatten(jsonFieldMap));
        // Flatten all rows in the list
        for (int i = 0; i < list.size(); i++) {
          if (list.get(i) instanceof Map) {
            list.set(i, flattener.flatten((Map<String, Object>) list.get(i)));
          }
        }
      }
    } else if (jsonObj instanceof Map) {
      //noinspection unchecked
      jsonFieldMap = (LinkedHashMap) jsonObj;
      // Apply flattening if requested
      if (options != null && Boolean.TRUE.equals(options.get("flatten"))) {
        String flattenSeparator = options.containsKey("flattenSeparator") 
            ? (String) options.get("flattenSeparator") : ".";
        JsonFlattener flattener = new JsonFlattener(",", 3, "", flattenSeparator);
        jsonFieldMap = new LinkedHashMap<>(flattener.flatten(jsonFieldMap));
      }
      //noinspection unchecked
//      list = new ArrayList(((LinkedHashMap) jsonObj).values());
      list = new ArrayList();
      ((List) list).add(jsonFieldMap);
    } else {
      jsonFieldMap.put("line", jsonObj);
      list = new ArrayList<>();
      list.add(0, jsonObj);
    }

    final List<RelDataType> types = new ArrayList<RelDataType>(jsonFieldMap.size());
    final List<String> names = new ArrayList<String>(jsonFieldMap.size());

    for (Object key : jsonFieldMap.keySet()) {
      final RelDataType type = typeFactory.createJavaType(jsonFieldMap.get(key).getClass());
      names.add(key.toString());
      types.add(type);
    }

    RelDataType relDataType = typeFactory.createStructType(Pair.zip(names, types));
    return new JsonDataConverter(relDataType, list);
  }

  @Override public Object[] current() {
    return enumerator.current();
  }

  @Override public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }

  /**
   * Json data and relDataType Converter.
   */
  static class JsonDataConverter {
    private final RelDataType relDataType;
    private final List<Object> dataList;

    private JsonDataConverter(RelDataType relDataType, List<Object> dataList) {
      this.relDataType = relDataType;
      this.dataList = dataList;
    }

    RelDataType getRelDataType() {
      return relDataType;
    }

    List<Object> getDataList() {
      return dataList;
    }
  }
}
