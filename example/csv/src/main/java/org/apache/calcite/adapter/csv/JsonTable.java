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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable {
  private final ObjectMapper objectMapper = new ObjectMapper();
  protected final List<Object> list;

  private LinkedHashMap<String, Object> jsonFieldMap = new LinkedHashMap<>(1);

  public JsonTable(Source source) {
    Object jsonObj = null;
    try {
      objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      if (source.file().exists() && source.file().length() > 0) {
        if ("file".equals(source.protocol())) {
          //noinspection unchecked
          jsonObj = objectMapper.readValue(source.file(), Object.class);
        } else {
          //noinspection unchecked
          jsonObj = objectMapper.readValue(source.url(), Object.class);
        }
      }
    } catch (MismatchedInputException e) {
      if (!e.getMessage().contains("No content")) {
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (jsonObj == null) {
      list = new ArrayList<>();
      jsonFieldMap.put("EmptyFileHasNoColumns", Boolean.TRUE);
    } else if (jsonObj instanceof Collection) {
      //noinspection unchecked
      list = (List<Object>) jsonObj;
      //noinspection unchecked
      jsonFieldMap = (LinkedHashMap) (list.get(0));
    } else if (jsonObj instanceof Map) {
      //noinspection unchecked
      jsonFieldMap = (LinkedHashMap) jsonObj;
      //noinspection unchecked
      list = new ArrayList(((LinkedHashMap) jsonObj).values());
    } else {
      jsonFieldMap.put("line", jsonObj);
      list = new ArrayList<>();
      list.add(0, jsonObj);
    }
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final List<RelDataType> types = new ArrayList<RelDataType>();
    final List<String> names = new ArrayList<String>();

    for (Object obj : jsonFieldMap.keySet()) {
      final RelDataType type = typeFactory.createJavaType(jsonFieldMap.get(obj).getClass());
      names.add(obj.toString());
      types.add(type);
    }

    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }
}

// End JsonTable.java
