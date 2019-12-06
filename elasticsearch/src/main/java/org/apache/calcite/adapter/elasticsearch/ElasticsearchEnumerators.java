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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Util functions which convert
 * {@link ElasticsearchJson.SearchHit}
 * into calcite specific return type (map, object[], list etc.)
 */
class ElasticsearchEnumerators {

  private ElasticsearchEnumerators() {}

  private static Function1<ElasticsearchJson.SearchHit, Map> mapGetter() {
    return ElasticsearchJson.SearchHit::sourceOrFields;
  }

  private static Function1<ElasticsearchJson.SearchHit, Object> singletonGetter(
      final String fieldName,
      final Class fieldClass,
      final Map<String, String> mapping) {
    return hit -> {
      final String key;
      if (hit.sourceOrFields().containsKey(fieldName)) {
        key = fieldName;
      } else {
        key = mapping.getOrDefault(fieldName, fieldName);
      }

      final Object value;
      if (ElasticsearchConstants.ID.equals(key)
          || ElasticsearchConstants.ID.equals(mapping.getOrDefault(fieldName, fieldName))) {
        // is the original projection on _id field?
        value = hit.id();
      } else {
        value = hit.valueOrNull(key);
      }
      return convert(value, fieldClass);
    };
  }

  /**
   * Function that extracts a given set of fields from elastic search result
   * objects.
   *
   * @param fields List of fields to project
   *
   * @return function that converts the search result into a generic array
   */
  private static Function1<ElasticsearchJson.SearchHit, Object[]> listGetter(
      final List<Map.Entry<String, Class>> fields, Map<String, String> mapping) {
    return hit -> {
      Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        final Map.Entry<String, Class> field = fields.get(i);
        final String key;
        if (hit.sourceOrFields().containsKey(field.getKey())) {
          key = field.getKey();
        } else {
          key = mapping.getOrDefault(field.getKey(), field.getKey());
        }

        final Object value;
        if (ElasticsearchConstants.ID.equals(key)
            || ElasticsearchConstants.ID.equals(mapping.get(field.getKey()))
            || ElasticsearchConstants.ID.equals(field.getKey())) {
          // is the original projection on _id field?
          value = hit.id();
        } else {
          value = hit.valueOrNull(key);
        }

        final Class type = field.getValue();
        objects[i] = convert(value, type);
      }
      return objects;
    };
  }

  static Function1<ElasticsearchJson.SearchHit, Object> getter(
      List<Map.Entry<String, Class>> fields, Map<String, String> mapping) {
    Objects.requireNonNull(fields, "fields");
    //noinspection unchecked
    final Function1 getter;
    if (fields.size() == 1) {
      // select foo from table
      // select * from table
      getter = singletonGetter(fields.get(0).getKey(), fields.get(0).getValue(), mapping);
    } else {
      // select a, b, c from table
      getter = listGetter(fields, mapping);
    }

    return getter;
  }

  private static Object convert(Object o, Class clazz) {
    if (o == null) {
      return null;
    }
    Primitive primitive = Primitive.of(clazz);
    if (primitive != null) {
      clazz = primitive.boxClass;
    } else {
      primitive = Primitive.ofBox(clazz);
    }
    if (clazz.isInstance(o)) {
      return o;
    }
    if (o instanceof Date && primitive != null) {
      o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
    }
    if (o instanceof Number && primitive != null) {
      return primitive.number((Number) o);
    }
    return o;
  }
}

// End ElasticsearchEnumerators.java
