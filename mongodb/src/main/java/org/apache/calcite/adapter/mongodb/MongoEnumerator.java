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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;

import com.mongodb.client.MongoCursor;

import org.bson.Document;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Enumerator that reads from a MongoDB collection. */
class MongoEnumerator implements Enumerator<Object> {
  private final Iterator<Document> cursor;
  private final Function1<Document, Object> getter;
  private Object current;

  /** Creates a MongoEnumerator.
   *
   * @param cursor Mongo iterator (usually a {@link com.mongodb.DBCursor})
   * @param getter Converts an object into a list of fields
   */
  MongoEnumerator(Iterator<Document> cursor,
      Function1<Document, Object> getter) {
    this.cursor = cursor;
    this.getter = getter;
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    try {
      if (cursor.hasNext()) {
        Document map = cursor.next();
        current = getter.apply(map);
        return true;
      } else {
        current = null;
        return false;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    if (cursor instanceof MongoCursor) {
      ((MongoCursor) cursor).close();
    }
    // AggregationOutput implements Iterator but not DBCursor. There is no
    // available close() method -- apparently there is no open resource.
  }

  static Function1<Document, Map> mapGetter() {
    return a0 -> (Map) a0;
  }

  static Function1<Document, Object> singletonGetter(final String fieldName,
      final Class fieldClass) {
    return a0 -> convert(a0.get(fieldName), fieldClass);
  }

  /**
   * @param fields List of fields to project; or null to return map
   */
  static Function1<Document, Object[]> listGetter(
      final List<Map.Entry<String, Class>> fields) {
    return a0 -> {
      Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        final Map.Entry<String, Class> field = fields.get(i);
        final String name = field.getKey();
        objects[i] = convert(a0.get(name), field.getValue());
      }
      return objects;
    };
  }

  static Function1<Document, Object> getter(
      List<Map.Entry<String, Class>> fields) {
    //noinspection unchecked
    return fields == null
        ? (Function1) mapGetter()
        : fields.size() == 1
            ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue())
            : (Function1) listGetter(fields);
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

// End MongoEnumerator.java
