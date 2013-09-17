/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.function.Function1;

import com.mongodb.*;

import java.util.*;

/** Enumerator that reads from a MongoDB collection. */
class MongoEnumerator implements Enumerator<Object> {
  private final Iterator<DBObject> cursor;
  private final Function1<DBObject, Object> getter;
  private Object current;

  /** Creates a MongoEnumerator.
   *
   * @param cursor Mongo iterator (usually a {@link com.mongodb.DBCursor})
   * @param getter Converts an object into a list of fields
   */
  public MongoEnumerator(Iterator<DBObject> cursor,
      Function1<DBObject, Object> getter) {
    this.cursor = cursor;
    this.getter = getter;
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    try {
      if (cursor.hasNext()) {
        DBObject map = cursor.next();
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
    if (cursor instanceof DBCursor) {
      ((DBCursor) cursor).close();
    }
    // AggregationOutput implements Iterator but not DBCursor. There is no
    // available close() method -- apparently there is no open resource.
  }

  static Function1<DBObject, Map> mapGetter() {
    return new Function1<DBObject, Map>() {
      public Map apply(DBObject a0) {
        return (Map) a0;
      }
    };
  }

  static Function1<DBObject, Object> singletonGetter(
      final String field) {
    return new Function1<DBObject, Object>() {
      public Object apply(DBObject a0) {
        return a0.get(field);
      }
    };
  }

  /**
   * @param fields List of fields to project; or null to return map
   */
  static Function1<DBObject, Object[]> listGetter(
      final List<String> fields) {
    return new Function1<DBObject, Object[]>() {
      public Object[] apply(DBObject a0) {
        Object[] objects = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          String s = fields.get(i);
          objects[i] = a0.get(s);
        }
        return objects;
      }
    };
  }

  static Function1<DBObject, Object> getter(List<String> fields) {
    //noinspection unchecked
    return fields == null
        ? (Function1) mapGetter()
        : fields.size() == 1
            ? singletonGetter(fields.get(0))
            : listGetter(fields);
  }
}

// End MongoEnumerator.java
