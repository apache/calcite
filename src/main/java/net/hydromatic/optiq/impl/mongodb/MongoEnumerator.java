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

import com.mongodb.*;

/** Enumerator that reads from a MongoDB collection. */
class MongoEnumerator implements Enumerator<Object> {
  private final DBCursor cursor;
  private DBObject current;

  /** Creates a MongoEnumerator.
   *
   * @param mongoDb Connection to a Mongo database
   * @param collectionName Collection name
   */
  public MongoEnumerator(DB mongoDb, String collectionName) {
    this.cursor = mongoDb.getCollection(collectionName).find();
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    try {
      if (cursor.hasNext()) {
        current = cursor.next();
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
}

// End MongoEnumerator.java
