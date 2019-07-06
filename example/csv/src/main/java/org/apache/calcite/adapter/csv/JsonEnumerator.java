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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Enumerator that reads from a JSON file.
 */
public class JsonEnumerator implements Enumerator<Object[]> {

  private Enumerator<Object[]> enumerator;

  public JsonEnumerator(List<Object> list) {
    List<Object[]> objs = new ArrayList<Object[]>();
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

  public Object[] current() {
    return enumerator.current();
  }

  public boolean moveNext() {
    return enumerator.moveNext();
  }

  public void reset() {
    enumerator.reset();
  }

  public void close() {
    enumerator.close();
  }

}

// End JsonEnumerator.java
