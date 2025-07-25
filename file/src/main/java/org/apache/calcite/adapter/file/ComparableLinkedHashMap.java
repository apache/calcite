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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;

public class ComparableLinkedHashMap<K, V> extends LinkedHashMap<K, V> implements Comparable<ComparableLinkedHashMap<K, V>> {
  @Override public int compareTo(ComparableLinkedHashMap<K, V> o) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Convert this map to a JSON string
      String thisJson = mapper.writeValueAsString(this);

      // Convert the other map to a JSON string
      String otherJson = mapper.writeValueAsString(o);

      // Return the comparison result
      return thisJson.compareTo(otherJson);

    } catch (IOException e) {
      // Handle the exception
      throw new RuntimeException("Failed to serialize LinkedHashMap to JSON", e);
    }
  }
}
