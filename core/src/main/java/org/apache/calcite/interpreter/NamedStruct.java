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
package org.apache.calcite.interpreter;

import java.util.ArrayList;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.calcite.avatica.util.StructImpl;

import java.sql.Struct;
import java.sql.SQLException;

public class NamedStruct extends StructImpl {
  
  final SortedMap<String, Object> fields;
  
  public NamedStruct(ArrayList<String> keys, ArrayList<Object> values) {
    super(values);

    assert(keys.size() == values.size());

    final SortedMap<String, Object> fields = new TreeMap<>();

    int i = 0;
    for (String key : keys) {
      Object value = values.get(i);
      fields.put(key, value);
      i += 1;
    }

    this.fields = fields;
  }

  public Object field(String key) {
    return fields.get(key);
  }
}