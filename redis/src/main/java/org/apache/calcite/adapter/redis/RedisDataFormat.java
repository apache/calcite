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
package org.apache.calcite.adapter.redis;

/**
 * Define the data processing type of redis.
 */
public enum RedisDataFormat {
  /**
   * Treat redis key and value as a string format.
   */
  RAW("raw"),

  /**
   * Treat redis key and value as a csv format And parse the string
   * to get the corresponding field content,The default separator is colon.
   */
  CSV("csv"),

  /**
   * Treat redis key and value as a json format And parse the json string
   * to get the corresponding field content.
   */
  JSON("json");

  private String typeName;

  RedisDataFormat(String typeName) {
    this.typeName = typeName;
  }

  public static RedisDataFormat fromTypeName(String typeName) {
    for (RedisDataFormat type : RedisDataFormat.values()) {
      if (type.getTypeName().equals(typeName)) {
        return type;
      }
    }
    return null;
  }

  public String getTypeName() {
    return this.typeName;
  }
}
