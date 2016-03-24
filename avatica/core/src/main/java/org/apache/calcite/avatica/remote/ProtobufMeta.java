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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Requests;

import java.util.List;

/**
 * An extension of {@link Meta} which allows for native processing of calls with the Protobuf
 * API objects instead of the POJOS (to avoid object translation). In the write-path, performing
 * this conversion tends to represent a signficant portion of execution time. The introduction
 * of this interface is to serve the purose of gradual migration to Meta implementations that
 * can naturally function over Protobuf objects instead of the POJOs.
 */
public interface ProtobufMeta extends Meta {

  /**
   * Executes a batch of commands on a prepared statement.
   *
   * @param h Statement handle
   * @param parameterValues A collection of list of typed values, one list per batch
   * @return An array of update counts containing one element for each command in the batch.
   */
  ExecuteBatchResult executeBatchProtobuf(StatementHandle h, List<Requests.UpdateBatch>
      parameterValues) throws NoSuchStatementException;
}

// End ProtobufMeta.java
