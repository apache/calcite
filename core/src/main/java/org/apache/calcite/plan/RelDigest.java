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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;

import org.apiguardian.api.API;

/**
 * The digest is the exact representation of the corresponding {@code RelNode},
 * at anytime, anywhere. The only difference is that digest is compared using
 * {@code #equals} and {@code #hashCode}, which are prohibited to override
 * for RelNode, for legacy reasons.
 *
 * <p>INTERNAL USE ONLY.</p>
 */
@API(since = "1.24", status = API.Status.INTERNAL)
public interface RelDigest {
  /**
   * Reset state, possibly cache of hash code.
   */
  void clear();

  /**
   * Returns the relnode that this digest is associated with.
   */
  RelNode getRel();
}
