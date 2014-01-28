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
package org.eigenbase.rel.metadata;

import org.eigenbase.rel.*;

/**
 * RelMetadataProvider defines an interface for obtaining metadata about
 * relational expressions. This interface is weakly-typed and is not intended to
 * be called directly in most contexts; instead, use a strongly-typed facade
 * such as {@link RelMetadataQuery}.
 *
 * <p>For background and motivation, see <a
 * href="http://wiki.eigenbase.org/RelationalExpressionMetadata">wiki</a>.
 */
public interface RelMetadataProvider {
  //~ Methods ----------------------------------------------------------------

  /**
   * Retrieves metadata about a relational expression.
   *
   * @param rel               relational expression of interest
   * @param metadataQueryName name of metadata query to invoke
   * @param args              arguments to metadata query (expected number and
   *                          type depend on query name; must have well-defined
   *                          hashCode/equals for use by caching); null can be
   *                          used instead of empty array
   * @return metadata result (actual type depends on query name), or null if
   * the provider cannot answer the given query/rel combination; it is better
   * to return null than to return a possibly incorrect answer
   */
  Object getRelMetadata(
      RelNode rel,
      String metadataQueryName,
      Object[] args);
}

// End RelMetadataProvider.java
