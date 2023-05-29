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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ControlFlowException;

/**
 * Provides {@link MetadataHandler} call sites for
 * {@link RelMetadataQuery}. The handlers provided are responsible for
 * updating the cache stored in {@link RelMetadataQuery}.
 */
public interface MetadataHandlerProvider {

  /**
   * Provide a handler for the requested metadata class.
   *
   * @param handlerClass The handler interface expected
   * @param <MH> The metadata type the handler relates to.
   * @return The handler implementation.
   */
  <MH extends MetadataHandler<?>> MH handler(Class<MH> handlerClass);

  /** Re-generates the handler for a given kind of metadata.  */
  /**
   * Revise the handler for a given kind of metadata.
   *
   * <p>Should be invoked if the existing handler throws a {@link NoHandler} exception.
   *
   * @param handlerClass The type of class to revise.
   * @param <MH> The type metadata the handler provides.
   * @return A new handler that should be used instead of any previous handler provided.
   */
  default <MH extends MetadataHandler<?>> MH revise(Class<MH> handlerClass) {
    throw new UnsupportedOperationException("This provider doesn't support handler revision.");
  }

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class. */
  class NoHandler extends ControlFlowException {
    public final Class<? extends RelNode> relClass;

    public NoHandler(Class<? extends RelNode> relClass) {
      this.relClass = relClass;
    }
  }

}
