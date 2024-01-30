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
package org.apache.calcite.server;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

/** Abstract implementation of {@link org.apache.calcite.server.DdlExecutor}. */
public class DdlExecutorImpl implements DdlExecutor, ReflectiveVisitor {
  /** Creates a DdlExecutorImpl.
   * Protected only to allow sub-classing;
   * use a singleton instance where possible. */
  protected DdlExecutorImpl() {
  }

  /** Dispatches calls to the appropriate method based on the type of the
   * first argument. */
  @SuppressWarnings({"method.invocation.invalid", "argument.type.incompatible"})
  private final ReflectUtil.MethodDispatcher<Void> dispatcher =
      ReflectUtil.createMethodDispatcher(void.class, this, "execute",
          SqlNode.class, CalcitePrepare.Context.class);

  @Override public void executeDdl(CalcitePrepare.Context context,
      SqlNode node) {
    dispatcher.invoke(node, context);
  }

  /** Template for methods that execute DDL commands.
   *
   * <p>The base implementation throws {@link UnsupportedOperationException}
   * because a {@link SqlNode} is not DDL, but overloaded methods such as
   * {@code public void execute(SqlCreateFoo, CalcitePrepare.Context)} are
   * called via reflection. */
  public void execute(SqlNode node, CalcitePrepare.Context context) {
    throw new UnsupportedOperationException("DDL not supported: " + node);
  }
}
