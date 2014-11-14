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
package org.apache.calcite.util;

/**
 * Exception intended to be used for control flow, as opposed to the usual
 * use of exceptions which is to signal an error condition.
 *
 * <p>{@code ControlFlowException} does not populate its own stack trace, which
 * makes instantiating one of these (or a sub-class) more efficient.</p>
 */
public class ControlFlowException extends RuntimeException {
  @Override public Throwable fillInStackTrace() {
    return this;
  }
}

// End ControlFlowException.java
