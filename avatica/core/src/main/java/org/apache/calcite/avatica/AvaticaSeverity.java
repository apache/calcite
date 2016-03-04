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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.proto.Common;

import java.util.Objects;

/**
 * An enumeration that denotes the severity of a given unexpected state.
 */
public enum AvaticaSeverity {
  /**
   * The severity of the outcome of some unexpected state is unknown.
   */
  UNKNOWN(0),

  /**
   * The system has been left in an unrecoverable state as a result of an operation.
   */
  FATAL(1),

  /**
   * The result of an action resulted in an error which the current operation cannot recover
   * from. Clients can attempt to execute the operation again.
   */
  ERROR(2),

  /**
   * The operation completed successfully but a message was generated to warn the client about
   * some unexpected state or action.
   */
  WARNING(3);

  private final int value;

  AvaticaSeverity(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public Common.Severity toProto() {
    switch (this) {
    case UNKNOWN:
      return Common.Severity.UNKNOWN_SEVERITY;
    case FATAL:
      return Common.Severity.FATAL_SEVERITY;
    case ERROR:
      return Common.Severity.ERROR_SEVERITY;
    case WARNING:
      return Common.Severity.WARNING_SEVERITY;
    default:
      throw new RuntimeException("Unhandled Severity level: " + this);
    }
  }

  public static AvaticaSeverity fromProto(Common.Severity proto) {
    switch (Objects.requireNonNull(proto)) {
    case UNKNOWN_SEVERITY:
      return AvaticaSeverity.UNKNOWN;
    case FATAL_SEVERITY:
      return AvaticaSeverity.FATAL;
    case ERROR_SEVERITY:
      return AvaticaSeverity.ERROR;
    case WARNING_SEVERITY:
      return AvaticaSeverity.WARNING;
    default:
      throw new RuntimeException("Unhandled protobuf Severity level: " + proto);
    }
  }
}

// End AvaticaSeverity.java
