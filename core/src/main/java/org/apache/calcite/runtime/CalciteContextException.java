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
package org.apache.calcite.runtime;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Calcite code.

/**
 * Exception which contains information about the textual context of the causing
 * exception.
 */
public class CalciteContextException extends CalciteException {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * SerialVersionUID created with JDK 1.5 serialver tool. Prevents
   * incompatible class conflict when serialized from JDK 1.5-built server to
   * JDK 1.4-built client.
   */
  private static final long serialVersionUID = -54978888153560134L;

  //~ Instance fields --------------------------------------------------------

  private int posLine;

  private int posColumn;

  private int endPosLine;

  private int endPosColumn;

  private String originalStatement;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new CalciteContextException object. This constructor is for
   * use by the generated factory.
   *
   * @param message error message
   * @param cause   underlying cause, must not be null
   */
  public CalciteContextException(String message, Throwable cause) {
    this(message, cause, 0, 0, 0, 0);
  }

  /**
   * Creates a new CalciteContextException object.
   *
   * @param message      error message
   * @param cause        underlying cause, must not be null
   * @param posLine      1-based start line number
   * @param posColumn    1-based start column number
   * @param endPosLine   1-based end line number
   * @param endPosColumn 1-based end column number
   */
  public CalciteContextException(
      String message,
      Throwable cause,
      int posLine,
      int posColumn,
      int endPosLine,
      int endPosColumn) {
    super(message, cause);
    assert cause != null;
    setPosition(posLine, posColumn, endPosLine, endPosColumn);
  }

  /**
   * Creates a new CalciteContextException object. This constructor is for
   * use by the generated factory.
   *
   * @param message   error message
   * @param cause     underlying cause, must not be null
   * @param inputText is the orginal SQL statement, may be null
   */
  public CalciteContextException(
      String message,
      Throwable cause,
      String inputText) {
    this(message, cause, 0, 0, 0, 0);
    this.originalStatement = inputText;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Sets a textual position at which this exception was detected.
   *
   * @param posLine   1-based line number
   * @param posColumn 1-based column number
   */
  public void setPosition(int posLine, int posColumn) {
    this.posLine = posLine;
    this.posColumn = posColumn;
    this.endPosLine = posLine;
    this.endPosColumn = posColumn;
  }

  /**
   * Sets a textual range at which this exception was detected.
   *
   * @param posLine      1-based start line number
   * @param posColumn    1-based start column number
   * @param endPosLine   1-based end line number
   * @param endPosColumn 1-based end column number
   */
  public void setPosition(
      int posLine,
      int posColumn,
      int endPosLine,
      int endPosColumn) {
    this.posLine = posLine;
    this.posColumn = posColumn;
    this.endPosLine = endPosLine;
    this.endPosColumn = endPosColumn;
  }

  /**
   * @return 1-based line number, or 0 for missing position information
   */
  public int getPosLine() {
    return posLine;
  }

  /**
   * @return 1-based column number, or 0 for missing position information
   */
  public int getPosColumn() {
    return posColumn;
  }

  /**
   * @return 1-based ending line number, or 0 for missing position information
   */
  public int getEndPosLine() {
    return endPosLine;
  }

  /**
   * @return 1-based ending column number, or 0 for missing position
   * information
   */
  public int getEndPosColumn() {
    return endPosColumn;
  }

  /**
   * @return the input string that is associated with the context
   */
  public String getOriginalStatement() {
    return originalStatement;
  }

  /**
   * @param originalStatement - String to associate with the current context
   */
  public void setOriginalStatement(String originalStatement) {
    this.originalStatement = originalStatement;
  }

  @Override public String getMessage() {
    // The superclass' message is the textual context information
    // for this exception, so we add in the underlying cause to the message
    return super.getMessage() + ": " + getCause().getMessage();
  }
}

// End CalciteContextException.java
