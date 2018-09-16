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
package org.apache.calcite.util.javac;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * A <code>JavaCompilerArgs</code> holds the arguments for a
 * {@link JavaCompiler}.
 *
 * <p>Specific implementations of {@link JavaCompiler} may override <code>
 * set<i>Argument</i></code> methods to store arguments in a different fashion,
 * or may throw {@link UnsupportedOperationException} to indicate that the
 * compiler does not support that argument.
 */
public class JavaCompilerArgs {
  //~ Instance fields --------------------------------------------------------

  List<String> argsList = new ArrayList<>();
  List<String> fileNameList = new ArrayList<>();

  ClassLoader classLoader;

  //~ Constructors -----------------------------------------------------------

  public JavaCompilerArgs() {
    classLoader = getClass().getClassLoader();
  }

  //~ Methods ----------------------------------------------------------------

  public void clear() {
    fileNameList.clear();
  }

  /**
   * Sets the arguments by parsing a standard java argument string.
   *
   * <p>A typical such string is <code>"-classpath <i>classpath</i> -d <i>
   * dir</i> -verbose [<i>file</i>...]"</code>
   */
  public void setString(String args) {
    List<String> list = new ArrayList<>();
    StringTokenizer tok = new StringTokenizer(args);
    while (tok.hasMoreTokens()) {
      list.add(tok.nextToken());
    }
    setStringArray(list.toArray(new String[0]));
  }

  /**
   * Sets the arguments by parsing a standard java argument string. A typical
   * such string is <code>"-classpath <i>classpath</i> -d <i>dir</i> -verbose
   * [<i>file</i>...]"</code>
   */
  public void setStringArray(String[] args) {
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("-classpath")) {
        if (++i < args.length) {
          setClasspath(args[i]);
        }
      } else if (arg.equals("-d")) {
        if (++i < args.length) {
          setDestdir(args[i]);
        }
      } else if (arg.equals("-verbose")) {
        setVerbose(true);
      } else {
        argsList.add(args[i]);
      }
    }
  }

  public String[] getStringArray() {
    argsList.addAll(fileNameList);
    return argsList.toArray(new String[0]);
  }

  public void addFile(String fileName) {
    fileNameList.add(fileName);
  }

  public String[] getFileNames() {
    return fileNameList.toArray(new String[0]);
  }

  public void setVerbose(boolean verbose) {
    if (verbose) {
      argsList.add("-verbose");
    }
  }

  public void setDestdir(String destdir) {
    argsList.add("-d");
    argsList.add(destdir);
  }

  public void setClasspath(String classpath) {
    argsList.add("-classpath");
    argsList.add(classpath);
  }

  public void setDebugInfo(int i) {
    if (i > 0) {
      argsList.add("-g=" + i);
    }
  }

  /**
   * Sets the source code (that is, the full java program, generally starting
   * with something like "package com.foo.bar;") and the file name.
   *
   * <p>This method is optional. It only works if the compiler supports
   * in-memory compilation. If this compiler does not return in-memory
   * compilation (which the base class does not), {@link #supportsSetSource}
   * returns false, and this method throws
   * {@link UnsupportedOperationException}.
   */
  public void setSource(String source, String fileName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether {@link #setSource} will work.
   */
  public boolean supportsSetSource() {
    return false;
  }

  public void setFullClassName(String fullClassName) {
    // NOTE jvs 28-June-2004: I added this in order to support Janino's
    // JavaSourceClassLoader, which needs it.  Non-Farrago users
    // don't need to call this method.
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }
}

// End JavaCompilerArgs.java
