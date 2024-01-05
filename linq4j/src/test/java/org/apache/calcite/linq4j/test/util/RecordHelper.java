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
package org.apache.calcite.linq4j.test.util;

import org.opentest4j.TestAbortedException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Helper that compiles a record instance dynamically, if compatible, and initializes instances of
 * Java records.
 */
public class RecordHelper {

  private RecordHelper(){}

  private static final String RECORD_TEMPLATE = "public record %s(String name, int count) {}";
  private static final String JAVA_FILE_NAME_TEMPLATE = "%s.java";

  /** Creates a Java record, aborts if the JDK is non-compatible. */
  public static Class<?> createRecordClass(Path tempDir, String className) {
    if (canSupportRecords()) {
      return compileAndLoadClass(tempDir, className);
    } else {
      throw new TestAbortedException("Records not supported");
    }
  }

  static boolean canSupportRecords() {
    try {
      Class.class.getMethod("isRecord");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /** Compiles and loads a Java record dynamically. */
  private static Class<?> compileAndLoadClass(Path tempDir, String className) {
    createAndCompileTempClass(tempDir, className);

    try {
      return Class.forName(className,
          true,
          URLClassLoader.newInstance(new URL[] {tempDir.toUri().toURL() }));
    } catch (ClassNotFoundException | MalformedURLException e) {
      throw new IllegalArgumentException("Could not load class.");
    }
  }

  /** Creates a temporary Java record and compiles it. */
  public static void createAndCompileTempClass(Path tempDir, String className) {
    String classSourceCode =
        String.format(Locale.ROOT, RECORD_TEMPLATE, className);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();


    Path tempJavaClassFile =
        tempDir.resolve(String.format(Locale.ROOT, JAVA_FILE_NAME_TEMPLATE, className));
    try {
      Files.write(tempJavaClassFile, classSourceCode.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not write file.");
    }
    compiler.run(null, null, null, tempJavaClassFile.toAbsolutePath().toString());
  }

  /** Creates new instances of the loaded class. */
  public static Object createInstance(Class<?> clazz, String nameFieldValue, int countFieldValue) {
    Constructor<?> constructor = null;
    try {
      constructor = clazz.getDeclaredConstructor(String.class, int.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Could not find constructor");
    }
    try {
      return constructor.newInstance(nameFieldValue, countFieldValue);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Could not create instance");
    }
  }
}
