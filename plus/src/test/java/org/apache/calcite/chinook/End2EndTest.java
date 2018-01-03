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
package org.apache.calcite.chinook;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.io.CharStreams;

import net.hydromatic.quidem.Quidem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Entry point for all e2e tests based on Chinook data in hsqldb wrapped by calcite schema
 */
@RunWith(Parameterized.class)
public class End2EndTest {

  private static final Pattern FILE_PATERN = Pattern.compile(".*\\.iq");

  private final Path pathToQuidemFile;

  public End2EndTest(Path pathToQuidemFile) {
    this.pathToQuidemFile = pathToQuidemFile;
  }

  @Test
  public void test() throws Exception {
    InputStream is = QuidemFilesLocator.getResource(pathToQuidemFile.toString());
    String original = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
    StringReader reader = new StringReader(original);
    StringWriter writer = new StringWriter(original.length());
    new Quidem(reader, writer, env(), new ConnectionFactory()).execute();
    String result = writer.toString();
    int index = StringUtils.indexOfDifference(original, result);
    if (index == -1) {
      return;
    }
    throw new QuidemDiffException(pathToQuidemFile.toString(), result, index);
  }

  @Parameterized.Parameters(name = "{index}: quidem({0})")
  public static Collection<Path> data() throws IOException {
    return QuidemFilesLocator.locateFiles("/chinook");
  }

  private Function<String, Object> env() {
    return new Function<String, Object>() {
      public Object apply(String f) {
        return null;
      }
    };
  }

  /**
   * Exception for wrapping quidem assertion
   */
  public static class QuidemDiffException extends Exception {

    public QuidemDiffException(String originalFile, String result, int index) {
      super(originalFile + " differs at index " + index + " see result:\n"
              + result);
    }

  }

  /**
   * Logic to basic search all quidem files (test assertions) in class path
   */
  private static class QuidemFilesLocator {

    private QuidemFilesLocator() {
    }

    public static Collection<Path> locateFiles(String root) throws IOException {
      InputStream rootPathStream = getResource(root);
      BufferedReader reader = new BufferedReader(
              new InputStreamReader(rootPathStream, StandardCharsets.UTF_8));
      Collection<Path> result = new ArrayList<>();
      String path;
      while ((path = reader.readLine()) != null) {
        if (!FILE_PATERN.matcher(path).matches()) {
          continue;
        }
        result.add(Paths.get(root, path));
      }
      return result;
    }

    private static InputStream getResourceFromContextClassLoaded(String root) {
      return Thread.currentThread().getContextClassLoader().getResourceAsStream(root);
    }

    public static InputStream getResource(String root) {
      InputStream stream = getResourceFromContextClassLoaded(root);
      if (stream != null) {
        return stream;
      }
      return getResourceFromClass(root);
    }

    private static InputStream getResourceFromClass(String root) {
      return QuidemFilesLocator.class.getResourceAsStream(root);
    }

  }

}

// End End2EndTest.java
