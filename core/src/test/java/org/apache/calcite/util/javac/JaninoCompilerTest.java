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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
public class JaninoCompilerTest {

  // Reproduces upstream issue from Janino https://github.com/janino-compiler/janino/issues/174
  @Test
  public void compile_long_method(@TempDir Path tempDir) throws Exception {
    String code = getClassCode(getClassBody(1000));
    JaninoCompiler compiler = new JaninoCompiler();
    compiler.getArgs().setDestdir(tempDir.toFile().getAbsolutePath());
    compiler.getArgs().setSource(code, "Nuke.java");
    compiler.getArgs().setFullClassName("Nuke");
    compiler.compile();

    URL[] urls = new URL[]{tempDir.toUri().toURL()};
    ClassLoader cl = new URLClassLoader(urls);
    Class clazz = cl.loadClass("Nuke");
    Object o = clazz.getDeclaredConstructor().newInstance();
    System.out.println(o);
  }

  private static String getClassCode(String body) {
    return "import java.util.Arrays;\n\n"
        + "public class Nuke {"
        + body
        + "}";
  }

  private static String getClassBody(int numAssignments) {
    String template = "public Object[] nuke() {\n"
        + "\tString bloat = \"some_bloat\";\n"
        + "\t//Lots of variables\n"
        + "%s"
        + "\t//Big array initialization\n"
        + "\treturn new Object[]\n"
        + "\t{\n"
        + "%s"
        + "\t};\n"
        + "}\n"
        + "\n"
        + ""
        + "public static void main(String[] args) throws Exception {"
        + "\tSystem.out.println(Arrays.toString(new Nuke().nuke()));"
        + "}";
    StringBuilder assignments = new StringBuilder();
    StringBuilder appends = new StringBuilder();
    for (int i = 0; i < numAssignments; i++) {
      assignments.append(String.format("\tfinal String current%s = bloat;\n", i));
      appends.append(String.format("\t\tcurrent%s", i));
      if (i < numAssignments - 1) {
        appends.append(",");
      }
      appends.append("\n");
    }
    return String.format(template, assignments, appends);
  }
}
