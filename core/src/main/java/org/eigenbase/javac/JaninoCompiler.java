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
package org.eigenbase.javac;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import org.eigenbase.util.*;

import org.codehaus.janino.*;
import org.codehaus.janino.util.*;
import org.codehaus.janino.util.resource.*;

/**
 * <code>JaninoCompiler</code> implements the {@link JavaCompiler} interface by
 * calling <a href="http://www.janino.net">Janino</a>.
 */
public class JaninoCompiler implements JavaCompiler {
  //~ Instance fields --------------------------------------------------------

  private JaninoCompilerArgs args = new JaninoCompilerArgs();

  // REVIEW jvs 28-June-2004:  pool this instance?  Is it thread-safe?
  private AccountingClassLoader classLoader;

  //~ Constructors -----------------------------------------------------------

  public JaninoCompiler() {
    args = new JaninoCompilerArgs();
  }

  //~ Methods ----------------------------------------------------------------

  // implement JavaCompiler
  public void compile() {
    // REVIEW: SWZ: 3/12/2006: When this method is invoked multiple times,
    // it creates a series of AccountingClassLoader objects, each with
    // the previous as its parent ClassLoader.  If we refactored this
    // class and its callers to specify all code to compile in one
    // go, we could probably just use a single AccountingClassLoader.

    assert args.destdir != null;
    assert args.fullClassName != null;
    assert args.source != null;

    ClassLoader parentClassLoader = args.getClassLoader();
    if (classLoader != null) {
      parentClassLoader = classLoader;
    }

    Map<String, byte[]> sourceMap = new HashMap<String, byte[]>();
    sourceMap.put(
        ClassFile.getSourceResourceName(args.fullClassName),
        args.source.getBytes());
    MapResourceFinder sourceFinder = new MapResourceFinder(sourceMap);

    classLoader =
        new AccountingClassLoader(
            parentClassLoader,
            sourceFinder,
            null,
            args.destdir == null ? null : new File(args.destdir));
    try {
      classLoader.loadClass(args.fullClassName);
    } catch (ClassNotFoundException ex) {
      throw Util.newInternal(ex, "while compiling " + args.fullClassName);
    }
  }

  // implement JavaCompiler
  public JavaCompilerArgs getArgs() {
    return args;
  }

  // implement JavaCompiler
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  // implement JavaCompiler
  public int getTotalByteCodeSize() {
    return classLoader.getTotalByteCodeSize();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Arguments to an invocation of the Janino compiler.
   */
  private static class JaninoCompilerArgs extends JavaCompilerArgs {
    String destdir;
    String fullClassName;
    String source;

    public JaninoCompilerArgs() {
    }

    public boolean supportsSetSource() {
      return true;
    }

    public void setDestdir(String destdir) {
      super.setDestdir(destdir);
      this.destdir = destdir;
    }

    public void setSource(String source, String fileName) {
      this.source = source;
      addFile(fileName);
    }

    public void setFullClassName(String fullClassName) {
      this.fullClassName = fullClassName;
    }
  }

  /**
   * Refinement of JavaSourceClassLoader which keeps track of the total
   * bytecode length of the classes it has compiled.
   */
  private static class AccountingClassLoader extends JavaSourceClassLoader {
    private final File destDir;
    private int nBytes;

    public AccountingClassLoader(
        ClassLoader parentClassLoader,
        ResourceFinder sourceFinder,
        String optionalCharacterEncoding,
        File destDir) {
      super(
          parentClassLoader,
          sourceFinder,
          optionalCharacterEncoding);
      this.destDir = destDir;
    }

    int getTotalByteCodeSize() {
      return nBytes;
    }

    // override JavaSourceClassLoader
    public Map generateBytecodes(String name)
        throws ClassNotFoundException {
      Map<String, byte[]> map = super.generateBytecodes(name);
      if (map == null) {
        return map;
      }

      if (destDir != null) {
        try {
          for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            File file = new File(destDir, entry.getKey() + ".class");
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(entry.getValue());
            fos.close();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // NOTE jvs 18-Oct-2006:  Janino has actually compiled everything
      // to bytecode even before all of the classes have actually
      // been loaded.  So we intercept their sizes here just
      // after they've been compiled.
      for (Object obj : map.values()) {
        byte[] bytes = (byte[]) obj;
        nBytes += bytes.length;
      }
      return map;
    }
  }
}

// End JaninoCompiler.java
