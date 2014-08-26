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

/**
 * SynchronizedJaninoCompiler exists as a fallback in case Janino has
 * more multi-threading bugs.  We hope never to have to use it, but
 * if necessary:
 * <code>alter system set "javaCompilerClassName" =
 * 'org.eigenbase.javac.SynchronizedJaninoCompiler';</code>
 */
public class SynchronizedJaninoCompiler extends JaninoCompiler {
  // override JaninoCompiler
  public void compile() {
    synchronized (SynchronizedJaninoCompiler.class) {
      super.compile();
    }
  }
}

// End SynchronizedJaninoCompiler.java
