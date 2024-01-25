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
package org.apache.calcite.adapter.gremlin.driver;

import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;

import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

public class BaseGroovyGremlinShellEnvironment implements GremlinShellEnvironment {

    private final Groovysh groovysh;
    private final IO io;

    public BaseGroovyGremlinShellEnvironment(final Groovysh groovysh) {
        this(groovysh, null);
    }

    public BaseGroovyGremlinShellEnvironment(final Groovysh groovysh, final IO io) {
        this.groovysh = groovysh;
        this.io = io;
    }

    @Override public <T> T getVariable(final String variableName) {
        return (T) groovysh.getInterp().getContext().getVariable(variableName);
    }

    @Override public <T> void setVariable(final String variableName, final T variableValue) {
        groovysh.getInterp().getContext().setVariable(variableName, variableValue);
    }

    @Override public void println(final String line) {
        if (null == io)
            groovysh.getIo().out.println(line);
        else
            io.out.println(line);
    }

    @Override public void errPrintln(final String line) {
        if (null == io)
            groovysh.getIo().err.println("[warn]" + line);
        else
            io.err.println("[warn]" + line);
    }

    @Override public <T> T execute(final String line) {
        return (T) groovysh.execute(line);
    }
}
