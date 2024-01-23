package org.apache.calcite.adapter.gremlin.driver;

import org.apache.groovy.groovysh.Groovysh;
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;

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

    @Override
    public <T> T getVariable(final String variableName) {
        return (T) groovysh.getInterp().getContext().getVariable(variableName);
    }

    @Override
    public <T> void setVariable(final String variableName, final T variableValue) {
        groovysh.getInterp().getContext().setVariable(variableName, variableValue);
    }

    @Override
    public void println(final String line) {
        if (null == io)
            groovysh.getIo().out.println(line);
        else
            io.out.println(line);
    }

    @Override
    public void errPrintln(final String line) {
        if (null == io)
            groovysh.getIo().err.println("[warn]" + line);
        else
            io.err.println("[warn]" + line);
    }

    @Override
    public <T> T execute(final String line) {
        return (T) groovysh.execute(line);
    }
}
