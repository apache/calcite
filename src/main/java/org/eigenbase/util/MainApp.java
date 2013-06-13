/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.util;

/**
 * Abstract base class for a Java application invoked from the command-line.
 *
 * <p>Example usage:
 *
 * <blockquote>
 * <pre>public class MyClass extends MainApp {
 *     public static void main(String[] args) {
 *         new MyClass(args).run();
 *     }
 *     public void mainImpl() {
 *         System.out.println("Hello, world!");
 *     }
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since Aug 31, 2003
 */
public abstract class MainApp
{
    //~ Instance fields --------------------------------------------------------

    protected final String [] args;
    private OptionsList options = new OptionsList();
    private int exitCode;

    //~ Constructors -----------------------------------------------------------

    protected MainApp(String [] args)
    {
        this.args = args;
        exitCode = 0;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Does the work of the application. Derived classes must implement this
     * method; they can throw any exception they like, and {@link #run} will
     * clean up after them.
     */
    public abstract void mainImpl()
        throws Exception;

    /**
     * Does the work of the application, handles any errors, then calls {@link
     * System#exit} to terminate the application.
     */
    public final void run()
    {
        try {
            initializeOptions();
            mainImpl();
        } catch (Throwable e) {
            handle(e);
        }
        System.exit(exitCode);
    }

    /**
     * Sets the code which this program will return to the operating system.
     *
     * @param exitCode Exit code
     *
     * @see System#exit
     */
    public void setExitCode(int exitCode)
    {
        this.exitCode = exitCode;
    }

    /**
     * Handles an error. Derived classes may override this method to provide
     * their own error-handling.
     *
     * @param throwable Error to handle.
     */
    public void handle(Throwable throwable)
    {
        throwable.printStackTrace();
    }

    public void parseOptions(OptionsList.OptionHandler values)
    {
        options.parse(args);
    }

    /**
     * Initializes the application.
     */
    protected void initializeOptions()
    {
        options.add(
            new OptionsList.BooleanOption(
                "-h",
                "help",
                "Prints command-line parameters",
                false,
                false,
                false));
    }
}

// End MainApp.java
