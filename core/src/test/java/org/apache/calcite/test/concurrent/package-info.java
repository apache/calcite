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

/**
 * A framework for concurrent JDBC unit tests.
 *
 * <p>The class <code>ConcurrentCommandGenerator</code> provides a Java API for
 * constructing concurrent JDBC tests: an instance of the class represents a
 * test case, which contains several sequences of SQL commands (abstracted as
 * subclasses of <code>ConcurrentCommand</code>). Each sequence is run in its
 * own thread as a separate jdbc client (ie a separate
 * <code>java.sql.Connection</code>). There are facilties to synchronize these
 * command threads. Only a simple command sequence is supported: no branching,
 * no looping.</p>
 *
 * <p>An alternative is to define a test by writing a test script in
 * <code>mtsql</code> format, as described below. An instance of
 * <code>ConcurrentCommandScript</code> parses and executes a script.</p>
 *
 * <h3>Script Format</h3>
 *
 * <h4>Syntax:</h4>
 *
 * <p>The syntactic structure of an <i>mtsql</i> script is:
 *
 * <blockquote><pre>
 *     &lt;directive&gt;*
 *     &lt;setup section&gt;?
 *     &lt;cleanup section&gt;?
 *     &lt;thread section&gt;+
 * &nbsp;
 *     &lt;directive&gt;       := &#64;[no]lockstep | &#64;enable | &#64;disable
 *     &lt;setup section&gt;   := &#64;setup &lt;basic command&gt;* &#64;end
 *     &lt;cleanup section&gt; := &#64;setup &lt;basic command&gt;* &#64;end
 *     &lt;thread section&gt;  := &#64;thread &lt;thread-name&gt;?
 *                                &lt;command&gt;* &#64;end
 * &nbsp;
 *     &lt;command&gt; :=
 *       &lt;basic command&gt; |
 *       &lt;command prefix&gt;? &lt;threadly command&gt; |
 *       &lt;synchronization point&gt;
 * </pre></blockquote>
 *
 * <p>Blank lines and comments are allowed anywhere.
 *     A comment starts with two hyphens and runs to the end of the line.
 *     Command names start with an '&#64;'.
 *     Some commands run to the end of the line; but a command that contains SQL
 *     can
 *     span lines and ends with a semicolon.
 *
 * <h4>Semantics:</h4>
 *
 * <p>Running a section means running its commands in sequence.
 *     First the setup section (if any) is run.
 *     Next all the thread sections are run at once, each in its own thread.
 *     When all these threads complete, the cleanup section (if any) is run.
 *
 * <h4>Synchronization:</h4>
 *
 * <p>The threads are synchronized by inserting synchronization points
 * (&#64;sync).</p>
 *
 * <p>When a thread reaches a &#64;sync, it waits until all threads are waiting
 * on the same &#64;sync: then all threads proceed. &#64;sync points have no
 * names. Clearly all thread sections must contain the same number of &#64;sync
 * points.</p>
 *
 * <p>The directive &#64;lockstep has the same effect as adding a &#64;sync
 * after each command in every thread section. Clearly it requires that all
 * thread sections have the same number of commands. The default is the antonym
 * &#64;nolockstep.</p>
 *
 * <p>The directive &#64;disable means "skip this script". The deault is the
 * antonym &#64;enable.</p>
 *
 *
 * <h4>Error handling: </h4>
 *
 * <p>When a sql command fails, the rest of its section is skipped. However, if
 * the attribute <i>force</i> is true the error is ignored, and the section
 * continues.  <i>force</i> has an independent value in each section.  Within a
 * section it can be toggled using the sql directive <code>!SET FORCE
 * <i>val</i></code>, where <i>val</i> can be <i>true, false, on, off.</i> (This
 * is modelled after sqlline and sqllineClient. Other sqlline
 * <i>!-</i>directives are ignored.)</p>
 *
 * <p>An error in a thread section will stop that thread, but the other threads
 * continue (with one fewer partner to synchronize with), and finally the
 * cleanup section runs. If the setup section quits, then only the cleanup
 * section is run.</p>
 *
 * <h4>Basic Commands (allowed in any section):</h4>
 *
 * <blockquote><pre>
 * &lt;SQL statement&gt;:
 *     An SQL statement terminated by a semicolon. The statement can span lines.
 * </pre></blockquote>
 *
 * <blockquote><pre>
 * &#64;include FILE
 *   Reads and executes the contents of FILE, another mtsql script.
 *   Inclusions may nest.
 * </pre></blockquote>
 *
 * <h4>Threaded Commands (allowed only in a &#64;thread section):</h4>
 *
 * <blockquote><pre>
 * &#64;sleep N        -- thread sleeps for N milliseconds
 * &#64;echo MESSAGE   -- prints the message to stdout
 * &nbsp;
 * &lt;SQL statement&gt; ';' -- executes the SQL
 * &#64;timeout N &lt;SQL&gt; ';' -- executes the SQL with the given ms timeout
 * &#64;rowlimit N &lt;SQL&gt; ';' -- executes the SQL, stops after N rows
 * &#64;err &lt;SQL&gt; ';' -- executes the SQL, expecting it to fail
 * &nbsp;
 * &#64;repeat N &lt;command&gt;+ &#64;end
 *     Denotes a repeated block of commands, with repeat count = N.
 *     N must be positive.
 * &nbsp;
 * &#64;prepare SQL-STATEMENT ';'
 *     Prepares the sql. A thread has at most one prepared statement at a time.
 * &nbsp;
 * &#64;print FORMAT
 *     Sets the result-printing format for the current prepared statement.
 *     FORMAT is a sequence of the phrases:
 *         none             -- means print nothing
 *         all              -- means print all rows (the default)
 *         every N          -- means print the rows 0, N, 2N, etc.
 *         count            -- means print row number (starts with 0).
 *         time             -- means print the time each printed row was fetched
 *         total            -- means print a final summary, with row count and
 *                             net fetch time (not including any timeout).
 * &nbsp;
 * (Sorry, no way yet to print selected columns, to print time in a special way,
 * etc.)
 * &nbsp;
 * &#64;fetch &lt;timeout&gt;?
 *     Starts fetching and printing result rows, with an optional timeout (in
 *     msecs).  Stop on EOD or on timeout.
 * &nbsp;
 * &#64;close
 *     Closes the current prepared statement. However that an open prepared
 *     statement will be closed automatically at the end of its thread.
 * &nbsp;
 * &#64;shell &lt;Shell Command&gt;
 *     Runs the command in a spawned subshell, proceeds after it concludes, but
 *     quits if it fails.  For &#64;shell and &#64;echo, the command or message
 *     runs to the end of the line in the script, but can be continued if the
 *     line ends with a single '\'.
 * </pre></blockquote>
 *
 * <h4>Substituted Variables</h4>
 *
 * <p>Needed mainly to pass arguments to the command of &#64;shell, but also
 *     useful to
 *     parameterize SQL statements, timeout values etc.
 *
 * <ul>
 *     <li>Variable Expansion: If VAR is a declared variable, $VAR is replaced
 *         by the value of VAR. Quotes are ignored.  $$ expands to $. A variable
 *         cannot expand to a mtsql command name.</li>
 *
 *     <li>Variable Declaration: Before being used, a script variable must be
 *         explicitly declared in the script (or an included script)
 *         by a &#64;var command.</li>
 * </ul>
 *
 * <blockquote><pre>
 * &#64;var VAR
 *     Declares a variable VAR
 * &#64;var VAR1  VAR2 ... VARn
 *     Declares n variables.
 * </pre></blockquote>
 *
 * <p>The initial value of a script variable VAR is taken from the shell
 *     environment variable of the same name.  The value can be set to a
 *     different value when the script is run, by employing a phrase
 *     VAR=VALUE on the mtsql command line.</p>
 *
 * <h4>Stand-Alone Tool</h4>
 *
 * <p>A command-line tool that runs an mtsql script against a specified JDBC
 *     connection,a nd prints the query results. (But see &#64;print command to
 *     filter the output.)</p>
 *
 * <p> Usage: mtsql [-qvg] -u SERVER -d DRIVER [-n USER] [-p PASSWORD]
 *     [VAR=VALUE]* SCRIPT [SCRIPT]*<br>
 *     Flags: -q : (quiet) do not print results.<br>
 *     -v : (verbose) trace as script is parsed.<br>
 *     -g : (debug) print command lists before starting the threads<br>
 *     -u SERVER : sets the target; a JDBC URL.<br>
 *     -d DRIVER : sets the jdbc driver; a class on the classpath<br>
 *     VAR=VALUE : binds the script variable VAR to the VALUE; VAR must be
 *     declared at the beginning of the script(s) in a &#64;var command.</p>
 *
 *
 * <h4>Example Script</h4>
 *
 * <blockquote><pre>-- redundant:
 * &#64;nolockstep
 * &nbsp;
 * -- Two threads reading the same data.
 * &#64;thread 1,2
 *         -- pre execute the SQL to prime the pumps
 *         &#64;timeout 1000 select * from sales.bids;
 * &nbsp;
 *         &#64;prepare select * from sales.bids;
 * &nbsp;
 *         -- rendezvous with writer thread
 *         &#64;sync
 *         &#64;fetch 15000
 *         &#64;sync
 *         &#64;close
 * &#64;end
 * &nbsp;
 * &#64;thread writer
 *         -- rendezvous with reader threads
 *         &#64;sync
 *         &#64;sleep 5000
 *         insert into sales.bids
 *                 values(1,  'ORCL', 100, 12.34, 10000, 'Oracle at 12.34');
 *         commit;
 *         insert into sales.bids
 *                 values(2,  'MSFT', 101, 23.45, 20000, 'Microsoft at 23.45');
 *         commit;
 * &nbsp;
 *         -- real test has more inserts here
 * &nbsp;
 *         &#64;sync
 * &#64;end</pre></blockquote>
 *
 * <h3>Example Output File</h3>
 *
 * <p>The output from each thread is stored in a temporary file until
 * the test completes. At that point, the files are merged together
 * into a single <code>.log</code> file containing the results of each
 * thread, in the order the threads were defined. The output for the
 * example script looks like:
 *
 * <blockquote><pre>-- thread 1
 * &gt; select * from sales.bids;
 * +---------+------------+
 * | DEPTNO  |    NAME    |
 * +---------+------------+
 * | 10      | Sales      |
 * | 20      | Marketing  |
 * | 30      | Accounts   |
 * +---------+------------+
 * &gt;
 * &gt; select * from sales.bids;
 * +---------+------------+
 * | DEPTNO  |    NAME    |
 * +---------+------------+
 * | 10      | Sales      |
 * | 20      | Marketing  |
 * | 30      | Accounts   |
 * +---------+------------+
 * &nbsp;
 * -- end of thread 1
 * &nbsp;
 * -- thread 2
 * &gt; select * from sales.bids;
 * +---------+------------+
 * | DEPTNO  |    NAME    |
 * +---------+------------+
 * | 10      | Sales      |
 * | 20      | Marketing  |
 * | 30      | Accounts   |
 * +---------+------------+
 * &gt;
 * &gt; select * from sales.bids;
 * +---------+------------+
 * | DEPTNO  |    NAME    |
 * +---------+------------+
 * | 10      | Sales      |
 * | 20      | Marketing  |
 * | 30      | Accounts   |
 * +---------+------------+
 * &nbsp;
 * -- end of thread 2
 * &nbsp;
 * -- thread writer
 * &gt; insert into sales.bids
 * &gt;    values(1,  'ORCL', 100, 12.34,     10000, 'Oracle at 12.34');
 * 1 row affected.
 * &gt; commit;
 * &gt; insert into sales.bids
 * &gt;    values(2,  'MSFT', 101, 23.45,     20000, 'Microsoft at 23.45');
 * 1 row affected.
 * &gt; commit;
 * -- end of thread writer</pre></blockquote>
 *
 * <p>(Yes the results of the select statements are obviously wrong.)
 *
 *         <h3>Open Issues</h3>
 *
 *         <ul>
 *             <li>Repeating tests for a period of time isn't supported.</li>
 *         </ul>
 *
 */
@PackageMarker
package org.apache.calcite.test.concurrent;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
