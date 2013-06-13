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
package org.eigenbase.test.concurrent;

import java.io.PrintStream;
import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

import org.eigenbase.util.Util;

import net.hydromatic.optiq.jdbc.SqlTimeoutException;


/**
 * ConcurrentTestCommandGenerator creates instances of {@link
 * ConcurrentTestCommand} that perform specific actions in a specific
 * order and within the context of a test thread ({@link
 * ConcurrentTestCommandExecutor}).
 *
 * <p>Typical actions include preparing a SQL statement for execution, executing
 * the statement and verifying its result set, and closing the statement.
 *
 * <p>A single ConcurrentTestCommandGenerator creates commands for
 * multiple threads. Each thread is represented by an integer "thread ID".
 * Thread IDs may take on any positive integer value and may be a sparse set
 * (e.g. 1, 2, 5).
 *
 * <p>When each command is created, it is associated with a thread and given an
 * execution order. Execution order values are positive integers, must be unique
 * within a thread, and may be a sparse set.
 *
 * <p>There are no restrictions on the order of command creation.
 */
public class ConcurrentTestCommandGenerator {
    //~ Static fields/initializers ---------------------------------------------

    private static final char APOS = '\'';
    private static final char COMMA = ',';
    private static final char LEFT_BRACKET = '{';
    private static final char RIGHT_BRACKET = '}';

    //~ Instance fields --------------------------------------------------------
    protected boolean debug = false;
    protected PrintStream debugStream = System.out;
    protected String jdbcURL;
    protected Properties jdbcProps;


    /**
     * Maps Integer thread IDs to a TreeMap. The TreeMap vaules map an Integer
     * execution order to a {@link ConcurrentTestCommand}.
     */
    private TreeMap<Integer, TreeMap<Integer, ConcurrentTestCommand>>
        threadMap;

    /**
     * Maps Integer thread IDs to thread names.
     */
    private TreeMap<Integer, String> threadNameMap;

    /** Describes a thread that failed */
    public static class FailedThread {
        public final String name;
        public final String location;
        public final Throwable failure;

        public FailedThread(String name, String location, Throwable failure) {
            this.name = name;
            this.location = location;
            this.failure = failure;
        }
    }

    /** Collects threads that failed. Cleared when execution starts, valid whe/n
     * execution has ended. Only failed threads appear in the list, so after a
     * successful test the list is empty.
     */
    private List<FailedThread> failedThreads;


    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new ConcurrentTestCommandGenerator.
     */
    public ConcurrentTestCommandGenerator()
    {
        threadMap =
            new TreeMap<Integer,
                TreeMap<Integer, ConcurrentTestCommand>>();
        threadNameMap = new TreeMap<Integer, String>();
        failedThreads = new ArrayList<FailedThread>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a synchronization commands. When a thread reaches a synchronization
     * command it stops and waits for all other threads to reach their
     * synchronization commands. When all threads have reached their
     * synchronization commands, they are all released simultaneously (or as
     * close as one can get with {@link Object#notifyAll()}). Each thread must
     * have exactly the same number of synchronization commands.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addSynchronizationCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new SynchronizationCommand());
    }

    /**
     * Causes the given thread to sleep for the indicated number of
     * milliseconds.  Thread executes {@link java.lang.Thread#sleep(long)}.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param millis the length of time to sleep in milliseconds (must not be
     * negative)
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addSleepCommand(
        int threadId,
        int order,
        long millis)
    {
        return addCommand(
            threadId,
            order,
            new SleepCommand(millis));
    }

    /**
     * Adds an "explain plan" command.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the explain plan SQL (e.g. <code>"explain plan for select *
     * from t"</code>)
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addExplainCommand(
        int threadId,
        int order,
        String sql)
    {
        assert (sql != null);

        ConcurrentTestCommand command = new ExplainCommand(sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Creates a {@link PreparedStatement} for the given SQL. This command does
     * not execute the SQL, it merely creates a PreparedStatement and stores it
     * in the ConcurrentTestCommandExecutor.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the SQL to prepare (e.g. <code>"select * from t"</code>)
     *
     * @return the newly-added command
     *
     * @see #addFetchAndCompareCommand(int, int, int, String)
     */
    public ConcurrentTestCommand addPrepareCommand(
        int threadId,
        int order,
        String sql)
    {
        assert (sql != null);

        ConcurrentTestCommand command = new PrepareCommand(sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Executes a previously {@link #addPrepareCommand(int, int, String)
     * prepared} SQL statement and compares its {@link ResultSet} to the given
     * data.
     *
     * <p><b>Expected data format:</b> <code>{ 'row1, col1 value', 'row1, col2
     * value', ... }, { 'row2, col1 value', 'row2, col2 value', ... },
     * ...</code>
     *
     * <ul>
     * <li>For string data: enclose value in apostrophes, use doubled apostrophe
     * to include an spostrophe in the value.</li>
     * <li>For integer or real data: simply use the stringified value (e.g. 123,
     * 12.3, 0.65). No scientific notation is allowed.</li>
     * <li>For null values, use the word <code>null</code> without quotes.</li>
     * </ul>
     * <b>Example:</b> <code>{ 'foo', 10, 3.14, null }</code>
     *
     * <p><b>Note on timeout:</b> If the previously prepared statement's {@link
     * Statement#setQueryTimeout(int)} method throws an <code>
     * UnsupportedOperationException</code> it is ignored and no timeout is set.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param timeout the query timeout, in seconds (see above)
     * @param expected the expected results (see above)
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addFetchAndCompareCommand(
        int threadId,
        int order,
        int timeout,
        String expected)
    {
        ConcurrentTestCommand command =
            new FetchAndCompareCommand(timeout, expected);

        return addCommand(threadId, order, command);
    }

    /**
     * Closes a previously {@link #addPrepareCommand(int, int, String) prepared}
     * SQL statement.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addCloseCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new CloseCommand());
    }

    /**
     * Executes the given SQL via {@link Statement#executeUpdate(String)}. May
     * be used for update as well as insert statements.
     *
     * <p><b>Note on timeout:</b> If the previously prepared statement's {@link
     * Statement#setQueryTimeout(int)} method throws an <code>
     * UnsupportedOperationException</code> it is ignored and no timeout is set.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param timeout the query timeout, in seconds (see above)
     * @param sql the insert/update/delete SQL
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addInsertCommand(
        int threadId,
        int order,
        int timeout,
        String sql)
    {
        ConcurrentTestCommand command = new InsertCommand(timeout, sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Commits pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addCommitCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new CommitCommand());
    }

    /**
     * Rolls back pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addRollbackCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new RollbackCommand());
    }

    /**
     * Executes a DDL statement immediately. Assumes the statement returns no
     * information.
     *
     * @return the newly-added command
     */
    public ConcurrentTestCommand addDdlCommand(
        int threadId,
        int order,
        String ddl)
    {
        return addCommand(
            threadId,
            order,
            new DdlCommand(ddl));
    }

    /**
     * Handles adding a command to {@link #threadMap}.
     *
     * @return the newly-added command
     */
    protected ConcurrentTestCommand addCommand(
        int threadId,
        int order,
        ConcurrentTestCommand command)
    {
        assert (threadId > 0);
        assert (order > 0);

        TreeMap<Integer, ConcurrentTestCommand> commandMap =
            threadMap.get(threadId);
        if (commandMap == null) {
            commandMap = new TreeMap<Integer, ConcurrentTestCommand>();
            threadMap.put(threadId, commandMap);
        }

        // check for duplicate order numbers
        assert (!commandMap.containsKey(order));

        commandMap.put(order, command);
        return command;
    }

    /**
     * Configures a human-readable name for a given thread identifier. Does not
     * imply that the thread will be created -- that only happens if there are
     * commands added to the thread.
     */
    public void setThreadName(int threadId, String name)
    {
        threadNameMap.put(new Integer(threadId), name);
    }

    protected void setDebug(boolean enabled)
    {
        debug = enabled;
    }

    protected void setDebug(
        boolean enabled,
        PrintStream alternatePrintStream)
    {
        debug = enabled;
        debugStream = alternatePrintStream;
    }


    /**
     * Sets the jdbc data source for executing the command threads.
     */
    public void setDataSource(String jdbcURL, Properties jdbcProps)
    {
        this.jdbcURL = jdbcURL;
        this.jdbcProps = jdbcProps;
    }


   /**
     * Creates a {@link ConcurrentTestCommandExecutor} object for each define thread,
     * and then runs them all.
     *
     * @throws Exception if no connection found or if a thread operation is
     * interrupted
     */
    public void execute() throws Exception
    {
        ConcurrentTestCommandExecutor [] threads = innerExecute();
        postExecute(threads);
    }

    protected ConcurrentTestCommandExecutor[] innerExecute() throws Exception
    {
        failedThreads.clear();
        Set threadIds = getThreadIds();
        ConcurrentTestCommandExecutor.Sync sync =
            new ConcurrentTestCommandExecutor.Sync(threadIds.size());

        // initialize command executors
        ConcurrentTestCommandExecutor [] threads =
            new ConcurrentTestCommandExecutor[threadIds.size()];

        int threadIndex = 0;
        for (Iterator i = threadIds.iterator(); i.hasNext();) {
            Integer threadId = (Integer) i.next();
            Iterator commands = getCommandIterator(threadId);

            if (debug) {
                debugStream.println(
                    "Thread ID: " + threadId + " ("
                    + getThreadName(threadId)
                    + ")");
                printCommands(debugStream, threadId);
            }

            threads[threadIndex++] =
                new ConcurrentTestCommandExecutor(
                    threadId.intValue(), getThreadName(threadId),
                    this.jdbcURL, this.jdbcProps,
                    commands,
                    sync,
                    this.debug ? this.debugStream : null);
        }

        // start all the threads
        for (int i = 0, n = threads.length; i < n; i++) {
            threads[i].start();
        }

        // wait for all threads to finish
        for (int i = 0, n = threads.length; i < n; i++) {
            threads[i].join();
        }
        return threads;
    }

    protected void postExecute(ConcurrentTestCommandExecutor[] threads)
        throws Exception
    {
        // check for failures
        if (requiresCustomErrorHandling()) {
            for (int i = 0, n = threads.length; i < n; i++) {
                ConcurrentTestCommandExecutor executor = threads[i];
                if (executor.getFailureCause() != null) {
                    customErrorHandler(executor);
                }
            }
        } else {
            for (int i = 0, n = threads.length; i < n; i++) {
                Throwable cause = threads[i].getFailureCause();
                if (cause != null) {
                    failedThreads.add(
                        new FailedThread(
                            threads[i].getName(),
                            threads[i].getFailureLocation(),
                            cause));
                }
            }
        }
    }


    /**
     * Returns whether any test thread failed. Valid after {@link #execute} has
     * returned.
     */
    public boolean failed()
    {
        return !failedThreads.isEmpty();
    }

    /** @return the list of failed threads (unmodifiable) */
    public List<FailedThread> getFailedThreads()
    {
        return Collections.unmodifiableList(failedThreads);
    }


    /**
     * Insures that the number of commands is the same for each thread, fills
     * missing order value with null commands, and interleaves a synchronization
     * command before each actual command. These steps are required for
     * synchronized execution in FarragoConcurrencyTestCase.
     */
    public void synchronizeCommandSets()
    {
        int maxCommands = 0;
        for (
            Iterator<TreeMap<Integer, ConcurrentTestCommand>> i =
                threadMap.values().iterator();
            i.hasNext();)
        {
            TreeMap<Integer, ConcurrentTestCommand> commands = i.next();

            // Fill in missing slots with null (no-op) commands.
            for (int j = 1; j < (commands.lastKey()).intValue(); j++) {
                Integer key = new Integer(j);
                if (!commands.containsKey(key)) {
                    commands.put(key, null);
                }
            }

            maxCommands =
                Math.max(
                    maxCommands,
                    commands.size());
        }

        // Make sure all threads have the same number of commands.
        for (
            Iterator<TreeMap<Integer, ConcurrentTestCommand>> i =
                threadMap.values().iterator();
            i.hasNext();)
        {
            TreeMap<Integer, ConcurrentTestCommand> commands = i.next();

            if (commands.size() < maxCommands) {
                for (int j = commands.size() + 1; j <= maxCommands; j++) {
                    commands.put(
                        new Integer(j),
                        null);
                }
            }
        }

        // Interleave synchronization commands before each command.
        for (
            Iterator<Map.Entry<Integer,
                TreeMap<Integer, ConcurrentTestCommand>>> i =
                threadMap.entrySet().iterator();
            i.hasNext();)
        {
            Map.Entry<Integer, TreeMap<Integer, ConcurrentTestCommand>>
                threadCommandsEntry =
                i.next();

            TreeMap<Integer, ConcurrentTestCommand> commands =
                threadCommandsEntry.getValue();

            TreeMap<Integer, ConcurrentTestCommand> synchronizedCommands =
                new TreeMap<Integer, ConcurrentTestCommand>();

            for (
                Iterator<Map.Entry<Integer, ConcurrentTestCommand>> j =
                    commands.entrySet().iterator();
                j.hasNext();)
            {
                Map.Entry<Integer, ConcurrentTestCommand> commandEntry =
                    j.next();

                int orderKey = (commandEntry.getKey()).intValue();
                ConcurrentTestCommand command = commandEntry.getValue();

                synchronizedCommands.put(
                    new Integer((orderKey * 2) - 1),
                    new AutoSynchronizationCommand());
                synchronizedCommands.put(
                    new Integer(orderKey * 2),
                    command);
            }

            threadCommandsEntry.setValue(synchronizedCommands);
        }
    }

    /**
     * Validates that all threads have the same number of
     * SynchronizationCommands (otherwise a deadlock is guaranteed).
     * @return true when valid, false when invalid.
     */
    public boolean hasValidSynchronization()
    {
        int numSyncs = -1;
        for (
            Iterator<Map.Entry<Integer,
                TreeMap<Integer, ConcurrentTestCommand>>> i =
                threadMap.entrySet().iterator();
            i.hasNext();)
        {
            Map.Entry<Integer, TreeMap<Integer, ConcurrentTestCommand>>
                threadCommandsEntry = i.next();

            TreeMap<Integer, ConcurrentTestCommand> commands =
                (TreeMap<Integer, ConcurrentTestCommand>)
                threadCommandsEntry.getValue();

            int numSyncsThisThread = 0;
            for (Iterator<ConcurrentTestCommand> j =
                commands.values().iterator(); j.hasNext();)
            {
                if (j.next() instanceof SynchronizationCommand) {
                    numSyncsThisThread++;
                }
            }
            if (numSyncs < 0) {
                numSyncs = numSyncsThisThread;
            }
            if (numSyncs != numSyncsThisThread) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a set of thread IDs.
     */
    protected Set<Integer> getThreadIds()
    {
        return threadMap.keySet();
    }

    /**
     * Retrieves the name of a given thread. If no thread names were configured,
     * returns the concatenation of "#" and the thread's numeric identifier.
     *
     * @return human-readable thread name
     */
    protected String getThreadName(Integer threadId)
    {
        if (threadNameMap.containsKey(threadId)) {
            return threadNameMap.get(threadId);
        } else {
            return "#" + threadId;
        }
    }

    /**
     * Indicates whether commands generated by this generator require special
     * handling. Default implement returns false.
     */
    boolean requiresCustomErrorHandling()
    {
        return false;
    }

    /**
     * Custom error handling occurs here if {@link
     * #requiresCustomErrorHandling()} returns true. Default implementation does
     * nothing.
     */
    void customErrorHandler(
        ConcurrentTestCommandExecutor executor)
    {
    }

    /**
     * Returns a {@link Collection} of {@link ConcurrentTestCommand}
     * objects for the given thread ID.
     */
    Collection getCommands(Integer threadId)
    {
        assert (threadMap.containsKey(threadId));

        return ((TreeMap) threadMap.get(threadId)).values();
    }

    /**
     * Returns an {@link Iterator} of {@link ConcurrentTestCommand}
     * objects for the given thread ID.
     */
    Iterator getCommandIterator(Integer threadId)
    {
        return getCommands(threadId).iterator();
    }

    /**
     * Prints a description of the commands to be executed for a given thread.
     */
    void printCommands(
        PrintStream out,
        Integer threadId)
    {
        int stepNumber = 1;
        for (Iterator i = getCommandIterator(threadId); i.hasNext();) {
            out.println(
                "\tStep " + stepNumber++ + ": "
                + i.next().getClass().getName());
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * abstract base to handle SQLExceptions
     */
    protected static abstract class AbstractCommand
        implements ConcurrentTestCommand
    {
        private boolean shouldFail = false;
        private String failComment = null; // describes an expected error
        private Pattern failPattern = null; // an expected error message
        private boolean failureExpected = false; // failure expected, no pattern

        // implement ConcurrentTestCommand
        public ConcurrentTestCommand markToFail(
            String comment,
            String pattern)
        {
            shouldFail = true;
            failComment = comment;
            failPattern = Pattern.compile(pattern);
            return this;
        }

        public boolean isFailureExpected()
        {
            return failureExpected;
        }

        public ConcurrentTestCommand markToFail()
        {
            this.failureExpected = true;
            return this;
        }

        // subclasses define this to execute themselves
        protected abstract void doExecute(
            ConcurrentTestCommandExecutor exec)
            throws Exception;

        // implement ConcurrentTestCommand
        public void execute(ConcurrentTestCommandExecutor exec)
            throws Exception
        {
            try {
                doExecute(exec);
                if (shouldFail) {
                    throw new ConcurrentTestCommand.ShouldHaveFailedException(
                        failComment);
                }
            } catch (SQLException err) {
                if (!shouldFail) {
                    throw err;
                }
                boolean matches = false;
                if (failPattern == null) {
                    matches = true; // by default
                } else {
                    for (
                        SQLException err2 = err;
                        err2 != null;
                        err2 = err2.getNextException())
                    {
                        String msg = err2.getMessage();
                        if (msg != null) {
                            matches = failPattern.matcher(msg).find();
                        }
                        if (matches) {
                            break;
                        }
                    }
                }
                if (!matches) {
                    // an unexpected error
                    throw err;
                } else {
                    // else swallow it
                    Util.swallow(err, null);
                }
            }
        }
    }

    /**
     * SynchronizationCommand causes the execution thread to wait for all other
     * threads in the test before continuing.
     */
    static class SynchronizationCommand
        extends AbstractCommand
    {
        private SynchronizationCommand()
        {
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws Exception
        {
            executor.getSynchronizer().waitForOthers();
        }
    }

    /**
     * AutoSynchronizationCommand is idential to SynchronizationCommand, except
     * that it is generated automatically by the test harness and is not counted
     * when displaying the step number in which an error occurred.
     */
    static class AutoSynchronizationCommand
        extends SynchronizationCommand
    {
        private AutoSynchronizationCommand()
        {
            super();
        }
    }

    /**
     * SleepCommand causes the execution thread to wait for all other threads in
     * the test before continuing.
     */
    private static class SleepCommand
        extends AbstractCommand
    {
        private long millis;

        private SleepCommand(long millis)
        {
            this.millis = millis;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws Exception
        {
            Thread.sleep(millis);
        }
    }

    /**
     * ExplainCommand executes explain plan commands. Automatically closes the
     * {@link Statement} before returning from {@link
     * #execute(ConcurrentTestCommandExecutor)}.
     */
    private static class ExplainCommand
        extends AbstractCommand
    {
        private String sql;

        private ExplainCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            try {
                ResultSet rset = stmt.executeQuery(sql);

                try {
                    int rowCount = 0;
                    while (rset.next()) {
                        // REVIEW: SZ 6/17/2004: Should we attempt to
                        // validate the results of the explain plan?
                        rowCount++;
                    }

                    assert (rowCount > 0);
                } finally {
                    rset.close();
                }
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * PrepareCommand creates a {@link PreparedStatement}. Stores the prepared
     * statement in the ConcurrentTestCommandExecutor.
     */
    private static class PrepareCommand
        extends AbstractCommand
    {
        private String sql;

        private PrepareCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                executor.getConnection().prepareStatement(sql);

            executor.setStatement(stmt);
        }
    }

    /**
     * CloseCommand closes a previously prepared statement. If no statement is
     * stored in the ConcurrentTestCommandExecutor, it does nothing.
     */
    private static class CloseCommand
        extends AbstractCommand
    {
        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getStatement();

            if (stmt != null) {
                stmt.close();
            }

            executor.clearStatement();
        }
    }

    private static abstract class CommandWithTimeout
        extends AbstractCommand
    {
        private int timeout;

        private CommandWithTimeout(int timeout)
        {
            this.timeout = timeout;
        }

        protected boolean setTimeout(Statement stmt)
            throws SQLException
        {
            assert (timeout >= 0);

            if (timeout > 0) {
                stmt.setQueryTimeout(timeout);
                return true;
            }

            return false;
        }
    }

    /**
     * FetchAndCompareCommand executes a previously prepared statement stored in
     * the ConcurrentTestCommandExecutor and then validates the returned
     * rows against expected data.
     */
    private static class FetchAndCompareCommand
        extends CommandWithTimeout
    {
        private List<List<Object>> expected;
        private List<List<Object>> result;

        private FetchAndCompareCommand(
            int timeout,
            String expected)
        {
            super(timeout);

            parseExpected(expected.trim());
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                (PreparedStatement) executor.getStatement();

            boolean timeoutSet = setTimeout(stmt);

            ResultSet rset = stmt.executeQuery();

            List<List<Object>> rows = new ArrayList<List<Object>>();
            try {
                int rsetColumnCount = rset.getMetaData().getColumnCount();

                while (rset.next()) {
                    List<Object> row = new ArrayList<Object>();

                    for (int i = 1; i <= rsetColumnCount; i++) {
                        Object value = rset.getObject(i);
                        if (rset.wasNull()) {
                            value = null;
                        }

                        row.add(value);
                    }

                    rows.add(row);
                }
            } catch (SqlTimeoutException e) {
                if (!timeoutSet) {
                    throw e;
                }
                Util.swallow(e, null);
            } finally {
                rset.close();
            }

            result = rows;

            testValues();
        }

        /**
         * Parses expected values. See {@link
         * ConcurrentTestCommandGenerator#addFetchAndCompareCommand(int,
         * int, int, String)} for details on format of <code>expected</code>.
         *
         * @throws IllegalStateException if there are formatting errors in
         * <code>expected</code>
         */
        private void parseExpected(String expected)
        {
            final int STATE_ROW_START = 0;
            final int STATE_VALUE_START = 1;
            final int STATE_STRING_VALUE = 2;
            final int STATE_OTHER_VALUE = 3;
            final int STATE_VALUE_END = 4;

            List<List<Object>> rows = new ArrayList<List<Object>>();
            int state = STATE_ROW_START;
            List<Object> row = null;
            StringBuilder value = new StringBuilder();

            for (int i = 0; i < expected.length(); i++) {
                char ch = expected.charAt(i);
                char nextCh =
                    (((i + 1) < expected.length()) ? expected.charAt(i + 1)
                        : 0);
                switch (state) {
                case STATE_ROW_START: // find start of row
                    if (ch == LEFT_BRACKET) {
                        row = new ArrayList<Object>();
                        state = STATE_VALUE_START;
                    }
                    break;
                case STATE_VALUE_START: // start value
                    if (!Character.isWhitespace(ch)) {
                        value.setLength(0);
                        if (ch == APOS) {
                            // a string value
                            state = STATE_STRING_VALUE;
                        } else {
                            // some other kind of value
                            value.append(ch);
                            state = STATE_OTHER_VALUE;
                        }
                    }
                    break;
                case STATE_STRING_VALUE: // handle string values
                    if (ch == APOS) {
                        if (nextCh == APOS) {
                            value.append(APOS);
                            i++;
                        } else {
                            row.add(value.toString());
                            state = STATE_VALUE_END;
                        }
                    } else {
                        value.append(ch);
                    }
                    break;
                case STATE_OTHER_VALUE: // handle other values (numeric, null)
                    if ((ch != COMMA) && (ch != RIGHT_BRACKET)) {
                        value.append(ch);
                        break;
                    }
                    String stringValue = value.toString().trim();
                    if (stringValue.matches("^-?[0-9]+$")) {
                        row.add(new BigInteger(stringValue));
                    } else if (stringValue.matches("^-?[0-9]*\\.[0-9]+$")) {
                        row.add(new BigDecimal(stringValue));
                    } else if (stringValue.equals("true")) {
                        row.add(Boolean.TRUE);
                    } else if (stringValue.equals("false")) {
                        row.add(Boolean.FALSE);
                    } else if (stringValue.equals("null")) {
                        row.add(null);
                    } else {
                        throw new IllegalStateException(
                            "unknown value type '"
                            + stringValue + "' for FetchAndCompare command");
                    }

                    state = STATE_VALUE_END;

                // FALL THROUGH
                case STATE_VALUE_END: // find comma or end of row
                    if (ch == COMMA) {
                        state = STATE_VALUE_START;
                    } else if (ch == RIGHT_BRACKET) {
                        // end of row
                        rows.add(row);
                        state = STATE_ROW_START;
                    } else if (!Character.isWhitespace(ch)) {
                        throw new IllegalStateException(
                            "unexpected character '" + ch + "' at position "
                            + i + " of expected values");
                    }
                    break;
                }
            }

            if (state != STATE_ROW_START) {
                throw new IllegalStateException(
                    "unterminated data in expected values");
            }

            if (rows.size() > 1) {
                Iterator rowIter = rows.iterator();

                int expectedNumColumns = ((ArrayList) rowIter.next()).size();

                while (rowIter.hasNext()) {
                    int numColumns = ((ArrayList) rowIter.next()).size();

                    if (numColumns != expectedNumColumns) {
                        throw new IllegalStateException(
                            "all rows in expected values must have the same number of columns");
                    }
                }
            }

            this.expected = rows;
        }

        /**
         * Validates expected data against retrieved data.
         */
        private void testValues()
        {
            if (expected.size() != result.size()) {
                dumpData(
                    "Expected " + expected.size() + " rows, got "
                    + result.size());
            }

            Iterator<List<Object>> expectedIter = expected.iterator();
            Iterator<List<Object>> resultIter = result.iterator();

            int rowNum = 1;
            while (expectedIter.hasNext() && resultIter.hasNext()) {
                List<Object> expectedRow = expectedIter.next();
                List<Object> resultRow = resultIter.next();

                testValues(expectedRow, resultRow, rowNum++);
            }
        }

        /**
         * Validates {@link ResultSet} against expected data.
         */
        private void testValues(
            List<Object> expectedRow,
            List<Object> resultRow,
            int rowNum)
        {
            if (expectedRow.size() != resultRow.size()) {
                dumpData(
                    "Row " + rowNum + " Expected " + expected.size()
                    + " columns, got " + result.size());
            }

            Iterator expectedIter = expectedRow.iterator();
            Iterator resultIter = resultRow.iterator();

            int colNum = 1;
            while (expectedIter.hasNext() && resultIter.hasNext()) {
                Object expectedValue = expectedIter.next();
                Object resultValue = resultIter.next();

                if ((expectedValue == null)
                    || (expectedValue instanceof String)
                    || (expectedValue instanceof Boolean))
                {
                    test(expectedValue, resultValue, rowNum, colNum);
                } else if (expectedValue instanceof BigInteger) {
                    BigInteger expectedInt = (BigInteger) expectedValue;

                    if (expectedInt.bitLength() <= 31) {
                        test(
                            expectedInt.intValue(),
                            ((Number) resultValue).intValue(),
                            rowNum,
                            colNum);
                    } else if (expectedInt.bitLength() <= 63) {
                        test(
                            expectedInt.longValue(),
                            ((Number) resultValue).longValue(),
                            rowNum,
                            colNum);
                    } else {
                        // REVIEW: how do we return very
                        // large integer values?
                        test(expectedInt, resultValue, rowNum, colNum);
                    }
                } else if (expectedValue instanceof BigDecimal) {
                    BigDecimal expectedReal = (BigDecimal) expectedValue;

                    float asFloat = expectedReal.floatValue();
                    double asDouble = expectedReal.doubleValue();

                    if ((asFloat != Float.POSITIVE_INFINITY)
                        && (asFloat != Float.NEGATIVE_INFINITY))
                    {
                        test(
                            asFloat,
                            ((Number) resultValue).floatValue(),
                            rowNum,
                            colNum);
                    } else if (
                        (asDouble != Double.POSITIVE_INFINITY)
                        && (asDouble != Double.NEGATIVE_INFINITY))
                    {
                        test(
                            asDouble,
                            ((Number) resultValue).doubleValue(),
                            rowNum,
                            colNum);
                    } else {
                        // REVIEW: how do we return very large decimal
                        // values?
                        test(expectedReal, resultValue, rowNum, colNum);
                    }
                } else {
                    throw new IllegalStateException(
                        "unknown type of expected value: "
                        + expectedValue.getClass().getName());
                }

                colNum++;
            }
        }

        private void test(
            Object expected,
            Object got,
            int rowNum,
            int colNum)
        {
            if ((expected == null) && (got == null)) {
                return;
            }

            if ((expected == null) || !expected.equals(got)) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            int expected,
            int got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            long expected,
            long got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            float expected,
            float got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            double expected,
            double got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void reportError(
            String expected,
            String got,
            int rowNum,
            int colNum)
        {
            dumpData(
                "Row " + rowNum + ", column " + colNum + ": expected <"
                + expected + ">, got <" + got + ">");
        }

        /**
         * Outputs expected and result data in tabular format.
         */
        private void dumpData(String message)
        {
            Iterator<List<Object>> expectedIter = expected.iterator();
            Iterator<List<Object>> resultIter = result.iterator();

            StringBuilder fullMessage = new StringBuilder(message);

            int rowNum = 1;
            while (expectedIter.hasNext() || resultIter.hasNext()) {
                StringBuilder expectedOut = new StringBuilder();
                expectedOut.append("Row ").append(rowNum).append(" exp:");

                StringBuilder resultOut = new StringBuilder();
                resultOut.append("Row ").append(rowNum).append(" got:");

                Iterator<Object> expectedRowIter = null;
                if (expectedIter.hasNext()) {
                    List<Object> expectedRow = expectedIter.next();
                    expectedRowIter = expectedRow.iterator();
                }

                Iterator<Object> resultRowIter = null;
                if (resultIter.hasNext()) {
                    List<Object> resultRow = resultIter.next();
                    resultRowIter = resultRow.iterator();
                }

                while (
                    ((expectedRowIter != null) && expectedRowIter.hasNext())
                    || ((resultRowIter != null) && resultRowIter.hasNext()))
                {
                    Object expectedObject =
                        ((expectedRowIter != null) ? expectedRowIter.next()
                            : "");

                    Object resultObject =
                        ((resultRowIter != null) ? resultRowIter.next() : "");

                    String expectedValue;
                    if (expectedObject == null) {
                        expectedValue = "<null>";
                    } else {
                        expectedValue = expectedObject.toString();
                    }

                    String resultValue;
                    if (resultObject == null) {
                        resultValue = "<null>";
                    } else {
                        resultValue = resultObject.toString();
                    }

                    int width =
                        Math.max(
                            expectedValue.length(),
                            resultValue.length());

                    expectedOut.append(" | ").append(expectedValue);
                    for (int i = 0; i < (width - expectedValue.length()); i++) {
                        expectedOut.append(' ');
                    }

                    resultOut.append(" | ").append(resultValue);
                    for (int i = 0; i < (width - resultValue.length()); i++) {
                        resultOut.append(' ');
                    }
                }

                if ((expectedRowIter == null) && (resultRowIter == null)) {
                    expectedOut.append('|');
                    resultOut.append('|');
                }

                expectedOut.append(" |");
                resultOut.append(" |");

                fullMessage.append('\n').append(expectedOut.toString()).append(
                    '\n').append(resultOut.toString());

                rowNum++;
            }

            throw new RuntimeException(fullMessage.toString());
        }
    }

    /**
     * InsertCommand exeutes an insert, update or delete SQL statement. Uses
     * {@link Statement#executeUpdate(String)}.
     */
    private static class InsertCommand
        extends CommandWithTimeout
    {
        private String sql;

        private InsertCommand(
            int timeout,
            String sql)
        {
            super(timeout);

            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            setTimeout(stmt);

            stmt.executeUpdate(sql);
        }
    }

    /**
     * CommitCommand commits pending transactions via {@link
     * Connection#commit()}.
     */
    private static class CommitCommand
        extends AbstractCommand
    {
        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().commit();
        }
    }

    /**
     * RollbackCommand rolls back pending transactions via {@link
     * Connection#rollback()}.
     */
    private static class RollbackCommand
        extends AbstractCommand
    {
        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().rollback();
        }
    }

    /**
     * DdlCommand executes DDL commands. Automatically closes the {@link
     * Statement} before returning from {@link
     * #doExecute(ConcurrentTestCommandExecutor)}.
     */
    private static class DdlCommand
        extends AbstractCommand
    {
        private String sql;

        private DdlCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            try {
                stmt.execute(sql);
            } finally {
                stmt.close();
            }
        }
    }
}

// End ConcurrentTestCommandGenerator.java
