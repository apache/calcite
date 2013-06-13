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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.eigenbase.util.Util;

import net.hydromatic.optiq.jdbc.SqlTimeoutException;


/**
 * ConcurrentTestCommandScript creates instances of {@link
 * ConcurrentTestCommand} that perform specific actions in a specific
 * order and within the context of a test thread ({@link
 * ConcurrentTestCommandExecutor}).
 *
 * <p>Actions are loaded from a script (see package javadoc for script format).
 *
 * <p>A single ConcurrentTestCommandScript creates commands
 * for multiple threads. Each thread is represented by an integer "thread ID"
 * and, optionally, a String thread name. Thread IDs may take on any positive
 * integer value and may be a sparse set (e.g. 1, 2, 5). Thread names may be any
 * String.
 *
 * <p>When each command is created, it is associated with a thread and given an
 * execution order. Execution order values are positive integers, must be unique
 * within a thread, and may be a sparse set.
 * See {@link ConcurrentTestCommandGenerator#synchronizeCommandSets} for other
 * considerations.
 */
public class ConcurrentTestCommandScript
    extends ConcurrentTestCommandGenerator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String PRE_SETUP_STATE = "pre-setup";
    private static final String SETUP_STATE = "setup";
    private static final String POST_SETUP_STATE = "post-setup";
    private static final String CLEANUP_STATE = "cleanup";
    private static final String POST_CLEANUP_STATE = "post-cleanup";
    private static final String THREAD_STATE = "thread";
    private static final String REPEAT_STATE = "repeat";
    private static final String SQL_STATE = "sql";
    private static final String POST_THREAD_STATE = "post-thread";
    private static final String EOF_STATE = "eof";

    private static final String VAR = "@var";
    private static final String LOCKSTEP = "@lockstep";
    private static final String NOLOCKSTEP = "@nolockstep";
    private static final String ENABLED = "@enabled";
    private static final String DISABLED = "@disabled";
    private static final String SETUP = "@setup";
    private static final String CLEANUP = "@cleanup";
    private static final String END = "@end";
    private static final String THREAD = "@thread";
    private static final String REPEAT = "@repeat";
    private static final String SYNC = "@sync";
    private static final String TIMEOUT = "@timeout";
    private static final String ROWLIMIT = "@rowlimit";
    private static final String PREPARE = "@prepare";
    private static final String PRINT = "@print";
    private static final String FETCH = "@fetch";
    private static final String CLOSE = "@close";
    private static final String SLEEP = "@sleep";
    private static final String ERR = "@err";
    private static final String ECHO = "@echo";
    private static final String INCLUDE = "@include";
    private static final String SHELL = "@shell";
    private static final String PLUGIN = "@plugin";

    private static final String SQL = "";
    private static final String EOF = null;

    private static final StateAction [] STATE_TABLE =
    {
        new StateAction(
            PRE_SETUP_STATE,
            new StateDatum[] {
                new StateDatum(VAR, PRE_SETUP_STATE),
                new StateDatum(LOCKSTEP, PRE_SETUP_STATE),
                new StateDatum(NOLOCKSTEP, PRE_SETUP_STATE),
                new StateDatum(ENABLED, PRE_SETUP_STATE),
                new StateDatum(DISABLED, PRE_SETUP_STATE),
                new StateDatum(PLUGIN, PRE_SETUP_STATE),
                new StateDatum(SETUP, SETUP_STATE),
                new StateDatum(CLEANUP, CLEANUP_STATE),
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            SETUP_STATE,
            new StateDatum[] {
                new StateDatum(END, POST_SETUP_STATE),
                new StateDatum(SQL, SETUP_STATE),
                new StateDatum(INCLUDE, SETUP_STATE),
            }),

        new StateAction(
            POST_SETUP_STATE,
            new StateDatum[] {
                new StateDatum(CLEANUP, CLEANUP_STATE),
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            CLEANUP_STATE,
            new StateDatum[] {
                new StateDatum(END, POST_CLEANUP_STATE),
                new StateDatum(SQL, CLEANUP_STATE),
                new StateDatum(INCLUDE, CLEANUP_STATE),
            }),

        new StateAction(
            POST_CLEANUP_STATE,
            new StateDatum[] {
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            THREAD_STATE,
            new StateDatum[] {
                new StateDatum(REPEAT, REPEAT_STATE),
                new StateDatum(SYNC, THREAD_STATE),
                new StateDatum(TIMEOUT, THREAD_STATE),
                new StateDatum(ROWLIMIT, THREAD_STATE),
                new StateDatum(PREPARE, THREAD_STATE),
                new StateDatum(PRINT, THREAD_STATE),
                new StateDatum(FETCH, THREAD_STATE),
                new StateDatum(CLOSE, THREAD_STATE),
                new StateDatum(SLEEP, THREAD_STATE),
                new StateDatum(SQL, THREAD_STATE),
                new StateDatum(ECHO, THREAD_STATE),
                new StateDatum(ERR, THREAD_STATE),
                new StateDatum(SHELL, THREAD_STATE),
                new StateDatum(END, POST_THREAD_STATE)
            }),

        new StateAction(
            REPEAT_STATE,
            new StateDatum[] {
                new StateDatum(SYNC, REPEAT_STATE),
                new StateDatum(TIMEOUT, REPEAT_STATE),
                new StateDatum(ROWLIMIT, REPEAT_STATE),
                new StateDatum(PREPARE, REPEAT_STATE),
                new StateDatum(PRINT, REPEAT_STATE),
                new StateDatum(FETCH, REPEAT_STATE),
                new StateDatum(CLOSE, REPEAT_STATE),
                new StateDatum(SLEEP, REPEAT_STATE),
                new StateDatum(SQL, REPEAT_STATE),
                new StateDatum(ECHO, REPEAT_STATE),
                new StateDatum(ERR, REPEAT_STATE),
                new StateDatum(SHELL, REPEAT_STATE),
                new StateDatum(END, THREAD_STATE)
            }),

        new StateAction(
            POST_THREAD_STATE,
            new StateDatum[] {
                new StateDatum(THREAD, THREAD_STATE),
                new StateDatum(EOF, EOF_STATE)
            })
    };

    private static final int FETCH_LEN = FETCH.length();
    private static final int PREPARE_LEN = PREPARE.length();
    private static final int PRINT_LEN = PRINT.length();
    private static final int REPEAT_LEN = REPEAT.length();
    private static final int SLEEP_LEN = SLEEP.length();
    private static final int THREAD_LEN = THREAD.length();
    private static final int TIMEOUT_LEN = TIMEOUT.length();
    private static final int ROWLIMIT_LEN = ROWLIMIT.length();
    private static final int ERR_LEN = ERR.length();
    private static final int ECHO_LEN = ECHO.length();
    private static final int SHELL_LEN = SHELL.length();
    private static final int PLUGIN_LEN = PLUGIN.length();
    private static final int INCLUDE_LEN = INCLUDE.length();
    private static final int VAR_LEN = VAR.length();

    private static final char [] spaces;
    private static final char [] dashes;

    private static final int BUF_SIZE = 1024;
    private static final int REPEAT_READ_AHEAD_LIMIT = 65536;

    static {
        spaces = new char[BUF_SIZE];
        dashes = new char[BUF_SIZE];

        for (int i = 0; i < BUF_SIZE; i++) {
            spaces[i] = ' ';
            dashes[i] = '-';
        }
    }

    // Special "thread ids" for setup & cleanup sections; actually setup &
    // cleanup SQL is executed by the main thread, and neither are in the the
    // thread map.
    private static final Integer SETUP_THREAD_ID = -1;
    private static final Integer CLEANUP_THREAD_ID = -2;

    //~ Instance fields (representing a single script):

    private boolean quiet = false;
    private boolean verbose = false;
    private Boolean lockstep;
    private Boolean disabled;
    private VariableTable vars = new VariableTable();
    private File scriptDirectory;
    private long scriptStartTime = 0;

    private final List<ConcurrentTestPlugin> plugins =
        new ArrayList<ConcurrentTestPlugin>();
    private final Map<String, ConcurrentTestPlugin> pluginForCommand =
        new HashMap<String, ConcurrentTestPlugin>();
    private final Map<String, ConcurrentTestPlugin> preSetupPluginForCommand =
        new HashMap<String, ConcurrentTestPlugin>();
    private List<String> setupCommands = new ArrayList<String>();
    private List<String> cleanupCommands = new ArrayList<String>();

    private Map<Integer, BufferedWriter> threadBufferedWriters =
        new HashMap<Integer, BufferedWriter>();
    private Map<Integer, StringWriter> threadStringWriters =
        new HashMap<Integer, StringWriter>();
    private Map<Integer, ResultsReader> threadResultsReaders =
        new HashMap<Integer, ResultsReader>();

    //~ Constructors -----------------------------------------------------------

    public ConcurrentTestCommandScript() throws IOException
    {
        super();
    }

    /**
     * Constructs and prepares a new ConcurrentTestCommandScript.
     */
    public ConcurrentTestCommandScript(String filename)
        throws IOException
    {
        this();
        prepare(filename,  null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Gets ready to execute: loads script FILENAME applying external variable
     * BINDINGS
     */
    private void prepare(String filename, List<String> bindings)
        throws IOException
    {
        vars = new VariableTable();
        CommandParser parser = new CommandParser();
        parser.rememberVariableRebindings(bindings);
        parser.load(filename);

        for (Integer threadId : getThreadIds()) {
            addThreadWriters(threadId);
        }

        // Backwards compatible: printed results always has a setup section, but
        // cleanup section is optional:
        setThreadName(SETUP_THREAD_ID, "setup");
        addThreadWriters(SETUP_THREAD_ID);
        if (!cleanupCommands.isEmpty()) {
            setThreadName(CLEANUP_THREAD_ID, "cleanup");
            addThreadWriters(CLEANUP_THREAD_ID);
        }
    }

    /** Executes the script */
    public void execute() throws Exception
    {
        scriptStartTime = System.currentTimeMillis();
        executeSetup();
        ConcurrentTestCommandExecutor threads[] = innerExecute();
        executeCleanup();
        postExecute(threads);
    }

    private void addThreadWriters(Integer threadId)
    {
        StringWriter w = new StringWriter();
        BufferedWriter bw = new BufferedWriter(w);
        threadStringWriters.put(threadId, w);
        threadBufferedWriters.put(threadId, bw);
        threadResultsReaders.put(threadId, new ResultsReader(bw));
    }

    public void setQuiet(boolean val)
    {
        quiet = val;
    }

    public void setVerbose(boolean val)
    {
        verbose = val;
    }


    public boolean useLockstep()
    {
        if (lockstep == null) {
            return false;
        }

        return lockstep.booleanValue();
    }

    public boolean isDisabled()
    {
        for (ConcurrentTestPlugin plugin : plugins) {
            if (plugin.isTestDisabled()) {
                return true;
            }
        }

        if (disabled == null) {
            return false;
        }

        return disabled.booleanValue();
    }

    public void executeSetup() throws Exception
    {
        executeCommands(SETUP_THREAD_ID, setupCommands);
    }

    public void executeCleanup() throws Exception
    {
        executeCommands(CLEANUP_THREAD_ID, cleanupCommands);
    }

    protected void executeCommands(int threadID, List<String> commands)
        throws Exception
    {
        if ((commands == null) || (commands.size() == 0)) {
            return;
        }

        Connection connection = DriverManager.getConnection(jdbcURL, jdbcProps);
        if (connection.getMetaData().supportsTransactions()) {
            connection.setAutoCommit(false);
        }

        boolean forced = false;         // flag, keep going after an error
        try {
            for (String command : commands) {
                String sql = (command).trim();
                storeSql(threadID, sql);

                if (isComment(sql)) {
                    continue;
                }

                // handle sqlline-type directives:
                if (sql.startsWith("!set")) {
                    String[] tokens = sql.split(" +");
                    // handle only SET FORCE
                    if ((tokens.length > 2)
                        && tokens[1].equalsIgnoreCase("force"))
                        {
                            forced = asBoolValue(tokens[2]);
                        }
                    continue;           // else ignore
                } else if (sql.startsWith("!")) {
                    continue;           // else ignore
                }

                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1);
                }

                if (isSelect(sql)) {
                    Statement stmt = connection.createStatement();
                    try {
                        ResultSet rset = stmt.executeQuery(sql);
                        storeResults(threadID, rset, -1);
                    } finally {
                        stmt.close();
                    }
                } else if (sql.equalsIgnoreCase("commit")) {
                    connection.commit();
                } else if (sql.equalsIgnoreCase("rollback")) {
                    connection.rollback();
                } else {
                    Statement stmt = connection.createStatement();
                    try {
                        int rows = stmt.executeUpdate(sql);
                        if (rows != 1) {
                            storeMessage(
                                threadID,
                                String.valueOf(rows)
                                    + " rows affected.");
                        } else {
                            storeMessage(threadID, "1 row affected.");
                        }
                    } catch (SQLException ex) {
                        if (forced) {
                            storeMessage(threadID, ex.getMessage()); // swallow
                        } else {
                            throw ex;
                        }
                    } finally {
                        stmt.close();
                    }
                }
            }
        } finally {
            if (connection.getMetaData().supportsTransactions()) {
                connection.rollback();
            }
            connection.close();
        }
    }

    // timeout < 0 means no timeout
    private void storeResults(Integer threadId, ResultSet rset, long timeout)
        throws SQLException
    {
        ResultsReader r = threadResultsReaders.get(threadId);
        r.read(rset, timeout);
    }

    /** Identifies the start of a comment line; same rules as sqlline */
    private boolean isComment(String line)
    {
        return line.startsWith("--") || line.startsWith("#");
    }

    /** translates argument of !set force etc. */
    private boolean asBoolValue(String s)
    {
        s = s.toLowerCase();
        return s.equals("true") || s.equals("yes") || s.equals("on");
    }

    /**
     * Determines if a block of SQL is a select statment or not.
     */
    private boolean isSelect(String sql)
    {
        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while ((line = rdr.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (isComment(line)) {
                    continue;
                }
                if (line.startsWith("select")
                    || line.startsWith("values")
                    || line.startsWith("explain"))
                {
                    return true;
                } else {
                    return false;
                }
            }
        } catch (IOException e) {
            assert (false) : "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch (IOException e) {
                assert (false) : "IOException via StringReader";
            }
        }

        return false;
    }

    /**
     * Builds a map of thread ids to result data for the thread. Each result
     * datum is an <code>String[2]</code> containing the thread name and the
     * thread's output.
     * @return the map.
     */
    private Map<Integer, String[]> collectResults()
    {
        TreeMap<Integer, String[]> results = new TreeMap<Integer, String[]>();

        // get all normal threads
        TreeSet<Integer> threadIds = new TreeSet<Integer>(getThreadIds());
        // add the "special threads"
        threadIds.add(SETUP_THREAD_ID);
        threadIds.add(CLEANUP_THREAD_ID);

        for (Integer threadId : threadIds) {
            try {
                BufferedWriter bout = threadBufferedWriters.get(threadId);
                if (bout != null) {
                    bout.flush();
                }
            } catch (IOException e) {
                assert (false) : "IOException via StringWriter";
            }
            String threadName = getFormattedThreadName(threadId);
            StringWriter out = threadStringWriters.get(threadId);
            if (out == null) {
                continue;
            }
            results.put(
                threadId,
                new String[]{threadName, out.toString()});
        }
        return results;
    }

    // solely for backwards-compatible output
    private String getFormattedThreadName(Integer id)
    {
        if (id < 0) {                   // special thread
            return getThreadName(id);
        } else {                        // normal thread
            return "thread " + getThreadName(id);
        }
    }

    public void printResults(BufferedWriter out) throws IOException
    {
        final Map<Integer, String[]> results = collectResults();
        if (verbose) {
            out.write(
                String.format(
                    "script execution started at %tc (%d)%n",
                    new Timestamp(scriptStartTime), scriptStartTime));
        }
        printThreadResults(out, results.get(SETUP_THREAD_ID));
        for (Integer id : results.keySet()) {
            if (id < 0) {
                continue;               // special thread
            }
            printThreadResults(out, results.get(id)); // normal thread
        }
        printThreadResults(out, results.get(CLEANUP_THREAD_ID));
    }

    private void printThreadResults(BufferedWriter out, String[] threadResult)
        throws IOException
    {
        if (threadResult == null) {
            return;
        }
        String threadName = threadResult[0];
        out.write("-- " + threadName);
        out.newLine();
        out.write(threadResult[1]);
        out.write("-- end of " + threadName);
        out.newLine();
        out.newLine();
        out.flush();
    }

    /**
     * Causes errors to be send here for custom handling. See {@link
     * #customErrorHandler(ConcurrentTestCommandExecutor)}.
     */
    boolean requiresCustomErrorHandling()
    {
        return true;
    }

    void customErrorHandler(
        ConcurrentTestCommandExecutor executor)
    {
        StringBuilder message = new StringBuilder();
        Throwable cause = executor.getFailureCause();
        ConcurrentTestCommand command = executor.getFailureCommand();

        if ((command == null) || !command.isFailureExpected()) {
            message.append(cause.getMessage());
            StackTraceElement [] trace = cause.getStackTrace();
            for (StackTraceElement aTrace : trace) {
                message.append("\n\t").append(aTrace.toString());
            }
        } else {
            message.append(cause.getClass().getName())
                .append(": ")
                .append(cause.getMessage());
        }

        storeMessage(
            executor.getThreadId(),
            message.toString());
    }

    /**
     * Retrieves the output stream for the given thread id.
     *
     * @return a BufferedWriter on a StringWriter for the thread.
     */
    private BufferedWriter getThreadWriter(Integer threadId)
    {
        assert (threadBufferedWriters.containsKey(threadId));
        return threadBufferedWriters.get(threadId);
    }


    /**
     * Saves a SQL command to be printed with the thread's output.
     */
    private void storeSql(Integer threadId, String sql)
    {
        StringBuilder message = new StringBuilder();

        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while ((line = rdr.readLine()) != null) {
                line = line.trim();

                if (message.length() > 0) {
                    message.append('\n');
                }

                message.append("> ").append(line);
            }
        } catch (IOException e) {
            assert (false) : "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch (IOException e) {
                assert (false) : "IOException via StringReader";
            }
        }

        storeMessage(
            threadId,
            message.toString());
    }

    /**
     * Saves a message to be printed with the thread's output.
     */
    private void storeMessage(Integer threadId, String message)
    {
        BufferedWriter out = getThreadWriter(threadId);
        try {
            if (verbose) {
                long t = System.currentTimeMillis() - scriptStartTime;
                out.write("at " + t + ": ");
            }
            out.write(message);
            out.newLine();
        } catch (IOException e) {
            assert (false) : "IOException on StringWriter";
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class StateAction
    {
        final String state;
        final StateDatum [] stateData;

        StateAction(String state, StateDatum [] stateData)
        {
            this.state = state;
            this.stateData = stateData;
        }
    }

    private static class StateDatum
    {
        final String x;
        final String y;

        StateDatum(String x, String y)
        {
            this.x = x;
            this.y = y;
        }
    }


    // Inner Class: a symbol table of script variables

    private class VariableTable {
        private final Map<String, String> map;

        // matches $$, $var, ${var}
        private final Pattern symbolPattern =
            Pattern.compile("\\$((\\$)|([A-Za-z]\\w*)|\\{([A-Za-z]\\w*)\\})");

        public VariableTable() {
            map = new HashMap<String, String>();
        }

        public class Excn extends IllegalArgumentException {
            public Excn(String msg) {
                super(msg);
            }
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public boolean isDefined(String sym) {
            return map.containsKey(sym);
        }

        // a symbol must be explicitly defined before it can be set or read.
        public void define(String sym, String val) throws Excn
        {
            if (isDefined(sym)) {
                throw new Excn("second declaration of variable " + sym);
            }
            // convert a null val to a null string
            map.put(sym, (val == null ? "" : val));
        }

        // returns null is SYM is not defined
        public String get(String sym) {
            if (isDefined(sym)) {
                return map.get(sym);
            } else {
                return null;
            }
        }

        public void set(String sym, String val) throws Excn
        {
            if (isDefined(sym)) {
                map.put(sym, val);
                return;
            }
            throw new Excn("undeclared variable " + sym);
        }

        public String expand(String in) {
            if (in.contains("$")) {
                StringBuffer out = new StringBuffer();
                Matcher matcher = symbolPattern.matcher(in);
                int lastEnd = 0;
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();
                    String val = null;
                    if (null != matcher.group(2)) {
                        val = "$";          // matched $$
                    } else {
                        String var = matcher.group(3); // matched $var
                        if (var == null) {
                            var = matcher.group(4); // matched ${var}
                        }
                        if (map.containsKey(var)) {
                            val = map.get(var);
                            val = expand(val);
                        } else {
                            // not our var, so can't expand
                            val = matcher.group(0);
                        }
                    }
                    out.append(in.substring(lastEnd, start));
                    out.append(val);
                    lastEnd = end;
                }
                out.append(in.substring(lastEnd));
                return out.toString();
            } else {
                return in;
            }
        }
    }


    // Inner Class: the command parser

    private class CommandParser {
        final Pattern splitWords = Pattern.compile("\\s+");
        final Pattern splitBinding = Pattern.compile("=");
        final Pattern matchesVarDefn =
            Pattern.compile("([A-Za-z]\\w*) *=(.*)$");
        // \1 is VAR, \2 is VAL

        // parser state
        private String state;
        private int threadId;
        private int nextThreadId;
        private int order;
        private int repeatCount;
        private boolean scriptHasVars;
        private Stack<File> currentDirectory = new Stack<File>();

        private class Binding {
            public final String var;
            public final String val;
            public Binding(String var, String val) {
                this.var = var;
                this.val = val;
            }
            // @param phrase has form VAR=VAL
            public Binding(String phrase) {
                String[] parts = splitBinding.split(phrase);
                assert parts.length == 2;
                this.var = parts[0];
                this.val = parts[1];
            }
        }

        // A list of Bindings that must be applied immediately after parsing
        // last @var.
        private List<Binding> deferredBindings = new ArrayList<Binding>();

        public CommandParser() {
            state  = PRE_SETUP_STATE;
            threadId =  nextThreadId = 1;
            order = 1;
            repeatCount = 0;
            scriptHasVars = false;
            currentDirectory.push(null);
        }

        // Parses a set of VAR=VAL pairs from the command line, and saves it for
        // later application.
        public void rememberVariableRebindings(List<String> pairs)
        {
            if (pairs == null) {
                return;
            }
            for (String pair : pairs) {
                deferredBindings.add(new Binding(pair));
            }
        }

        // to call after all @var commands but before any SQL.
        private void applyVariableRebindings()
        {
            for (Binding binding : deferredBindings) {
                vars.set(binding.var, binding.val);
            }
        }

        // trace loading of a script
        private void trace(String prefix, Object message)
        {
            if (verbose && !quiet) {
                if (prefix != null) {
                    System.out.print(prefix + ": ");
                }
                System.out.println(message);
            }
        }

        private void trace(String message)
        {
            trace(null, message);
        }

        /**
         * Parses a multi-threaded script and converts it into test commands.
         */
        private void load(String scriptFileName) throws IOException {
            File scriptFile = new File(currentDirectory.peek(), scriptFileName);
            currentDirectory.push(scriptDirectory = scriptFile.getParentFile());
            BufferedReader in = new BufferedReader(new FileReader(scriptFile));
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    Map<String, String> commandStateMap = lookupState(state);
                    String command = null;
                    boolean isSql = false;
                    if (line.equals("") || line.startsWith("--")) {
                        continue;
                    } else if (line.startsWith("@")) {
                        command = firstWord(line);
                    } else {
                        isSql = true;
                        command = SQL;
                    }
                    if (!commandStateMap.containsKey(command)) {
                        throw new IllegalStateException(
                            command + " not allowed in state " + state);
                    }

                    boolean changeState;
                    if (isSql) {
                        String sql = readSql(line, in);
                        loadSql(sql);
                        changeState = true;
                    } else {
                        changeState = loadCommand(command, line, in);
                    }
                    if (changeState) {
                        String nextState = commandStateMap.get(command);
                        assert (nextState != null);
                        if (! nextState.equals(state)) {
                            doEndOfState(state);
                        }
                        state = nextState;
                    }
                }

                // at EOF
                currentDirectory.pop();
                if (currentDirectory.size() == 1) {
                    // at top EOF
                    if (!lookupState(state).containsKey(EOF)) {
                        throw new IllegalStateException(
                            "Premature end of file in '" + state + "' state");
                    }
                }
            } finally {
                in.close();
            }
        }

        private void loadSql(String sql) {
            if (SETUP_STATE.equals(state)) {
                trace("@setup", sql);
                setupCommands.add(sql);
            } else if (CLEANUP_STATE.equals(state)) {
                trace("@cleanup", sql);
                cleanupCommands.add(sql);
            } else if (
                THREAD_STATE.equals(state) || REPEAT_STATE.equals(state))
            {
                boolean isSelect = isSelect(sql);
                trace(sql);
                for (int i = threadId; i < nextThreadId; i++) {
                    CommandWithTimeout cmd = isSelect?
                        new SelectCommand(sql) : new SqlCommand(sql);
                    addCommand(i, order, cmd);
                }
                order++;
            } else {
                assert (false);
            }
        }

        // returns TRUE when load-state should advance, FALSE when it must not.
        private boolean loadCommand(
            String command, String line, BufferedReader in)
            throws IOException
        {
            if (VAR.equals(command)) {
                String args = line.substring(VAR_LEN).trim();
                scriptHasVars = true;
                trace("@var",  args);
                defineVariables(args);

            } else if (LOCKSTEP.equals(command)) {
                assert (lockstep == null)
                    : LOCKSTEP + " and " + NOLOCKSTEP + " may only appear once";
                lockstep = Boolean.TRUE;
                trace("lockstep");

            } else if (NOLOCKSTEP.equals(command)) {
                assert (lockstep == null)
                    : LOCKSTEP + " and " + NOLOCKSTEP + " may only appear once";
                lockstep = Boolean.FALSE;
                trace("no lockstep");

            } else if (DISABLED.equals(command)) {
                assert (disabled == null)
                    : DISABLED + " and " + ENABLED + " may only appear once";
                disabled = Boolean.TRUE;
                trace("disabled");

            } else if (ENABLED.equals(command)) {
                assert (disabled == null)
                    : DISABLED + " and " + ENABLED + " may only appear once";
                disabled = Boolean.FALSE;
                trace("enabled");

            } else if (SETUP.equals(command)) {
                trace("@setup");

            } else if (CLEANUP.equals(command)) {
                trace("@cleanup");

            } else if (INCLUDE.equals(command)) {
                String includedFile =
                    vars.expand(line.substring(INCLUDE_LEN).trim());
                trace("@include", includedFile);
                load(includedFile);
                trace("end @include", includedFile);

            } else if (THREAD.equals(command)) {
                String threadNamesStr = line.substring(THREAD_LEN).trim();
                trace("@thread", threadNamesStr);
                StringTokenizer threadNamesTok =
                    new StringTokenizer(threadNamesStr, ",");
                while (threadNamesTok.hasMoreTokens()) {
                    setThreadName(
                        nextThreadId++,
                        threadNamesTok.nextToken());
                }

            } else if (REPEAT.equals(command)) {
                String arg = line.substring(REPEAT_LEN).trim();
                repeatCount = Integer.parseInt(vars.expand(arg));
                trace("start @repeat block", repeatCount);
                assert (repeatCount > 0) : "Repeat count must be > 0";
                in.mark(REPEAT_READ_AHEAD_LIMIT);

            } else if (END.equals(command)) {
                if (SETUP_STATE.equals(state)) {
                    trace("end @setup");
                } else if (CLEANUP_STATE.equals(state)) {
                    trace("end @cleanup");
                } else if (THREAD_STATE.equals(state)) {
                    threadId = nextThreadId;
                } else if (REPEAT_STATE.equals(state)) {
                    trace("repeating");
                    repeatCount--;
                    if (repeatCount > 0) {
                        try {
                            in.reset();
                        } catch (IOException e) {
                            throw new IllegalStateException(
                                "Unable to reset reader -- repeat "
                                + "contents must be less than "
                                + REPEAT_READ_AHEAD_LIMIT + " bytes");
                        }

                        trace("end @repeat block");
                        return false;   // don't change the state
                    }
                } else {
                    assert (false);
                }

            } else if (SYNC.equals(command)) {
                trace("@sync");
                for (int i = threadId; i < nextThreadId; i++) {
                    addSynchronizationCommand(i, order);
                }
                order++;

            } else if (TIMEOUT.equals(command)) {
                String args = line.substring(TIMEOUT_LEN).trim();
                String millisStr = vars.expand(firstWord(args));
                long millis = Long.parseLong(millisStr);
                assert (millis >= 0L) : "Timeout must be >= 0";

                String sql = readSql(skipFirstWord(args).trim(), in);
                trace("@timeout", sql);
                boolean isSelect = isSelect(sql);
                for (int i = threadId; i < nextThreadId; i++) {
                    CommandWithTimeout cmd =
                        isSelect ? new SelectCommand(sql, millis)
                        : new SqlCommand(sql, millis);
                    addCommand(i, order, cmd);
                }
                order++;

            } else if (ROWLIMIT.equals(command)) {
                String args = line.substring(ROWLIMIT_LEN).trim();
                String limitStr = vars.expand(firstWord(args));
                int limit = Integer.parseInt(limitStr);
                assert (limit >= 0) : "Rowlimit must be >= 0";

                String sql = readSql(skipFirstWord(args).trim(), in);
                trace("@rowlimit ", sql);
                boolean isSelect = isSelect(sql);
                if (!isSelect) {
                    throw new IllegalStateException(
                        "Only select can be used with rowlimit");
                }
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(i, order, new SelectCommand(sql, 0, limit));
                }
                order++;

            } else if (PRINT.equals(command)) {
                String spec = vars.expand(line.substring(PRINT_LEN).trim());
                trace("@print", spec);
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(i, order, new PrintCommand(spec));
                }
                order++;

            } else if (PREPARE.equals(command)) {
                String startOfSql =
                    line.substring(PREPARE_LEN).trim();
                String sql = readSql(startOfSql, in);
                trace("@prepare", sql);
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(i, order, new PrepareCommand(sql));
                }
                order++;

            } else if (PLUGIN.equals(command)) {
                String cmd = line.substring(PLUGIN_LEN).trim();
                String pluginName = readLine(cmd, in).trim();
                trace("@plugin", pluginName);
                plugin(pluginName);

            } else if (pluginForCommand.containsKey(command)) {
                String cmd = line.substring(command.length())
                    .trim();
                cmd = readLine(cmd, in);
                trace("@" + command, cmd);
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(
                        i,
                        order,
                        new PluginCommand(
                            command, cmd));
                }
                order++;

            } else if (preSetupPluginForCommand.containsKey(command)) {
                String cmd = line.substring(command.length()) .trim();
                cmd = readLine(cmd, in);
                trace("@" + command, cmd);
                ConcurrentTestPlugin plugin =
                    preSetupPluginForCommand.get(command);
                plugin.preSetupFor(command, cmd);


            } else if (SHELL.equals(command)) {
                String cmd = line.substring(SHELL_LEN).trim();
                cmd = readLine(cmd, in);
                trace("@shell", cmd);
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(i, order, new ShellCommand(cmd));
                }
                order++;

            } else if (ECHO.equals(command)) {
                String msg = line.substring(ECHO_LEN).trim();
                msg = readLine(msg, in);
                trace("@echo", msg);
                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(i, order, new EchoCommand(msg));
                }
                order++;

            } else if (ERR.equals(command)) {
                String startOfSql =
                    line.substring(ERR_LEN).trim();
                String sql = readSql(startOfSql, in);
                trace("@err ", sql);
                boolean isSelect = isSelect(sql);
                for (int i = threadId; i < nextThreadId; i++) {
                    CommandWithTimeout cmd =
                        isSelect ? new SelectCommand(sql, true)
                        : new SqlCommand(sql, true);
                    addCommand(i, order, cmd);
                }
                order++;

            } else if (FETCH.equals(command)) {
                String arg = vars.expand(line.substring(FETCH_LEN).trim());
                trace("@fetch", arg);
                long millis = 0L;
                if (arg.length() > 0) {
                    millis = Long.parseLong(arg);
                    assert (millis >= 0L) : "Fetch timeout must be >= 0";
                }

                for (int i = threadId; i < nextThreadId; i++) {
                    addCommand(
                        i,
                        order,
                        new FetchAndPrintCommand(millis));
                }
                order++;

            } else if (CLOSE.equals(command)) {
                trace("@close");
                for (int i = threadId; i < nextThreadId; i++) {
                    addCloseCommand(i, order);
                }
                order++;

            } else if (SLEEP.equals(command)) {
                String arg = vars.expand(line.substring(SLEEP_LEN).trim());
                trace("@sleep", arg);
                long millis = Long.parseLong(arg);
                assert (millis >= 0L) : "Sleep timeout must be >= 0";

                for (int i = threadId; i < nextThreadId; i++) {
                    addSleepCommand(i, order, millis);
                }
                order++;

            } else {
                assert false : "Unknown command " + command;
            }

            return true;                // normally, advance the state
        }

        private void doEndOfState(String state)
        {
            if (state.equals(PRE_SETUP_STATE)) {
                applyVariableRebindings();
            }
        }

        private void defineVariables(String line)
        {
            // two forms: "VAR VAR*" and "VAR=VAL$"
            Matcher varDefn = matchesVarDefn.matcher(line);
            if (varDefn.lookingAt()) {
                String var = varDefn.group(1);
                String val = varDefn.group(2);
                vars.define(var, val);
            } else {
                String[] words = splitWords.split(line);
                for (String var : words) {
                    String value = System.getenv(var);
                    vars.define(var, value);
                }
            }
        }

        private void plugin(String pluginName) throws IOException
        {
            try {
                Class<?> pluginClass = Class.forName(pluginName);
                ConcurrentTestPlugin plugin =
                    (ConcurrentTestPlugin) pluginClass.newInstance();
                plugins.add(plugin);
                addExtraCommands(
                    plugin.getSupportedThreadCommands(), THREAD_STATE);
                addExtraCommands(
                    plugin.getSupportedThreadCommands(), REPEAT_STATE);
                for (String commandName : plugin.getSupportedThreadCommands()) {
                    pluginForCommand.put(commandName, plugin);
                }
                addExtraCommands(
                    plugin.getSupportedPreSetupCommands(), PRE_SETUP_STATE);
                for (String commandName : plugin.getSupportedPreSetupCommands())
                {
                    preSetupPluginForCommand.put(commandName, plugin);
                }
            } catch (Exception e) {
                throw new IOException(e.toString());
            }
        }

        private void addExtraCommands(Iterable<String> commands, String state)
        {
            assert (state != null);

            for (int i = 0, n = STATE_TABLE.length; i < n; i++) {
                if (state.equals(STATE_TABLE[i].state)) {
                    StateDatum[] stateData = STATE_TABLE[i].stateData;
                    ArrayList<StateDatum> stateDataList =
                        new ArrayList<StateDatum>(Arrays.asList(stateData));
                    for (String cmd : commands) {
                        stateDataList.add(new StateDatum(cmd, state));
                    }
                    STATE_TABLE[i] =
                        new StateAction(
                            state, stateDataList.toArray(stateData));
                }
            }
        }

        /**
         * Manages state transitions.
         * Converts a state name into a map. Map keys are the names of available
         * commands (e.g. @sync), and map values are the state to switch to open
         * seeing the command.
         */
        private Map<String, String> lookupState(String state)
        {
            assert (state != null);

            for (int i = 0, n = STATE_TABLE.length; i < n; i++) {
                if (state.equals(STATE_TABLE[i].state)) {
                    StateDatum [] stateData = STATE_TABLE[i].stateData;

                    Map<String, String> result = new HashMap<String, String>();
                    for (int j = 0, m = stateData.length; j < m; j++) {
                        result.put(stateData[j].x, stateData[j].y);
                    }
                    return result;
                }
            }

            throw new IllegalArgumentException();
        }

        /**
         * Returns the first word of the given line, assuming the line is
         * trimmed. Returns the characters up the first non-whitespace
         * character in the line.
         */
        private String firstWord(String trimmedLine)
        {
            return trimmedLine.replaceFirst("\\s.*", "");
        }

        /**
         * Returns everything but the first word of the given line, assuming the
         * line is trimmed. Returns the characters following the first series of
         * consecutive whitespace characters in the line.
         */
        private String skipFirstWord(String trimmedLine)
        {
            return trimmedLine.replaceFirst("^\\S+\\s+", "");
        }

        /**
         * Returns an input line, possible extended by the continuation
         * character (\).  Scans the script until it finds an un-escaped
         * newline.
         */
        private String readLine(String line, BufferedReader in)
            throws IOException
        {
            line = line.trim();
            boolean more = line.endsWith("\\");
            if (more) {
                line = line.substring(0, line.lastIndexOf('\\')); // snip
                StringBuffer buf = new StringBuffer(line);        // save
                while (more) {
                    line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    line = line.trim();
                    more = line.endsWith("\\");
                    if (more) {
                        line = line.substring(0, line.lastIndexOf('\\'));
                    }
                    buf.append(' ').append(line);
                }
                line = buf.toString().trim();
            }

            if (scriptHasVars && line.contains("$")) {
                line = vars.expand(line);
            }

            return line;
        }

        /**
         * Returns a block of SQL, starting with the given String. Returns
         * <code> startOfSql</code> concatenated with each line from
         * <code>in</code> until a line ending with a semicolon is found.
         */
        private String readSql(String startOfSql, BufferedReader in)
            throws IOException
        {
            // REVIEW mb StringBuffer not always needed
            StringBuffer sql = new StringBuffer(startOfSql);
            sql.append('\n');

            String line;
            if (!startOfSql.trim().endsWith(";")) {
                while ((line = in.readLine()) != null) {
                    sql.append(line).append('\n');
                    if (line.trim().endsWith(";")) {
                        break;
                    }
                }
            }

            line = sql.toString().trim();
            if (scriptHasVars && line.contains("$")) {
                line = vars.expand(line);
            }
            return line;
        }
    }


    // Inner Classes: the Commands

    // When executed, a @print command defines how any following @fetch
    // or @select commands will handle their resuult rows. MTSQL can print all
    // rows, no rows, or every nth row. A printed row can be prefixed by a
    // sequence nuber and/or the time it was received (a different notion than
    // its rowtime, which often tells when it was inserted).
    private class PrintCommand extends AbstractCommand
    {
        // print every nth row: 1 means all rows, 0 means no rows.
        private final int nth;
        private final boolean count;    // print a sequence number
        private final boolean time;     // print the time row was fetched
        // print total row count and elapsed fetch time:
        private final boolean total;
        // TODO: more control of formats

        PrintCommand(String spec)
        {
            int nth = 0;
            boolean count = false;
            boolean time = false;
            boolean total = false;
            StringTokenizer tokenizer = new StringTokenizer(spec);

            if (tokenizer.countTokens() == 0) {
                // a bare "@print" means "@print all"
                nth = 1;
            } else {
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if (token.equalsIgnoreCase("none")) {
                        nth = 0;
                    } else if (token.equalsIgnoreCase("all")) {
                        nth = 1;
                    } else if (token.equalsIgnoreCase("total")) {
                        total = true;
                    } else if (token.equalsIgnoreCase("count")) {
                        count = true;
                    } else if (token.equalsIgnoreCase("time")) {
                        time = true;
                    } else if (token.equalsIgnoreCase("every")) {
                        nth = 1;
                        if (tokenizer.hasMoreTokens()) {
                            token = tokenizer.nextToken();
                            nth = Integer.parseInt(token);
                        }
                    }
                }
            }
            this.nth = nth;
            this.count = count;
            this.time = time;
            this.total = total;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Integer threadId = executor.getThreadId();
            BufferedWriter out = threadBufferedWriters.get(threadId);
            threadResultsReaders.put(
                threadId, new ResultsReader(out, nth, count, time, total));
        }
    }

    private class EchoCommand extends AbstractCommand
    {
        private final String msg;
        private EchoCommand(String msg)
        {
            this.msg = msg;
        }
        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            storeMessage(executor.getThreadId(), msg);
        }
    }

    private class PluginCommand extends AbstractCommand
    {

        private final ConcurrentTestPluginCommand pluginCommand;

        private PluginCommand(
            String command,
            String params) throws IOException
        {
            ConcurrentTestPlugin plugin = pluginForCommand.get(command);
            pluginCommand = plugin.getCommandFor(command, params);
        }

        protected void doExecute(final ConcurrentTestCommandExecutor exec)
            throws Exception
        {
            ConcurrentTestPluginCommand.TestContext context =
                new ConcurrentTestPluginCommand.TestContext() {
                public void storeMessage(String message)
                {
                    ConcurrentTestCommandScript.this.storeMessage(
                        exec.getThreadId(), message);
                }

                public Connection getConnection()
                {
                    return exec.getConnection();
                }

                public Statement getCurrentStatement()
                {
                    return exec.getStatement();
                }
            };
            pluginCommand.execute(context);
        }
    }

    // Matches shell wilcards and other special characters: when a command
    // contains some of these, it needs a shell to run it.
    private final Pattern shellWildcardPattern = Pattern.compile("[*?$|<>&]");

    // REVIEW mb 2/24/09 (Mardi Gras) Should this have a timeout?
    private class ShellCommand extends AbstractCommand
    {
        private final String command;
        private List<String> argv;      // the command, parsed and massaged

        private ShellCommand(String command)
        {
            this.command = command;
            boolean needShell = hasWildcard(command);
            if (needShell) {
                argv = new ArrayList<String>();
                argv.add("/bin/sh");
                argv.add("-c");
                argv.add(command);
            } else {
                argv = tokenize(command);
            }
        }

        private boolean hasWildcard(String command)
        {
            return shellWildcardPattern.matcher(command).find();
        }

        private List<String> tokenize(String s)
        {
            List<String> result = new ArrayList<String>();
            StringTokenizer tokenizer = new StringTokenizer(s);
            while (tokenizer.hasMoreTokens()) {
                result.add(tokenizer.nextToken());
            }
            return result;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
        {
            Integer threadId = executor.getThreadId();
            storeMessage(threadId, command);

            // argv[0] is found on $PATH. Working directory is the script's home
            // directory.
            ProcessBuilder pb = new ProcessBuilder(argv);
            pb.directory(scriptDirectory);
            try {
                // direct stdout & stderr to the the threadWriter
                int status =
                    Util.runAppProcess(
                        pb, null, null, getThreadWriter(threadId));
                if (status != 0) {
                    storeMessage(
                        threadId, "command " + command
                        + ": exited with status " + status);
                }
            } catch (Exception e) {
                storeMessage(
                    threadId, "command " + command
                    + ": failed with exception " + e.getMessage());
            }
        }
    }


    // TODO: replace by super.CommmandWithTimeout
    private static abstract class CommandWithTimeout
        extends AbstractCommand
    {
        private long timeout;

        private CommandWithTimeout(long timeout)
        {
            this.timeout = timeout;
        }

        // returns the timeout as set (-1 means no timeout)
        protected long setTimeout(Statement stmt)
            throws SQLException
        {
            assert (timeout >= 0);
            if (timeout > 0) {
                // FIX: call setQueryTimeoutMillis() when available.
                assert (timeout >= 1000) : "timeout too short";
                int t = (int) (timeout / 1000);
                stmt.setQueryTimeout(t);
                return t;
            }
            return -1;
        }
    }

    private static abstract class CommandWithTimeoutAndRowLimit
        extends CommandWithTimeout
    {
        private int rowLimit;

        private CommandWithTimeoutAndRowLimit(long timeout)
        {
            this(timeout, 0);
        }

        private CommandWithTimeoutAndRowLimit(long timeout, int rowLimit)
        {
            super(timeout);
            this.rowLimit = rowLimit;
        }

        protected void setRowLimit(Statement stmt)
            throws SQLException
        {
            assert (rowLimit >= 0);
            if (rowLimit > 0) {
                stmt.setMaxRows(rowLimit);
            }
        }
    }

    /**
     * SelectCommand creates and executes a SQL select statement, with optional
     * timeout and row limit.
     */
    private class SelectCommand
        extends CommandWithTimeoutAndRowLimit
    {
        private String sql;

        private SelectCommand(String sql)
        {
            this(sql, 0, 0);
        }

        private SelectCommand(String sql, boolean errorExpected)
        {
            this(sql, 0, 0);
            if (errorExpected) {
                this.markToFail();
            }
        }

        private SelectCommand(String sql, long timeout)
        {
            this(sql, timeout, 0);
        }

        private SelectCommand(String sql, long timeout, int rowLimit)
        {
            super(timeout, rowLimit);
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            // TODO: trim and chop in constructor; stash sql in base class;
            // execute() calls storeSql.
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);
            long timeout = setTimeout(stmt);
            setRowLimit(stmt);

            try {
                storeResults(
                    executor.getThreadId(),
                    stmt.executeQuery(),
                    timeout);
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * SelectCommand creates and executes a SQL select statement, with optional
     * timeout.
     */
    private class SqlCommand
        extends CommandWithTimeout
    {
        private String sql;

        private SqlCommand(String sql)
        {
            super(0);
            this.sql = sql;
        }

        private SqlCommand(String sql, boolean errorExpected)
        {
            super(0);
            this.sql = sql;
            if (errorExpected) {
                this.markToFail();
            }
        }

        private SqlCommand(String sql, long timeout)
        {
            super(timeout);
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            if (properSql.equalsIgnoreCase("commit")) {
                executor.getConnection().commit();
                return;
            } else if (properSql.equalsIgnoreCase("rollback")) {
                executor.getConnection().rollback();
                return;
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);
            long timeout = setTimeout(stmt);
            boolean timeoutSet = (timeout >= 0);

            try {
                boolean haveResults = stmt.execute();
                if (haveResults) {
                    // Farrago rewrites "call" statements as selects.
                    storeMessage(
                        executor.getThreadId(),
                        "0 rows affected.");
                    // is there anything interesting in the ResultSet?
                } else {
                    int rows = stmt.getUpdateCount();
                    if (rows != 1) {
                        storeMessage(
                            executor.getThreadId(),
                            String.valueOf(rows) + " rows affected.");
                    } else {
                        storeMessage(
                            executor.getThreadId(),
                            "1 row affected.");
                    }
                }
            } catch (SqlTimeoutException e) {
                if (!timeoutSet) {
                    throw e;
                }

                Util.swallow(e, null);
                storeMessage(
                    executor.getThreadId(),
                    "Timeout");
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * PrepareCommand creates a {@link PreparedStatement}, which is saved as the
     * current statement of its test thread. For a preparted query (a SELECT or
     * a CALL with results), a subsequent FetchAndPrintCommand executes the
     * statement and fetches its reults, until end-of-data or a timeout. A
     * PrintCommand attaches a listener, called for each rows, that selects rows
     * to save and print, and sets the format. By default, if no PrintCommand
     * appears before a FetchAndPrintCommand, all rows are printed. A
     * CloseCommand closes and discards the prepared statement.
     */
    private class PrepareCommand
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
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            executor.setStatement(stmt);
        }
    }

    /**
     * FetchAndPrintCommand executes a previously prepared statement stored
     * inthe ConcurrentTestCommandExecutor and then outputs the returned
     * rows.
     */
    private class FetchAndPrintCommand
        extends CommandWithTimeout
    {
        private FetchAndPrintCommand(long timeout)
        {
            super(timeout);
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                (PreparedStatement) executor.getStatement();
            long timeout = setTimeout(stmt);

            storeResults(
                executor.getThreadId(),
                stmt.executeQuery(),
                timeout);
        }
    }

    private class ResultsReader
    {
        private final PrintWriter out;
        // print every Nth row. 1 means all rows, 0 means none.
        private final int nth;
        // prefix printed row with its sequence number
        private final boolean counted;
        // prefix printed row with time it was fetched
        private final boolean timestamped;
        // print final summary, rows & elapsed time.
        private final boolean totaled;

        private long baseTime = 0;
        private int rowCount = 0;
        private int ncols = 0;
        private int[] widths;
        private String[] labels;

        ResultsReader(BufferedWriter out)
        {
            this(out, 1, false, false, false);
        }

        ResultsReader(
            BufferedWriter out,
            int nth, boolean counted, boolean timestamped, boolean totaled)
        {
            this.out = new PrintWriter(out);
            this.nth = nth;
            this.counted = counted;
            this.timestamped = timestamped;
            this.totaled = totaled;
            this.baseTime = scriptStartTime;
        }

        void prepareFormat(ResultSet rset) throws SQLException
        {
            ResultSetMetaData meta = rset.getMetaData();
            ncols = meta.getColumnCount();
            widths = new int[ncols];
            labels = new String[ncols];
            for (int i = 0; i < ncols; i++) {
                labels[i] = meta.getColumnLabel(i + 1);
                int displaySize = meta.getColumnDisplaySize(i + 1);

                // NOTE jvs 13-June-2006: I put this in to cap EXPLAIN PLAN,
                // which now returns a very large worst-case display size.
                if (displaySize > 4096) {
                    displaySize = 0;
                }
                widths[i] = Math.max(labels[i].length(), displaySize);
            }
        }

        private void printHeaders()
        {
            printSeparator();
            indent(); printRow(labels);
            printSeparator();
        }

        void read(ResultSet rset, long timeout) throws SQLException
        {
            boolean withTimeout = (timeout >= 0);
            boolean timedOut = false;
            long startTime = 0, endTime = 0;
            try {
                prepareFormat(rset);
                String [] values = new String[ncols];
                int printedRowCount = 0;
                if (nth > 0) {
                    printHeaders();
                }
                startTime = System.currentTimeMillis();
                for (rowCount = 0; rset.next(); rowCount++) {
                    if (nth == 0) {
                        continue;
                    }
                    if (nth == 1 || rowCount % nth == 0) {
                        long time = System.currentTimeMillis();
                        if (printedRowCount > 0
                            && (printedRowCount % 100 == 0))
                            {
                                printHeaders();
                            }
                        for (int i = 0; i < ncols; i++) {
                            values[i] = rset.getString(i + 1);
                        }
                        if (counted) {
                            printRowCount(rowCount);
                        }
                        if (timestamped) {
                            printTimestamp(time);
                        }
                        printRow(values);
                        printedRowCount++;
                    }
                }
            } catch (SqlTimeoutException e) {
                endTime = System.currentTimeMillis();
                timedOut = true;
                if (!withTimeout) {
                    throw e;
                }
                Util.swallow(e, null);
            } catch (SQLException e) {
                endTime = System.currentTimeMillis();
                timedOut = true;

                // 2007-10-23 hersker: hack to ignore timeout exceptions
                // from other Farrago projects without being able to
                // import/reference the actual exceptions
                final String eClassName = e.getClass().getName();
                if (eClassName.endsWith("TimeoutException")) {
                    if (!withTimeout) {
                        throw e;
                    }
                    Util.swallow(e, null);
                } else {
                    Util.swallow(e, null);
                    out.println(e.getMessage());
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (endTime == 0) {
                    endTime = System.currentTimeMillis();
                }
                rset.close();
                if (nth > 0) {
                    printSeparator();
                    out.println();
                }
                if (verbose) {
                    out.printf(
                        "fetch started at %tc %d, %s at %tc %d%n",
                        startTime, startTime,
                        (timedOut ? "timeout" : "eos"),
                        endTime, endTime);
                }
                if (totaled) {
                    long dt = endTime - startTime;
                    if (withTimeout) {
                        dt -= timeout;
                    }
                    assert (dt >= 0);
                    out.printf(
                        "fetched %d rows in %d msecs %s%n",
                        rowCount, dt, timedOut ? "(timeout)" : "(end)");
                }
            }
        }

        private void printRowCount(int count)
        {
            out.printf("(%06d) ", count);
        }

        private void printTimestamp(long time)
        {
            time -= baseTime;
            out.printf("(% 4d.%03d) ", time / 1000, time % 1000);
        }

        // indent a heading or separator line to match a row-values line
        private void indent()
        {
            if (counted) {
                out.print("         ");
            }
            if (timestamped) {
                out.print("           ");
            }
        }

        /**
         * Prints an output table separator. Something like <code>
         * "+----+--------+"</code>.
         */
        private void printSeparator()
        {
            indent();
            for (int i = 0; i < widths.length; i++) {
                if (i > 0) {
                    out.write("-+-");
                } else {
                    out.write("+-");
                }

                int numDashes = widths[i];
                while (numDashes > 0) {
                    out.write(
                        dashes,
                        0,
                        Math.min(numDashes, BUF_SIZE));
                    numDashes -= Math.min(numDashes, BUF_SIZE);
                }
            }
            out.println("-+");
        }

        /**
         * Prints an output table row. Something like <code>"| COL1 | COL2
         * |"</code>.
         */
        private void printRow(String [] values)
        {
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                if (value == null) {
                    value = "";
                }
                if (i > 0) {
                    out.write(" | ");
                } else {
                    out.write("| ");
                }
                out.write(value);
                int excess = widths[i] - value.length();
                while (excess > 0) {
                    out.write(
                        spaces,
                        0,
                        Math.min(excess, BUF_SIZE));
                    excess -= Math.min(excess, BUF_SIZE);
                }
            }
            out.println(" |");
        }
    }


    // Inner class: stand-alone client test tool
    private static class Tool
    {
        boolean quiet = false;          // -q
        boolean verbose = false;        // -v
        boolean debug = false;          // -g
        String server;                  // -u
        String driver;                  // -d
        String user;                    // -n
        String password;                // -p
        List<String> bindings;          // VAR=VAL
        List<String> files;             // FILE

        public Tool()
        {
            bindings = new ArrayList<String>();
            files = new ArrayList<String>();
        }

        // returns 0 on success, 1 on error, 2 on bad invocation.
        public int run(String[] args)
        {
            try {
                if (!parseCommand(args)) {
                    usage();
                    return 2;
                }

                Class z = Class.forName(driver); // load driver
                Properties jdbcProps = new Properties();
                if (user != null) {
                    jdbcProps.setProperty("user", user);
                }
                if (password != null) {
                    jdbcProps.setProperty("password", password);
                }

                BufferedWriter cout =
                    new BufferedWriter(new OutputStreamWriter(System.out));
                for (String file : files) {
                    ConcurrentTestCommandScript script =
                        new ConcurrentTestCommandScript();
                    try {
                        script.setQuiet(quiet);
                        script.setVerbose(verbose);
                        script.setDebug(debug);
                        script.prepare(file, bindings);
                        script.setDataSource(server, jdbcProps);
                        script.execute();
                    } finally {
                        if (!quiet) {
                            script.printResults(cout);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return 1;
            }
            return 0;
        }

        static void usage()
        {
            System.err.println(
                "Usage: mtsql [-vg] -u SERVER -d DRIVER "
                + "[-n USER][-p PASSWORD] SCRIPT [SCRIPT]...");
        }

        boolean parseCommand(String[] args)
        {
            try {
                // very permissive as to order
                for (int i = 0; i < args.length;) {
                    String arg = args[i++];
                    if (arg.charAt(0) == '-') {
                        switch (arg.charAt(1)) {
                        case 'v':
                            verbose = true;
                            break;
                        case 'q':
                            quiet = true;
                            break;
                        case 'g':
                            debug = true;
                            break;
                        case 'u':
                            this.server = args[i++];
                            break;
                        case 'd':
                            this.driver = args[i++];
                            break;
                        case 'n':
                            this.user = args[i++];
                            break;
                        case 'p':
                            this.password = args[i++];
                            break;
                        default:
                            return false;
                        }
                    } else if (arg.contains("=")) {
                        if (Character.isJavaIdentifierStart(arg.charAt(0))) {
                            bindings.add(arg);
                        } else {
                            return false;
                        }
                    } else {
                        files.add(arg);
                    }
                }
                if (server == null || driver == null) {
                    return false;
                }
            } catch (Throwable th) {
                return false;
            }
            return true;
        }
    }

    /**
     * Client tool that connects via jdbc and runs one or more mtsql on that
     * connection.
     *
     * <p>Usage: mtsql [-vgq] -u SERVER -d DRIVER [-n USER][-p PASSWORD]
     * [VAR=VAL]...  SCRIPT [SCRIPT]...
     */
    public static void main(String[] args)
    {
        int status = new Tool().run(args);
        System.exit(status);
    }
}

// End ConcurrentTestCommandScript.java
