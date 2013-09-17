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
package org.eigenbase.test;

import java.io.*;

import java.util.*;
import java.util.regex.*;

import org.eigenbase.util.*;

import org.incava.util.diff.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;


/**
 * DiffTestCase is an abstract base for JUnit tests which produce multi-line
 * output to be verified by diffing against a pre-existing reference file.
 */
public abstract class DiffTestCase {
    //~ Instance fields --------------------------------------------------------

    private final String testCaseName;

    /**
     * Name of current .log file.
     */
    protected File logFile;

    /**
     * Name of current .ref file.
     */
    protected File refFile;

    /**
     * OutputStream for current test log.
     */
    protected OutputStream logOutputStream;

    /**
     * Diff masks defined so far
     */
    // private List diffMasks;
    private String diffMasks;
    Matcher compiledDiffMatcher;
    private String ignorePatterns;
    Matcher compiledIgnoreMatcher;

    int gcInterval;

    /**
     * Whether to give verbose message if diff fails.
     */
    private boolean verbose;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new DiffTestCase.
     *
     * @param testCaseName Test case name
     */
    protected DiffTestCase(String testCaseName) throws Exception {
        this.testCaseName = testCaseName;
        // diffMasks = new ArrayList();
        diffMasks = "";
        ignorePatterns = "";
        compiledIgnoreMatcher = null;
        compiledDiffMatcher = null;
        gcInterval = 0;
        String verboseVal =
            System.getProperty(DiffTestCase.class.getName() + ".verbose");
        if (verboseVal != null) {
            verbose = true;
        }
    }

    //~ Methods ----------------------------------------------------------------

    @Before protected void setUp() {
        // diffMasks.clear();
        diffMasks = "";
        ignorePatterns = "";
        compiledIgnoreMatcher = null;
        compiledDiffMatcher = null;
        gcInterval = 0;
    }

    @After protected void tearDown() throws IOException {
        if (logOutputStream != null) {
            logOutputStream.close();
            logOutputStream = null;
        }
    }

    /**
     * Initializes a diff-based test. Any existing .log and .dif files
     * corresponding to this test case are deleted, and a new, empty .log file
     * is created. The default log file location is a subdirectory under the
     * result getTestlogRoot(), where the subdirectory name is based on the
     * unqualified name of the test class. The generated log file name will be
     * testMethodName.log, and the expected reference file will be
     * testMethodName.ref.
     *
     * @return Writer for log file, which caller should use as a destination for
     * test output to be diffed
     */
    protected Writer openTestLog()
        throws Exception
    {
        File testClassDir =
            new File(
                getTestlogRoot(),
                ReflectUtil.getUnqualifiedClassName(getClass()));
        testClassDir.mkdirs();
        File testLogFile =
            new File(
                testClassDir,
                testCaseName);
        return new OutputStreamWriter(openTestLogOutputStream(testLogFile));
    }

    /**
     * @return the root under which testlogs should be written
     */
    protected abstract File getTestlogRoot()
        throws Exception;

    /**
     * Initializes a diff-based test, overriding the default log file naming
     * scheme altogether.
     *
     * @param testFileSansExt full path to log filename, without .log/.ref
     * extension
     */
    protected OutputStream openTestLogOutputStream(File testFileSansExt)
        throws IOException
    {
        assert (logOutputStream == null);

        logFile = new File(testFileSansExt.toString() + ".log");
        logFile.delete();

        refFile = new File(testFileSansExt.toString() + ".ref");

        logOutputStream = new FileOutputStream(logFile);
        return logOutputStream;
    }

    /**
     * Finishes a diff-based test. Output that was written to the Writer
     * returned by openTestLog is diffed against a .ref file, and if any
     * differences are detected, the test case fails. Note that the diff used is
     * just a boolean test, and does not create any .dif ouput.
     *
     * <p>NOTE: if you wrap the Writer returned by openTestLog() (e.g. with a
     * PrintWriter), be sure to flush the wrapping Writer before calling this
     * method.</p>
     *
     * @see #diffFile(File, File)
     */
    protected void diffTestLog()
        throws IOException
    {
        assert (logOutputStream != null);
        logOutputStream.close();
        logOutputStream = null;

        if (!refFile.exists()) {
            Assert.fail("Reference file " + refFile + " does not exist");
        }
        diffFile(logFile, refFile);
    }

    /**
     * Compares a log file with its reference log.
     *
     * <p>Usually, the log file and the reference log are in the same directory,
     * one ending with '.log' and the other with '.ref'.
     *
     * <p>If the files are identical, removes logFile.
     *
     * @param logFile Log file
     * @param refFile Reference log
     */
    protected void diffFile(File logFile, File refFile)
        throws IOException
    {
        int n = 0;
        FileReader logReader = null;
        FileReader refReader = null;
        try {
            if (compiledIgnoreMatcher != null) {
                if (gcInterval != 0) {
                    n++;
                    if (n == gcInterval) {
                        n = 0;
                        System.gc();
                    }
                }
            }

            // NOTE: Use of diff.mask is deprecated, use diff_mask.
            String diffMask = System.getProperty("diff.mask", null);
            if (diffMask != null) {
                addDiffMask(diffMask);
            }

            diffMask = System.getProperty("diff_mask", null);
            if (diffMask != null) {
                addDiffMask(diffMask);
            }

            logReader = new FileReader(logFile);
            refReader = new FileReader(refFile);
            LineNumberReader logLineReader = new LineNumberReader(logReader);
            LineNumberReader refLineReader = new LineNumberReader(refReader);
            for (;;) {
                String logLine = logLineReader.readLine();
                String refLine = refLineReader.readLine();
                while ((logLine != null) && matchIgnorePatterns(logLine)) {
                    // System.out.println("logMatch Line:" + logLine);
                    logLine = logLineReader.readLine();
                }
                while ((refLine != null) && matchIgnorePatterns(refLine)) {
                    // System.out.println("refMatch Line:" + logLine);
                    refLine = refLineReader.readLine();
                }
                if ((logLine == null) || (refLine == null)) {
                    if (logLine != null) {
                        diffFail(
                            logFile,
                            logLineReader.getLineNumber());
                    }
                    if (refLine != null) {
                        diffFail(
                            logFile,
                            refLineReader.getLineNumber());
                    }
                    break;
                }
                logLine = applyDiffMask(logLine);
                refLine = applyDiffMask(refLine);
                if (!logLine.equals(refLine)) {
                    diffFail(
                        logFile,
                        logLineReader.getLineNumber());
                }
            }
        } finally {
            if (logReader != null) {
                logReader.close();
            }
            if (refReader != null) {
                refReader.close();
            }
        }

        // no diffs detected, so delete redundant .log file
        logFile.delete();
    }

    /**
     * set the number of lines for garbage collection.
     *
     * @param n an integer, the number of line for garbage collection, 0 means
     * no garbage collection.
     */
    protected void setGC(int n)
    {
        gcInterval = n;
    }

    /**
     * Adds a diff mask. Strings matching the given regular expression will be
     * masked before diffing. This can be used to suppress spurious diffs on a
     * case-by-case basis.
     *
     * @param mask a regular expression, as per String.replaceAll
     */
    protected void addDiffMask(String mask)
    {
        // diffMasks.add(mask);
        if (diffMasks.length() == 0) {
            diffMasks = mask;
        } else {
            diffMasks = diffMasks + "|" + mask;
        }
        Pattern compiledDiffPattern = Pattern.compile(diffMasks);
        compiledDiffMatcher = compiledDiffPattern.matcher("");
    }

    protected void addIgnorePattern(String javaPattern)
    {
        if (ignorePatterns.length() == 0) {
            ignorePatterns = javaPattern;
        } else {
            ignorePatterns = ignorePatterns + "|" + javaPattern;
        }
        Pattern compiledIgnorePattern = Pattern.compile(ignorePatterns);
        compiledIgnoreMatcher = compiledIgnorePattern.matcher("");
    }

    private String applyDiffMask(String s)
    {
        if (compiledDiffMatcher != null) {
            compiledDiffMatcher.reset(s);

            // we assume most of lines do not match
            // so compiled matches will be faster than replaceAll.
            if (compiledDiffMatcher.find()) {
                return s.replaceAll(diffMasks, "XYZZY");
            }
        }
        return s;
    }

    private boolean matchIgnorePatterns(String s)
    {
        if (compiledIgnoreMatcher != null) {
            compiledIgnoreMatcher.reset(s);
            return compiledIgnoreMatcher.matches();
        }
        return false;
    }

    private void diffFail(
        File logFile,
        int lineNumber)
    {
        final String message =
            "diff detected at line " + lineNumber + " in " + logFile;
        if (verbose) {
            if (inIde()) {
                // If we're in IntelliJ, it's worth printing the 'expected
                // <...> actual <...>' string, becauase IntelliJ can format
                // this intelligently. Otherwise, use the more concise
                // diff format.
                Assert.assertEquals(
                    message,
                    fileContents(refFile),
                    fileContents(logFile));
            } else {
                String s = diff(refFile, logFile);
                Assert.fail(
                    message + TestUtil.NL + s + TestUtil.NL);
            }
        }
        Assert.fail(message);
    }

    /**
     * Returns whether this test is running inside the IntelliJ IDE.
     *
     * @return whether we're running in IntelliJ.
     */
    private static boolean inIde()
    {
        Throwable runtimeException = new Throwable();
        runtimeException.fillInStackTrace();
        final StackTraceElement [] stackTrace =
            runtimeException.getStackTrace();
        StackTraceElement lastStackTraceElement =
            stackTrace[stackTrace.length - 1];

        // Junit test launched from IntelliJ 6.0
        if (lastStackTraceElement.getClassName().equals(
                "com.intellij.rt.execution.junit.JUnitStarter")
            && lastStackTraceElement.getMethodName().equals("main"))
        {
            return true;
        }

        // Application launched from IntelliJ 6.0
        if (lastStackTraceElement.getClassName().equals(
                "com.intellij.rt.execution.application.AppMain")
            && lastStackTraceElement.getMethodName().equals("main"))
        {
            return true;
        }
        return false;
    }

    /**
     * Returns a string containing the difference between the contents of two
     * files. The string has a similar format to the UNIX 'diff' utility.
     */
    private static String diff(File file1, File file2)
    {
        List<String> lines1 = fileLines(file1);
        List<String> lines2 = fileLines(file2);
        return diffLines(lines1, lines2);
    }

    /**
     * Returns a string containing the difference between the two sets of lines.
     */
    public static String diffLines(List<String> lines1, List<String> lines2)
    {
        Diff differencer = new Diff(lines1, lines2);
        List<Difference> differences = differencer.diff();
        StringWriter sw = new StringWriter();
        int offset = 0;
        for (Difference d : differences) {
            final int as = d.getAddedStart() + 1;
            final int ae = d.getAddedEnd() + 1;
            final int ds = d.getDeletedStart() + 1;
            final int de = d.getDeletedEnd() + 1;
            if (ae == 0) {
                if (de == 0) {
                    // no change
                } else {
                    // a deletion: "<ds>,<de>d<as>"
                    sw.append(String.valueOf(ds));
                    if (de > ds) {
                        sw.append(",").append(String.valueOf(de));
                    }
                    sw.append("d").append(String.valueOf(as - 1)).append(
                        TestUtil.NL);
                    for (int i = ds - 1; i < de; ++i) {
                        sw.append("< ").append(lines1.get(i)).append(
                            TestUtil.NL);
                    }
                }
            } else {
                if (de == 0) {
                    // an addition: "<ds>a<as,ae>"
                    sw.append(String.valueOf(ds - 1)).append("a").append(
                        String.valueOf(as));
                    if (ae > as) {
                        sw.append(",").append(String.valueOf(ae));
                    }
                    sw.append(TestUtil.NL);
                    for (int i = as - 1; i < ae; ++i) {
                        sw.append("> ").append(lines2.get(i)).append(
                            TestUtil.NL);
                    }
                } else {
                    // a change: "<ds>,<de>c<as>,<ae>
                    sw.append(String.valueOf(ds));
                    if (de > ds) {
                        sw.append(",").append(String.valueOf(de));
                    }
                    sw.append("c").append(String.valueOf(as));
                    if (ae > as) {
                        sw.append(",").append(String.valueOf(ae));
                    }
                    sw.append(TestUtil.NL);
                    for (int i = ds - 1; i < de; ++i) {
                        sw.append("< ").append(lines1.get(i)).append(
                            TestUtil.NL);
                    }
                    sw.append("---").append(TestUtil.NL);
                    for (int i = as - 1; i < ae; ++i) {
                        sw.append("> ").append(lines2.get(i)).append(
                            TestUtil.NL);
                    }
                    offset = offset + (ae - as) - (de - ds);
                }
            }
        }
        return sw.toString();
    }

    /**
     * Returns a list of the lines in a given file.
     *
     * @param file File
     *
     * @return List of lines
     */
    private static List<String> fileLines(File file)
    {
        List<String> lines = new ArrayList<String>();
        try {
            LineNumberReader r = new LineNumberReader(new FileReader(file));
            String line;
            while ((line = r.readLine()) != null) {
                lines.add(line);
            }
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the contents of a file as a string.
     *
     * @param file File
     *
     * @return Contents of the file
     */
    protected static String fileContents(File file)
    {
        try {
            char [] buf = new char[2048];
            final FileReader reader = new FileReader(file);
            int readCount;
            final StringWriter writer = new StringWriter();
            while ((readCount = reader.read(buf)) >= 0) {
                writer.write(buf, 0, readCount);
            }
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets whether to give verbose message if diff fails.
     */
    protected void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    /**
     * Sets the diff masks that are common to .REF files
     */
    protected void setRefFileDiffMasks()
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");

        // NOTE hersker 2006-06-02:
        // The following two patterns can be used to mask out the
        // sqlline JDBC URI and continuation prompts. This is useful
        // during transition periods when URIs are changed, or when
        // new drivers are deployed which have their own URIs but
        // should first pass the existing test suite before their
        // own .ref files get checked in.
        //
        // It is not recommended to use these patterns on an everyday
        // basis. Real differences in the output are difficult to spot
        // when diff-ing .ref and .log files which have different
        // sqlline prompts at the start of each line.

        // mask out sqlline JDBC URI prompt
        addDiffMask("0: \\bjdbc(:[^:>]+)+:>");

        // mask out different-length sqlline continuation prompts
        addDiffMask("^(\\.\\s?)+>");
    }
}

// End DiffTestCase.java
