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

import javax.xml.parsers.*;

import org.eigenbase.util.*;
import org.eigenbase.xom.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.w3c.dom.*;

import org.xml.sax.*;

import static org.junit.Assert.*;


/**
 * A collection of resources used by tests.
 *
 * <p>Loads files containing test input and output into memory. If there are
 * differences, writes out a log file containing the actual output.
 *
 * <p>Typical usage is as follows:
 *
 * <ol>
 * <li>A testcase class defines a method
 *
 * <blockquote><code>
 * <pre>
 *
 * package com.acme.test;
 *
 * public class MyTest extends TestCase {
 *     public DiffRepository getDiffRepos() {
 *         return DiffRepository.lookup(MyTest.class);
 *     }
 *
 *     @Test public void testToUpper() {
 *          getDiffRepos().assertEquals("${result}", "${string}");
 *     }

 *     @Test public void testToLower() {
 *          getDiffRepos().assertEquals("Multi-line\nstring", "${string}");
 *     }
 * }</pre>
 * </code></blockquote>
 *
 * There is an accompanying reference file named after the class, <code>
 * com/acme/test/MyTest.ref.xml</code>:
 *
 * <blockquote><code>
 * <pre>
 * &lt;Root&gt;
 *     &lt;TestCase name="testToUpper"&gt;
 *         &lt;Resource name="string"&gt;
 *             &lt;![CDATA[String to be converted to upper case]]&gt;
 *         &lt;/Resource&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[STRING TO BE CONVERTED TO UPPER CASE]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 *     &lt;TestCase name="testToLower"&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[multi-line
 * string]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 * &lt;/Root&gt;
 * </pre>
 * </code></blockquote>
 *
 * <p>If any of the testcases fails, a log file is generated, called <code>
 * com/acme/test/MyTest.log.xml</code> containing the actual output. The log
 * file is otherwise identical to the reference log, so once the log file has
 * been verified, it can simply be copied over to become the new reference
 * log.</p>
 *
 * <p>If a resource or testcase does not exist, <code>DiffRepository</code>
 * creates them in the log file. Because DiffRepository is so forgiving, it is
 * very easy to create new tests and testcases.</p>
 *
 * <p>The {@link #lookup} method ensures that all test cases share the same
 * instance of the repository. This is important more than one one test case
 * fails. The shared instance ensures that the generated <code>.log.xml</code>
 * file contains the actual for <em>both</em> test cases.
 */
public class DiffRepository {
    //~ Static fields/initializers ---------------------------------------------

/*
    Example XML document:

    <Root>
      <TestCase name="testFoo">
        <Resource name="sql">
          <![CDATA[select from emps]]>
         </Resource>
         <Resource name="plan">
           <![CDATA[MockTableImplRel.FENNEL_EXEC(table=[SALES, EMP])]]>
         </Resource>
       </TestCase>
       <TestCase name="testBar">
         <Resource name="sql">
           <![CDATA[select * from depts where deptno = 10]]>
         </Resource>
         <Resource name="output">
           <![CDATA[10, 'Sales']]>
         </Resource>
       </TestCase>
     </Root>
*/
    private static final String RootTag = "Root";
    private static final String TestCaseTag = "TestCase";
    private static final String TestCaseNameAttr = "name";
    private static final String TestCaseOverridesAttr = "overrides";
    private static final String ResourceTag = "Resource";
    private static final String ResourceNameAttr = "name";

    /**
     * Holds one diff-repository per class. It is necessary for all testcases in
     * the same class to share the same diff-repository: if the repos gets
     * loaded once per testcase, then only one diff is recorded.
     */
    private static final Map<Class, DiffRepository> mapClassToRepos =
        new HashMap<Class, DiffRepository>();

    //~ Instance fields --------------------------------------------------------

    private final DiffRepository baseRepos;
    private final DocumentBuilder docBuilder;
    private Document doc;
    private final Element root;
    private final File refFile;
    private final File logFile;
    private final Filter filter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a DiffRepository.
     *
     * @param refFile Reference file
     * @param logFile Log file
     * @param baseRepos Parent repository or null
     * @param filter Filter or null
     */
    private DiffRepository(
        File refFile,
        File logFile,
        DiffRepository baseRepos,
        Filter filter)
    {
        this.baseRepos = baseRepos;
        this.filter = filter;
        if (refFile == null) {
            throw new IllegalArgumentException("url must not be null");
        }
        this.refFile = refFile;
        Util.discard(this.refFile);
        this.logFile = logFile;

        // Load the document.
        DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
        try {
            this.docBuilder = fac.newDocumentBuilder();
            if (refFile.exists()) {
                // Parse the reference file.
                this.doc = docBuilder.parse(new FileInputStream(refFile));
                // Don't write a log file yet -- as far as we know, it's still
                // identical.
            } else {
                // There's no reference file. Create and write a log file.
                this.doc = docBuilder.newDocument();
                this.doc.appendChild(
                    doc.createElement(RootTag));
                flushDoc();
            }
            this.root = doc.getDocumentElement();
            if (!root.getNodeName().equals(RootTag)) {
                throw new RuntimeException(
                    "expected root element of type '" + RootTag
                    + "', but found '" + root.getNodeName() + "'");
            }
        } catch (ParserConfigurationException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (IOException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (SAXException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        }
    }

    //~ Methods ----------------------------------------------------------------

    private static File findFile(Class clazz, final String suffix)
    {
        // The reference file for class "com.foo.Bar" is "com/foo/Bar.ref.xml"
        String rest = clazz.getName().replace('.', File.separatorChar) + suffix;
        File fileBase = getFileBase(clazz);
        return new File(fileBase, rest);
    }

    /**
     * Returns the base directory relative to which test logs are stored. If
     * environment variable EIGEN_HOME is set, attempts to use that; otherwise,
     * attempts to use working directory and then its ancestors.
     */
    private static File getFileBase(Class clazz)
    {
        File file =
            getFileBaseGivenRoot(
                clazz,
                System.getenv("EIGEN_HOME"),
                false);
        if (file == null) {
            file =
                getFileBaseGivenRoot(
                    clazz,
                    System.getProperty("user.dir"),
                    true);
        }
        if (file == null) {
            throw new RuntimeException("cannot find base dir");
        }
        return file;
    }

    private static File getFileBaseGivenRoot(
        Class clazz,
        String root,
        boolean searchParent)
    {
        if (root == null) {
            return null;
        }
        String javaFileName =
            clazz.getName().replace('.', File.separatorChar) + ".java";
        File file = new File(root);
        while (true) {
            File file2 = new File(file, "src/test/java");
            if (new File(file2, javaFileName).exists()) {
                return file2;
            }
            file2 = new File(file, "src/main/java");
            if (new File(file2, javaFileName).exists()) {
                return file2;
            }

            file = file.getParentFile();
            if ((file == null) || !searchParent) {
                return null;
            }
        }
    }

    /**
     * Expands a string containing one or more variables. (Currently only works
     * if there is one variable.)
     */
    public String expand(String tag, String text)
    {
        if (text == null) {
            return null;
        } else if (text.startsWith("${")
            && text.endsWith("}"))
        {
            final String testCaseName = getCurrentTestCaseName(true);
            final String token = text.substring(2, text.length() - 1);
            if (tag == null) {
                tag = token;
            }
            assert token.startsWith(tag) : "token '" + token
                + "' does not match tag '" + tag + "'";
            String expanded = get(testCaseName, token);
            if (expanded == null) {
                // Token is not specified. Return the original text: this will
                // cause a diff, and the actual value will be written to the
                // log file.
                return text;
            }
            if (filter != null) {
                expanded =
                    filter.filter(this, testCaseName, tag, text, expanded);
            }
            return expanded;
        } else {
            // Make sure what appears in the resource file is consistent with
            // what is in the Java. It helps to have a redundant copy in the
            // resource file.
            final String testCaseName = getCurrentTestCaseName(true);
            if ((baseRepos != null)
                && (baseRepos.get(testCaseName, tag) != null))
            {
                // set in base repos; don't override
            } else {
                set(tag, text);
            }
            return text;
        }
    }

    /**
     * Sets the value of a given resource of the current testcase.
     *
     * @param resourceName Name of the resource, e.g. "sql"
     * @param value Value of the resource
     */
    public synchronized void set(String resourceName, String value)
    {
        assert resourceName != null;
        final String testCaseName = getCurrentTestCaseName(true);
        update(testCaseName, resourceName, value);
    }

    public void amend(String expected, String actual)
    {
        if (expected.startsWith("${")
            && expected.endsWith("}"))
        {
            String token = expected.substring(2, expected.length() - 1);
            set(token, actual);
        } else {
            // do nothing
        }
    }

    /**
     * Returns a given resource from a given testcase.
     *
     * @param testCaseName Name of test case, e.g. "testFoo"
     * @param resourceName Name of resource, e.g. "sql", "plan"
     *
     * @return The value of the resource, or null if not found
     */
    private String get(
        final String testCaseName,
        String resourceName)
    {
        Element testCaseElement = getTestCaseElement(testCaseName, true);
        if (testCaseElement == null) {
            if (baseRepos != null) {
                return baseRepos.get(testCaseName, resourceName);
            } else {
                return null;
            }
        }
        final Element resourceElement =
            getResourceElement(testCaseElement, resourceName);
        if (resourceElement != null) {
            return getText(resourceElement);
        }
        return null;
    }

    /**
     * Returns the text under an element.
     */
    private static String getText(Element element)
    {
        // If there is a <![CDATA[ ... ]]> child, return its text and ignore
        // all other child elements.
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof CDATASection) {
                return node.getNodeValue();
            }
        }

        // Otherwise return all the text under this element (including
        // whitespace).
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof Text) {
                buf.append(((Text) node).getWholeText());
            }
        }
        return buf.toString();
    }

    /**
     * Returns the &lt;TestCase&gt; element corresponding to the current test
     * case.
     *
     * @param testCaseName Name of test case
     * @param checkOverride Make sure that if an element overrides an element in
     * a base repository, it has overrides="true"
     *
     * @return TestCase element, or null if not found
     */
    private Element getTestCaseElement(
        final String testCaseName,
        boolean checkOverride)
    {
        final NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(TestCaseTag)) {
                Element testCase = (Element) child;
                if (testCaseName.equals(
                        testCase.getAttribute(TestCaseNameAttr)))
                {
                    if (checkOverride
                        && (baseRepos != null)
                        && (baseRepos.getTestCaseElement(
                            testCaseName,
                            false) != null)
                        && !"true".equals(
                            testCase.getAttribute(TestCaseOverridesAttr)))
                    {
                        throw new RuntimeException(
                            "TestCase  '" + testCaseName + "' overrides a "
                            + "testcase in the base repository, but does "
                            + "not specify 'overrides=true'");
                    }
                    return testCase;
                }
            }
        }
        return null;
    }

    /**
     * Returns the name of the current testcase by looking up the call stack for
     * a method whose name starts with "test", for example "testFoo".
     *
     * @param fail Whether to fail if no method is found
     *
     * @return Name of current testcase, or null if not found
     */
    private String getCurrentTestCaseName(boolean fail)
    {
        // REVIEW jvs 12-Mar-2006: Too clever by half.  Someone might not know
        // about this and use a private helper method whose name also starts
        // with test. Perhaps just require them to pass in getName() from the
        // calling TestCase's setUp method and store it in a thread-local,
        // failing here if they forgot?

        // Clever, this. Dump the stack and look up it for a method which
        // looks like a testcase name, e.g. "testFoo".
        final StackTraceElement [] stackTrace;
        Throwable runtimeException = new Throwable();
        runtimeException.fillInStackTrace();
        stackTrace = runtimeException.getStackTrace();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement stackTraceElement = stackTrace[i];
            final String methodName = stackTraceElement.getMethodName();
            if (methodName.startsWith("test")) {
                return methodName;
            }
        }
        if (fail) {
            throw new RuntimeException("no testcase on current callstack");
        } else {
            return null;
        }
    }

    public void assertEquals(String tag, String expected, String actual)
    {
        final String testCaseName = getCurrentTestCaseName(true);
        String expected2 = expand(tag, expected);
        if (expected2 == null) {
            update(testCaseName, expected, actual);
            throw new AssertionError(
                "reference file does not contain resource '" + expected
                + "' for testcase '" + testCaseName
                + "'");
        } else {
            try {
                // TODO jvs 25-Apr-2006:  reuse bulk of
                // DiffTestCase.diffTestLog here; besides newline
                // insensitivity, it can report on the line
                // at which the first diff occurs, which is useful
                // for largish snippets
                String expected2Canonical =
                    expected2.replace(Util.lineSeparator, "\n");
                String actualCanonical =
                    actual.replace(Util.lineSeparator, "\n");
                Assert.assertEquals(
                    expected2Canonical,
                    actualCanonical);
            } catch (ComparisonFailure e) {
                amend(expected, actual);
                throw e;
            }
        }
    }

    /**
     * As {@link #assertEquals(String, String, String)}, but checks multiple
     * values in parallel.
     *
     * <p>If any of the values do not match, throws an {@link AssertFailure},
     * but still updates the other values. This is convenient, because if a unit
     * test needs to check N values, you can correct the logfile in 1 pass
     * through the test rather than N.
     *
     * @param tags Array of tags
     * @param expecteds Array of expected values
     * @param actuals Array of actual values
     * @param ignoreNulls Whether to ignore entries for which expected[i] ==
     * null
     */
    public void assertEqualsMulti(
        String [] tags,
        String [] expecteds,
        String [] actuals,
        boolean ignoreNulls)
    {
        final int count = tags.length;
        assert expecteds.length == count;
        assert actuals.length == count;

        AssertionError e0 = null;
        final String testCaseName = getCurrentTestCaseName(true);
        for (int i = 0; i < count; i++) {
            String tag = tags[i];
            String expected = expecteds[i];
            String actual = actuals[i];

            if (ignoreNulls) {
                if (expected == null) {
                    continue;
                }
            }
            String expected2 = expand(tag, expected);
            if (expected2 == null) {
                update(testCaseName, expected, actual);
                AssertionError e =
                    new AssertionError(
                        "reference file does not contain resource '" + expected
                        + "' for testcase '" + testCaseName
                        + "'");
                if (e0 == null) {
                    e0 = e;
                }
            } else {
                try {
                    // TODO jvs 25-Apr-2006:  reuse bulk of
                    // DiffTestCase.diffTestLog here; besides newline
                    // insensitivity, it can report on the line
                    // at which the first diff occurs, which is useful
                    // for largish snippets
                    String expected2Canonical =
                        expected2.replace(Util.lineSeparator, "\n");
                    String actualCanonical =
                        actual.replace(Util.lineSeparator, "\n");
                    Assert.assertEquals(
                        expected2Canonical,
                        actualCanonical);
                } catch (ComparisonFailure e) {
                    amend(expected, actual);
                    if (e0 == null) {
                        e0 = e;
                    }
                }
            }
        }
        if (e0 != null) {
            throw e0;
        }
    }

    /**
     * Creates a new document with a given resource.
     *
     * <p>This method is synchronized, in case two threads are running test
     * cases of this test at the same time.
     *
     * @param testCaseName Test case name
     * @param resourceName Resource name
     * @param value        New value of resource
     */
    private synchronized void update(
        String testCaseName,
        String resourceName,
        String value)
    {
        Element testCaseElement = getTestCaseElement(testCaseName, true);
        if (testCaseElement == null) {
            testCaseElement = doc.createElement(TestCaseTag);
            testCaseElement.setAttribute(TestCaseNameAttr, testCaseName);
            root.appendChild(testCaseElement);
        }
        Element resourceElement =
            getResourceElement(testCaseElement, resourceName);
        if (resourceElement == null) {
            resourceElement = doc.createElement(ResourceTag);
            resourceElement.setAttribute(ResourceNameAttr, resourceName);
            testCaseElement.appendChild(resourceElement);
        } else {
            removeAllChildren(resourceElement);
        }
        if (!value.equals("")) {
            resourceElement.appendChild(doc.createCDATASection(value));
        }

        // Write out the document.
        flushDoc();
    }

    /**
     * Flush the reference document to the file system.
     */
    private void flushDoc()
    {
        FileWriter w = null;
        try {
            w = new FileWriter(logFile);
            write(doc, w);
        } catch (IOException e) {
            throw Util.newInternal(
                e,
                "error while writing test reference log '" + logFile + "'");
        } finally {
            if (w != null) {
                try {
                    w.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Returns a given resource from a given testcase.
     *
     * @param testCaseElement The enclosing TestCase element, e.g. <code>
     * &lt;TestCase name="testFoo"&gt;</code>.
     * @param resourceName Name of resource, e.g. "sql", "plan"
     *
     * @return The value of the resource, or null if not found
     */
    private static Element getResourceElement(
        Element testCaseElement,
        String resourceName)
    {
        final NodeList childNodes = testCaseElement.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(ResourceTag)
                && resourceName.equals(
                    ((Element) child).getAttribute(ResourceNameAttr)))
            {
                return (Element) child;
            }
        }
        return null;
    }

    private static void removeAllChildren(Element element)
    {
        final NodeList childNodes = element.getChildNodes();
        while (childNodes.getLength() > 0) {
            element.removeChild(childNodes.item(0));
        }
    }

    /**
     * Serializes an XML document as text.
     *
     * <p>FIXME: I'm sure there's a library call to do this, but I'm danged if I
     * can find it. -- jhyde, 2006/2/9.
     */
    private static void write(Document doc, Writer w)
    {
        final XMLOutput out = new XMLOutput(w);
        out.setGlob(true);
        out.setIndentString("    ");
        writeNode(doc, out);
    }

    private static void writeNode(Node node, XMLOutput out)
    {
        final NodeList childNodes;
        switch (node.getNodeType()) {
        case Node.DOCUMENT_NODE:
            out.print("<?xml version=\"1.0\" ?>" + TestUtil.NL);
            childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                writeNode(child, out);
            }

            //            writeNode(((Document) node).getDocumentElement(),
            // out);
            break;

        case Node.ELEMENT_NODE:
            Element element = (Element) node;
            final String tagName = element.getTagName();
            out.beginBeginTag(tagName);

            // Attributes.
            final NamedNodeMap attributeMap = element.getAttributes();
            for (int i = 0; i < attributeMap.getLength(); i++) {
                final Node att = attributeMap.item(i);
                out.attribute(
                    att.getNodeName(),
                    att.getNodeValue());
            }
            out.endBeginTag(tagName);

            // Write child nodes, ignoring attributes but including text.
            childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                if (child.getNodeType() == Node.ATTRIBUTE_NODE) {
                    continue;
                }
                writeNode(child, out);
            }
            out.endTag(tagName);
            break;

        case Node.ATTRIBUTE_NODE:
            out.attribute(
                node.getNodeName(),
                node.getNodeValue());
            break;

        case Node.CDATA_SECTION_NODE:
            CDATASection cdata = (CDATASection) node;
            out.cdata(
                cdata.getNodeValue(),
                true);
            break;

        case Node.TEXT_NODE:
            Text text = (Text) node;
            final String wholeText = text.getNodeValue();
            if (!isWhitespace(wholeText)) {
                out.cdata(wholeText, false);
            }
            break;

        case Node.COMMENT_NODE:
            Comment comment = (Comment) node;
            out.print("<!--" + comment.getNodeValue() + "-->" + TestUtil.NL);
            break;

        default:
            throw new RuntimeException(
                "unexpected node type: " + node.getNodeType()
                + " (" + node + ")");
        }
    }

    private static boolean isWhitespace(String text)
    {
        for (int i = 0, count = text.length(); i < count; ++i) {
            final char c = text.charAt(i);
            switch (c) {
            case ' ':
            case '\t':
            case '\n':
                break;
            default:
                return false;
            }
        }
        return true;
    }

     /**
      * Finds the repository instance for a given class, with no base
      * repository or filter.
      *
      * @param clazz Testcase class
      *
      * @return The diff repository shared between testcases in this class.
      */
    public static DiffRepository lookup(Class clazz)
    {
        return lookup(clazz, null);
    }

    /**
     * Finds the repository instance for a given class and inheriting from
     * a given repository.
     *
     * @param clazz Testcase class
     * @param baseRepos Base class of test class
     *
     * @return The diff repository shared between testcases in this class.
     */
    public static DiffRepository lookup(
        Class clazz,
        DiffRepository baseRepos)
    {
        return lookup(clazz, baseRepos, null);
    }

    /**
     * Finds the repository instance for a given class.
     *
     * <p>It is important that all testcases in a class share the same
     * repository instance. This ensures that, if two or more testcases fail,
     * the log file will contains the actual results of both testcases.
     *
     * <p>The <code>baseRepos</code> parameter is useful if the test is an
     * extension to a previous test. If the test class has a base class which
     * also has a repository, specify the repository here. DiffRepository will
     * look for resources in the base class if it cannot find them in this
     * repository. If test resources from testcases in the base class are
     * missing or incorrect, it will not write them to the log file -- you
     * probably need to fix the base test.
     *
     * <p>Use the <code>filter</code> parameter if you expect the test to
     * return results slightly different than in the repository. This happens
     * if the behavior of a derived test is slightly different than a base
     * test. If you do not specify a filter, no filtering will happen.
     *
     * @param clazz Testcase class
     * @param baseRepos Base repository
     * @param filter Filters each string returned by the repository
     *
     * @return The diff repository shared between testcases in this class.
     */
    public static DiffRepository lookup(
        Class clazz,
        DiffRepository baseRepos,
        Filter filter)
    {
        DiffRepository diffRepos = mapClassToRepos.get(clazz);
        if (diffRepos == null) {
            final File refFile = findFile(clazz, ".ref.xml");
            final File logFile = findFile(clazz, ".log.xml");
            diffRepos =
                new DiffRepository(
                    refFile, logFile, baseRepos, filter);
            mapClassToRepos.put(clazz, diffRepos);
        }
        return diffRepos;
    }

    /**
     * Callback to filter strings before returning them.
     */
    public interface Filter {
        /**
         * Filters a string.
         *
         * @param diffRepository Repository
         * @param testCaseName Test case name
         * @param tag Tag being expanded
         * @param text Text being expanded
         * @param expanded Expanded text @return Expanded text after filtering
         */
        String filter(
            DiffRepository diffRepository,
            String testCaseName,
            String tag,
            String text,
            String expanded);
    }
}

// End DiffRepository.java
