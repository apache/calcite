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

import org.eigenbase.util.property.*;


/**
 * PersistentPropertyTest tests persistent properties using temporary files.
 *
 * @author Stephan Zuercher
 * @version $Id$
 * @since December 3, 2004
 */
public class PersistentPropertyTest
    extends EigenbaseTestCase
{
    //~ Constructors -----------------------------------------------------------

    public PersistentPropertyTest(String name)
        throws Exception
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testPersistentStringProperty()
        throws Exception
    {
        final String DEFAULT_VALUE = "default value";
        final String NEW_VALUE = "new value";
        final String PROP_NAME = "test.eigenbase.persistent.string";
        final String EXISTING_PROP_NAME1 = "test.eigenbase.existing1";
        final String EXISTING_PROP_VALUE1 = "existing value 1";
        final String EXISTING_PROP_NAME2 = "test.eigenbase.existing2";
        final String EXISTING_PROP_VALUE2 = "existing value 2";
        final String EXISTING_PROP_NAME3 = "test.eigenbase.existing3";
        final String EXISTING_PROP_VALUE3 = "existing value 3";
        final String EXISTING_NEW_VALUE = "new value for existing prop";
        final String EXISTING_DEFAULT_VALUE = "existing default value";

        File tempPropFile = File.createTempFile("eigenbaseTest", ".properties");
        BufferedWriter writer =
            new BufferedWriter(new FileWriter(tempPropFile));
        writer.write("# Test config file");
        writer.newLine();
        writer.newLine();
        writer.write(EXISTING_PROP_NAME1 + "=" + EXISTING_PROP_VALUE1);
        writer.newLine();
        writer.write(EXISTING_PROP_NAME2 + "=" + EXISTING_PROP_VALUE2);
        writer.newLine();
        writer.write(EXISTING_PROP_NAME3 + "=" + EXISTING_PROP_VALUE3);
        writer.newLine();
        writer.flush();
        writer.close();

        Properties props = new Properties();
        props.load(new FileInputStream(tempPropFile));

        StringProperty propertyFileLocation =
            new StringProperty(
                props,
                "test.eigenbase.properties",
                tempPropFile.getAbsolutePath());

        PersistentStringProperty persistentProperty =
            new PersistentStringProperty(
                props,
                PROP_NAME,
                DEFAULT_VALUE,
                propertyFileLocation);

        PersistentStringProperty persistentExistingProperty =
            new PersistentStringProperty(
                props,
                EXISTING_PROP_NAME2,
                EXISTING_DEFAULT_VALUE,
                propertyFileLocation);

        assertEquals(
            DEFAULT_VALUE,
            persistentProperty.get());
        assertNull(props.getProperty(PROP_NAME));
        assertEquals(
            EXISTING_PROP_VALUE1,
            props.getProperty(EXISTING_PROP_NAME1));
        assertEquals(
            EXISTING_PROP_VALUE2,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_PROP_VALUE2,
            props.getProperty(EXISTING_PROP_NAME2));
        assertEquals(
            EXISTING_PROP_VALUE3,
            props.getProperty(EXISTING_PROP_NAME3));

        persistentProperty.set(NEW_VALUE);

        assertEquals(
            NEW_VALUE,
            persistentProperty.get());
        assertEquals(
            NEW_VALUE,
            props.getProperty(PROP_NAME));

        persistentExistingProperty.set(EXISTING_NEW_VALUE);

        assertEquals(
            EXISTING_NEW_VALUE,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_NEW_VALUE,
            props.getProperty(EXISTING_PROP_NAME2));

        // reset properties, location and persistent property (reloads
        // properties stored in file)
        props = new Properties();
        props.load(new FileInputStream(tempPropFile));

        propertyFileLocation =
            new StringProperty(
                props,
                "test.eigenbase.properties",
                tempPropFile.getAbsolutePath());

        persistentProperty =
            new PersistentStringProperty(
                props,
                PROP_NAME,
                DEFAULT_VALUE,
                propertyFileLocation);

        assertEquals(
            NEW_VALUE,
            persistentProperty.get());
        assertEquals(
            NEW_VALUE,
            props.getProperty(PROP_NAME));

        assertEquals(
            EXISTING_NEW_VALUE,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_NEW_VALUE,
            props.getProperty(EXISTING_PROP_NAME2));

        assertEquals(
            EXISTING_PROP_VALUE1,
            props.getProperty(EXISTING_PROP_NAME1));
        assertEquals(
            EXISTING_PROP_VALUE3,
            props.getProperty(EXISTING_PROP_NAME3));

        // delete file if test succeeded
        tempPropFile.delete();
    }
}

// End PersistentPropertyTest.java
