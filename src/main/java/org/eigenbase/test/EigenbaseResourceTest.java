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

import java.util.*;

import junit.framework.*;

import org.eigenbase.resource.*;


/**
 * Tests generated package org.eigenbase.resource (mostly a sanity check for
 * resgen infrastructure).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class EigenbaseResourceTest
    extends TestCase
{
    //~ Constructors -----------------------------------------------------------

    public EigenbaseResourceTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Verifies that resource properties such as SQLSTATE are available at
     * runtime.
     */
    public void testSqlstateProperty()
    {
        Properties props =
            EigenbaseResource.instance().IllegalIntervalLiteral.getProperties();
        assertEquals(
            "42000",
            props.get("SQLSTATE"));
    }
}

// End EigenbaseResourceTest.java
