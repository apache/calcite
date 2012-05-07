/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.oj;

import openjava.mop.*;

import org.eigenbase.reltype.*;


/**
 * Extended {@link RelDataTypeFactory} which can convert to and from {@link
 * openjava.mop.OJClass}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 1, 2003
 */
public interface OJTypeFactory
    extends RelDataTypeFactory
{
    //~ Methods ----------------------------------------------------------------

    OJClass toOJClass(
        OJClass declarer,
        RelDataType type);

    RelDataType toType(OJClass ojClass);
}

// End OJTypeFactory.java
