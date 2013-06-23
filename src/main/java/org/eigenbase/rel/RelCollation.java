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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.RelTrait;


/**
 * Description of the physical ordering of a relational expression.
 *
 * <p>An ordering consists of a list of one or more column ordinals and the
 * direction of the ordering.
 *
 * @author jhyde
 * @version $Id$
 * @since March 6, 2006
 */
public interface RelCollation extends RelTrait
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the ordinals and directions of the columns in this ordering.
     */
    List<RelFieldCollation> getFieldCollations();
}

// End RelCollation.java
