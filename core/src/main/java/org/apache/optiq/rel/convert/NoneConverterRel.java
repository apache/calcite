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
package org.apache.optiq.rel.convert;

import java.util.List;

import org.apache.optiq.rel.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.util.*;

/**
 * <code>NoneConverter</code> converts a plan from <code>inConvention</code> to
 * {@link org.apache.optiq.relopt.Convention#NONE}.
 */
public class NoneConverterRel extends ConverterRelImpl {
  //~ Constructors -----------------------------------------------------------

  public NoneConverterRel(
      RelOptCluster cluster,
      RelNode child) {
    super(
        cluster,
        ConventionTraitDef.INSTANCE,
        cluster.traitSetOf(Convention.NONE),
        child);
  }

  //~ Methods ----------------------------------------------------------------


  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.comprises(Convention.NONE);
    return new NoneConverterRel(
        getCluster(),
        sole(inputs));
  }

  public static void init(RelOptPlanner planner) {
    // we can't convert from any conventions, therefore no rules to register
    Util.discard(planner);
  }
}

// End NoneConverterRel.java
