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
package org.apache.calcite.runtime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class to ensure the the behavior of {@link HoistedVariables}
 */
public class HoistedVariablesTest {
  @Test
  public void registerAndSetSingleVariable() {
    HoistedVariables v = new HoistedVariables();

    // Register the variable when generating the java code.
    // This should happen in EnumerableRel's implement() method.
    final Integer variableSlot = v.registerVariable("testVariable");

    assertFalse("testVariable isn't bound yet, allVariablesBound should return false",
        v.allVariablesBound());

    // Set the variable after the code is generated but prior to it being run.
    // This should happen in EnumerableRel's hoistedVariables() method.
    final String variableValue = "testing value";
    v.setVariable(variableSlot, variableValue);
    assertTrue("testVariable is now bound, allVariablesBound should be true",
        v.allVariablesBound());

    // Finally, get the variable emulating what the compiled code does at runtime.
    // This should happen while executing Bindable's bind() method.
    assertEquals("Fetching variable out of hoistedVariables should return what was bound",
        variableValue, v.getVariable(variableSlot));
  }

  @Test(expected = IllegalArgumentException.class)
  public void fetchVariableThatIsntRegistered() {
    HoistedVariables v = new HoistedVariables();

    final Integer variableSlot = v.registerVariable("testVariable");
    final String variableValue = "testing value";
    v.setVariable(variableSlot, variableValue);

    v.getVariable(Integer.valueOf(178));
  }
}
// End HoistedVariablesTest.java
