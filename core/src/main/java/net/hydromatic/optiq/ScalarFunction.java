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
package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

/**
 * Function that returns a scalar result.
 *
 * <p>NOTE: it is a sub-class of {@link net.hydromatic.optiq.TableFunction}
 * for a short period only. We will later introduce a common base class for
 * scalar and table functions.</p>
 */
public interface ScalarFunction extends TableFunction {
  RelDataType getReturnType(RelDataTypeFactory typeFactory);
}

// End ScalarFunction.java
