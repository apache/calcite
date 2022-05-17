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
package org.apache.calcite.test.schemata.lingual;

import java.util.Objects;

/**
 * Lingual emp model.
 */
public class LingualEmp {
  public final int EMPNO;
  public final int DEPTNO;

  public LingualEmp(int EMPNO, int DEPTNO) {
    this.EMPNO = EMPNO;
    this.DEPTNO = DEPTNO;
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof LingualEmp
        && EMPNO == ((LingualEmp) obj).EMPNO;
  }

  @Override public int hashCode() {
    return Objects.hash(EMPNO);
  }
}
