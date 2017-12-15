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
package org.apache.calcite.runtime.mr;

/**
 * Tuple Object In Match recognize
 */
public class Tuple {

  private int tupleID;
  private Object[] data;

  public Tuple(int tid, Object[] values) {
    this.tupleID = tid;
    this.data = values;
  }

  public Tuple(int length) {
    data = new Object[length];
  }

  public Tuple(Object[] dat) {
    assert dat != null;
    data = new Object[dat.length];
    System.arraycopy(dat, 0, data, 0, dat.length);
  }

  public int size() {
    assert data != null;
    return data.length;
  }

  public int getTID() {
    return tupleID;
  }

  public void setTID(int tupleID) {
    this.tupleID = tupleID;
  }

  public Object getData(int index) {
    assert index >= 0 && index < data.length;
    return data[index];
  }

  public Object[] getAllData() {
    return data;
  }

  public void setData(int index, Object value) {
    assert index >= 0 && index < data.length;
    data[index] = value;
  }

  public void setData(Object[] value) {
    assert this.size() == value.length;
    this.data = value;
  }

}

// End Tuple.java
