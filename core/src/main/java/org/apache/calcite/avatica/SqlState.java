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
package org.apache.calcite.avatica;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * SQL error codes.
 *
 * <p>Based upon Table 33 &mdash; SQLSTATE class and subclass values in
 * SQL:2014 section 24.1, which is as follows.
 *
 * <table border=1>
 * <caption>Table 33 &mdash; SQLSTATE class and subclass values</caption>
 * <tr>
 *   <th>Category</th>
 *   <th>Condition</th>
 *   <th>Class</th>
 *   <th>Subcondition</th>
 *   <th>Subclass</th>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>ambiguous cursor name</td>
 *   <td>3C</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>attempt to assign to non-updatable column</td>
 *   <td>0U</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>attempt to assign to ordering column</td>
 *   <td>0V</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>cli specific condition</td>
 *   <td>HY</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>cardinality violation</td>
 *   <td>21</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>connection exception</td>
 *   <td>08</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>connection does not exist</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>connection failure</td>
 *   <td>006</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>connection name in use</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>SQL-client unable to establish SQL-connection</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>SQL-server rejected establishment of SQL-connection</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>transaction resolution unknown</td>
 *   <td>007</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>cursor sensitivity exception</td>
 *   <td>36</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>request failed</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>request rejected</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>data exception</td>
 *   <td>22</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>array data, right truncation</td>
 *   <td>02F</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>array element error</td>
 *   <td>02E</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>attempt to replace a zero-length string</td>
 *   <td>01U</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>character not in repertoire</td>
 *   <td>021</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>datetime field overflow</td>
 *   <td>008</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>division by zero</td>
 *   <td>012</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>error in assignment</td>
 *   <td>005</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>escape character conflict</td>
 *   <td>00B</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>indicator overflow</td>
 *   <td>022</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>interval field overflow</td>
 *   <td>015</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>interval value out of range</td>
 *   <td>00P</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for natural logarithm</td>
 *   <td>01E</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for NTILE function</td>
 *   <td>014</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for NTH_VALUE function</td>
 *   <td>016</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for power function</td>
 *   <td>01F</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for row pattern navigation operation</td>
 *   <td>02J</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid argument for width bucket function</td>
 *   <td>01G</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid character value for cast</td>
 *   <td>018</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid datetime format</td>
 *   <td>007</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid escape character</td>
 *   <td>019</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid escape octet</td>
 *   <td>00D</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid escape sequence</td>
 *   <td>025</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid indicator parameter value</td>
 *   <td>010</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid interval format</td>
 *   <td>006</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid parameter value</td>
 *   <td>023</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid period value</td>
 *   <td>020</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid preceding or following size in window function</td>
 *   <td>013</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid regular expression</td>
 *   <td>01B</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid repeat argument in a sample clause</td>
 *   <td>02G</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid row count in fetch first clause</td>
 *   <td>01W</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid row count in result offset clause</td>
 *   <td>01X</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid row version</td>
 *   <td>01H</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid sample size</td>
 *   <td>02H</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid time zone displacement value</td>
 *   <td>009</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid use of escape character</td>
 *   <td>00C</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid XQuery option flag</td>
 *   <td>01T</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid XQuery regular expression</td>
 *   <td>01S</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid XQuery replacement string</td>
 *   <td>01V</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>most specific type mismatch</td>
 *   <td>00G</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>multiset value overflow</td>
 *   <td>00Q</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>noncharacter in UCS string</td>
 *   <td>029</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value substituted for mutator subject parameter</td>
 *   <td>02D</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null row not permitted in table</td>
 *   <td>01C</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value in array target</td>
 *   <td>00E</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value, no indicator parameter</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value not allowed</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>numeric value out of range</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>sequence generator limit exceeded</td>
 *   <td>00H</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>skip to non-existent row</td>
 *   <td>02K</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>skip to first row of match</td>
 *   <td>02L</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>string data, length mismatch</td>
 *   <td>026</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>string data, right truncation</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>substring error</td>
 *   <td>011</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>trim error</td>
 *   <td>027</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>unterminated C string</td>
 *   <td>024</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>zero-length character string</td>
 *   <td>00F</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>dependent privilege descriptors still exist</td>
 *   <td>2B</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>diagnostics exception</td>
 *   <td>0Z</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>maximum number of stacked diagnostics areas exceeded</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>dynamic SQL error</td>
 *   <td>07</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>cursor specification cannot be executed</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>data type transform function violation</td>
 *   <td>00B</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid DATA target</td>
 *   <td>00D</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid DATETIME_INTERVAL_CODE</td>
 *   <td>00F</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid descriptor count</td>
 *   <td>008</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid descriptor index</td>
 *   <td>009</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid LEVEL value</td>
 *   <td>00E</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>prepared statement not a cursor specification</td>
 *   <td>005</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>restricted data type attribute violation</td>
 *   <td>006</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>undefined DATA value</td>
 *   <td>00C</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>using clause does not match dynamic parameter specifications</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>using clause does not match target specifications</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>using clause required for dynamic parameters</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>using clause required for result fields</td>
 *   <td>007</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>external routine exception</td>
 *   <td>38</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>containing SQL not permitted</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>modifying SQL-data not permitted</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>prohibited SQL-statement attempted</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>reading SQL-data not permitted</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>external routine invocation exception</td>
 *   <td>39</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value not allowed</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>feature not supported</td>
 *   <td>0A</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>multiple server transactions</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>integrity constraint violation</td>
 *   <td>23</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>restrict violation</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid authorization specification</td>
 *   <td>28</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid catalog name</td>
 *   <td>3D</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid character set name</td>
 *   <td>2C</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>cannot drop SQL-session default character set</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid condition number</td>
 *   <td>35</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid connection name</td>
 *   <td>2E</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid cursor name</td>
 *   <td>34</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid cursor state</td>
 *   <td>24</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid grantor</td>
 *   <td>0L</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid role specification</td>
 *   <td>0P</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid schema name</td>
 *   <td>3F</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid schema name list specification</td>
 *   <td>0E</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid collation name</td>
 *   <td>2H</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid SQL descriptor name</td>
 *   <td>33</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid SQL-invoked procedure reference</td>
 *   <td>0M</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid SQL statement name</td>
 *   <td>26</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid SQL statement identifier</td>
 *   <td>30</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid target type specification</td>
 *   <td>0D</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid transaction state</td>
 *   <td>25</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>active SQL-transaction</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>branch transaction already active</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>held cursor requires same isolation level</td>
 *   <td>008</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>inappropriate access mode for branch transaction</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>inappropriate isolation level for branch transaction</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>no active SQL-transaction for branch transaction</td>
 *   <td>005</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>read-only SQL-transaction</td>
 *   <td>006</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>schema and data statement mixing not supported</td>
 *   <td>007</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid transaction termination</td>
 *   <td>2D</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>invalid transform group name specification</td>
 *   <td>0S</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>locator exception</td>
 *   <td>0F</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid specification</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>N</td>
 *   <td>no data</td>
 *   <td>02</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>no additional result sets returned</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>prohibited statement encountered during trigger execution</td>
 *   <td>0W</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>modify table modified by data change delta table</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>Remote Database Access</td>
 *   <td>HZ</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>savepoint exception</td>
 *   <td>3B</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid specification</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>too many</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>SQL routine exception</td>
 *   <td>2F</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>function executed no return statement</td>
 *   <td>005</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>modifying SQL-data not permitted</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>prohibited SQL-statement attempted</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>reading SQL-data not permitted</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>S</td>
 *   <td>successful completion</td>
 *   <td>00</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>syntax error or access rule violation</td>
 *   <td>42</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>syntax error or access rule violation in direct statement</td>
 *   <td>2A</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>syntax error or access rule violation in dynamic statement</td>
 *   <td>37</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>target table disagrees with cursor specification</td>
 *   <td>0T</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>transaction rollback</td>
 *   <td>40</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>integrity constraint violation</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>serialization failure</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>statement completion unknown</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>triggered action exception</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>triggered action exception</td>
 *   <td>09</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>triggered data change violation</td>
 *   <td>27</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>modify table modified by data change delta table</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>W</td>
 *   <td>warning</td>
 *   <td>01</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>additional result sets returned</td>
 *   <td>00D</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>array data, right truncation</td>
 *   <td>02F</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>attempt to return too many result sets</td>
 *   <td>00E</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>cursor operation conflict</td>
 *   <td>001</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>default value too long for information schema</td>
 *   <td>00B</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>disconnect error</td>
 *   <td>002</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>insufficient item descriptor areas</td>
 *   <td>005</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>invalid number of conditions</td>
 *   <td>012</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>null value eliminated in set function</td>
 *   <td>003</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>privilege not granted</td>
 *   <td>007</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>privilege not revoked</td>
 *   <td>006</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>query expression too long for information schema</td>
 *   <td>00A</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>result sets returned</td>
 *   <td>00C</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>search condition too long for information schema</td>
 *   <td>009</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>statement too long for information schema</td>
 *   <td>00F</td>
 * </tr>
 * <tr>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>&nbsp;</td>
 *   <td>string data, right truncation</td>
 *   <td>004</td>
 * </tr>
 * <tr>
 *   <td>X</td>
 *   <td>with check option violation</td>
 *   <td>44</td>
 *   <td>(no subclass)</td>
 *   <td>000</td>
 * </tr>
 * </table>
 */
public enum SqlState {
  /** 3C000: ambiguous cursor name */
  AMBIGUOUS_CURSOR_NAME_NO_SUBCLASS(Category.X, "ambiguous cursor name", "3C", null, null),
  /** 0U000: attempt to assign to non-updatable column */
  ATTEMPT_TO_ASSIGN_TO_NON_UPDATABLE_COLUMN_NO_SUBCLASS(Category.X,
      "attempt to assign to non-updatable column", "0U", null, null),
  /** 0V000: attempt to assign to ordering column */
  ATTEMPT_TO_ASSIGN_TO_ORDERING_COLUMN_NO_SUBCLASS(Category.X,
      "attempt to assign to ordering column", "0V", null, null),
  /** HY000: cli specific condition */
  CLI_SPECIFIC_CONDITION_NO_SUBCLASS(Category.X, "cli specific condition", "HY", null, null),
  /** 21000: cardinality violation */
  CARDINALITY_VIOLATION_NO_SUBCLASS(Category.X, "cardinality violation", "21", null, null),
  /** 08000: connection exception */
  CONNECTION_EXCEPTION_NO_SUBCLASS(Category.X, "connection exception", "08", null, null),
  /** 08003: connection exception: connection does not exist */
  CONNECTION_EXCEPTION_CONNECTION_DOES_NOT_EXIST(Category.X, "connection exception", "08",
      "connection does not exist", "003"),
  /** 08006: connection exception: connection failure */
  CONNECTION_EXCEPTION_CONNECTION_FAILURE(Category.X, "connection exception", "08",
      "connection failure", "006"),
  /** 08002: connection exception: connection name in use */
  CONNECTION_EXCEPTION_CONNECTION_NAME_IN_USE(Category.X, "connection exception", "08",
      "connection name in use", "002"),
  /** 08001: connection exception: SQL-client unable to establish SQL-connection */
  CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION(Category.X,
      "connection exception", "08", "SQL-client unable to establish SQL-connection", "001"),
  /** 08004: connection exception: SQL-server rejected establishment of SQL-connection */
  CONNECTION_EXCEPTION_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION(Category.X,
      "connection exception", "08", "SQL-server rejected establishment of SQL-connection", "004"),
  /** 08007: connection exception: transaction resolution unknown */
  CONNECTION_EXCEPTION_TRANSACTION_RESOLUTION_UNKNOWN(Category.X, "connection exception", "08",
      "transaction resolution unknown", "007"),
  /** 36000: cursor sensitivity exception */
  CURSOR_SENSITIVITY_EXCEPTION_NO_SUBCLASS(Category.X, "cursor sensitivity exception", "36", null,
      null),
  /** 36002: cursor sensitivity exception: request failed */
  CURSOR_SENSITIVITY_EXCEPTION_REQUEST_FAILED(Category.X, "cursor sensitivity exception", "36",
      "request failed", "002"),
  /** 36001: cursor sensitivity exception: request rejected */
  CURSOR_SENSITIVITY_EXCEPTION_REQUEST_REJECTED(Category.X, "cursor sensitivity exception", "36",
      "request rejected", "001"),
  /** 22000: data exception */
  DATA_EXCEPTION_NO_SUBCLASS(Category.X, "data exception", "22", null, null),
  /** 2202F: data exception: array data, right truncation */
  DATA_EXCEPTION_ARRAY_DATA_RIGHT_TRUNCATION(Category.X, "data exception", "22",
      "array data, right truncation", "02F"),
  /** 2202E: data exception: array element error */
  DATA_EXCEPTION_ARRAY_ELEMENT_ERROR(Category.X, "data exception", "22", "array element error",
      "02E"),
  /** 2201U: data exception: attempt to replace a zero-length string */
  DATA_EXCEPTION_ATTEMPT_TO_REPLACE_A_ZERO_LENGTH_STRING(Category.X, "data exception", "22",
      "attempt to replace a zero-length string", "01U"),
  /** 22021: data exception: character not in repertoire */
  DATA_EXCEPTION_CHARACTER_NOT_IN_REPERTOIRE(Category.X, "data exception", "22",
      "character not in repertoire", "021"),
  /** 22008: data exception: datetime field overflow */
  DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW(Category.X, "data exception", "22",
      "datetime field overflow", "008"),
  /** 22012: data exception: division by zero */
  DATA_EXCEPTION_DIVISION_BY_ZERO(Category.X, "data exception", "22", "division by zero", "012"),
  /** 22005: data exception: error in assignment */
  DATA_EXCEPTION_ERROR_IN_ASSIGNMENT(Category.X, "data exception", "22", "error in assignment",
      "005"),
  /** 2200B: data exception: escape character conflict */
  DATA_EXCEPTION_ESCAPE_CHARACTER_CONFLICT(Category.X, "data exception", "22",
      "escape character conflict", "00B"),
  /** 22022: data exception: indicator overflow */
  DATA_EXCEPTION_INDICATOR_OVERFLOW(Category.X, "data exception", "22", "indicator overflow",
      "022"),
  /** 22015: data exception: interval field overflow */
  DATA_EXCEPTION_INTERVAL_FIELD_OVERFLOW(Category.X, "data exception", "22",
      "interval field overflow", "015"),
  /** 2200P: data exception: interval value out of range */
  DATA_EXCEPTION_INTERVAL_VALUE_OUT_OF_RANGE(Category.X, "data exception", "22",
      "interval value out of range", "00P"),
  /** 2201E: data exception: invalid argument for natural logarithm */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_NATURAL_LOGARITHM(Category.X, "data exception", "22",
      "invalid argument for natural logarithm", "01E"),
  /** 22014: data exception: invalid argument for NTILE function */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_NTILE_FUNCTION(Category.X, "data exception", "22",
      "invalid argument for NTILE function", "014"),
  /** 22016: data exception: invalid argument for NTH_VALUE function */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION(Category.X, "data exception", "22",
      "invalid argument for NTH_VALUE function", "016"),
  /** 2201F: data exception: invalid argument for power function */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_POWER_FUNCTION(Category.X, "data exception", "22",
      "invalid argument for power function", "01F"),
  /** 2202J: data exception: invalid argument for row pattern navigation operation */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_ROW_PATTERN_NAVIGATION_OPERATION(Category.X, "data exception",
      "22", "invalid argument for row pattern navigation operation", "02J"),
  /** 2201G: data exception: invalid argument for width bucket function */
  DATA_EXCEPTION_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION(Category.X, "data exception", "22",
      "invalid argument for width bucket function", "01G"),
  /** 22018: data exception: invalid character value for cast */
  DATA_EXCEPTION_INVALID_CHARACTER_VALUE_FOR_CAST(Category.X, "data exception", "22",
      "invalid character value for cast", "018"),
  /** 22007: data exception: invalid datetime format */
  DATA_EXCEPTION_INVALID_DATETIME_FORMAT(Category.X, "data exception", "22",
      "invalid datetime format", "007"),
  /** 22019: data exception: invalid escape character */
  DATA_EXCEPTION_INVALID_ESCAPE_CHARACTER(Category.X, "data exception", "22",
      "invalid escape character", "019"),
  /** 2200D: data exception: invalid escape octet */
  DATA_EXCEPTION_INVALID_ESCAPE_OCTET(Category.X, "data exception", "22", "invalid escape octet",
      "00D"),
  /** 22025: data exception: invalid escape sequence */
  DATA_EXCEPTION_INVALID_ESCAPE_SEQUENCE(Category.X, "data exception", "22",
      "invalid escape sequence", "025"),
  /** 22010: data exception: invalid indicator parameter value */
  DATA_EXCEPTION_INVALID_INDICATOR_PARAMETER_VALUE(Category.X, "data exception", "22",
      "invalid indicator parameter value", "010"),
  /** 22006: data exception: invalid interval format */
  DATA_EXCEPTION_INVALID_INTERVAL_FORMAT(Category.X, "data exception", "22",
      "invalid interval format", "006"),
  /** 22023: data exception: invalid parameter value */
  DATA_EXCEPTION_INVALID_PARAMETER_VALUE(Category.X, "data exception", "22",
      "invalid parameter value", "023"),
  /** 22020: data exception: invalid period value */
  DATA_EXCEPTION_INVALID_PERIOD_VALUE(Category.X, "data exception", "22", "invalid period value",
      "020"),
  /** 22013: data exception: invalid preceding or following size in window function */
  DATA_EXCEPTION_INVALID_PRECEDING_OR_FOLLOWING_SIZE_IN_WINDOW_FUNCTION(Category.X,
      "data exception", "22", "invalid preceding or following size in window function", "013"),
  /** 2201B: data exception: invalid regular expression */
  DATA_EXCEPTION_INVALID_REGULAR_EXPRESSION(Category.X, "data exception", "22",
      "invalid regular expression", "01B"),
  /** 2202G: data exception: invalid repeat argument in a sample clause */
  DATA_EXCEPTION_INVALID_REPEAT_ARGUMENT_IN_A_SAMPLE_CLAUSE(Category.X, "data exception", "22",
      "invalid repeat argument in a sample clause", "02G"),
  /** 2201W: data exception: invalid row count in fetch first clause */
  DATA_EXCEPTION_INVALID_ROW_COUNT_IN_FETCH_FIRST_CLAUSE(Category.X, "data exception", "22",
      "invalid row count in fetch first clause", "01W"),
  /** 2201X: data exception: invalid row count in result offset clause */
  DATA_EXCEPTION_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE(Category.X, "data exception", "22",
      "invalid row count in result offset clause", "01X"),
  /** 2201H: data exception: invalid row version */
  DATA_EXCEPTION_INVALID_ROW_VERSION(Category.X, "data exception", "22", "invalid row version",
      "01H"),
  /** 2202H: data exception: invalid sample size */
  DATA_EXCEPTION_INVALID_SAMPLE_SIZE(Category.X, "data exception", "22", "invalid sample size",
      "02H"),
  /** 22009: data exception: invalid time zone displacement value */
  DATA_EXCEPTION_INVALID_TIME_ZONE_DISPLACEMENT_VALUE(Category.X, "data exception", "22",
      "invalid time zone displacement value", "009"),
  /** 2200C: data exception: invalid use of escape character */
  DATA_EXCEPTION_INVALID_USE_OF_ESCAPE_CHARACTER(Category.X, "data exception", "22",
      "invalid use of escape character", "00C"),
  /** 2201T: data exception: invalid XQuery option flag */
  DATA_EXCEPTION_INVALID_XQUERY_OPTION_FLAG(Category.X, "data exception", "22",
      "invalid XQuery option flag", "01T"),
  /** 2201S: data exception: invalid XQuery regular expression */
  DATA_EXCEPTION_INVALID_XQUERY_REGULAR_EXPRESSION(Category.X, "data exception", "22",
      "invalid XQuery regular expression", "01S"),
  /** 2201V: data exception: invalid XQuery replacement string */
  DATA_EXCEPTION_INVALID_XQUERY_REPLACEMENT_STRING(Category.X, "data exception", "22",
      "invalid XQuery replacement string", "01V"),
  /** 2200G: data exception: most specific type mismatch */
  DATA_EXCEPTION_MOST_SPECIFIC_TYPE_MISMATCH(Category.X, "data exception", "22",
      "most specific type mismatch", "00G"),
  /** 2200Q: data exception: multiset value overflow */
  DATA_EXCEPTION_MULTISET_VALUE_OVERFLOW(Category.X, "data exception", "22",
      "multiset value overflow", "00Q"),
  /** 22029: data exception: noncharacter in UCS string */
  DATA_EXCEPTION_NONCHARACTER_IN_UCS_STRING(Category.X, "data exception", "22",
      "noncharacter in UCS string", "029"),
  /** 2202D: data exception: null value substituted for mutator subject parameter */
  DATA_EXCEPTION_NULL_VALUE_SUBSTITUTED_FOR_MUTATOR_SUBJECT_PARAMETER(Category.X, "data exception",
      "22", "null value substituted for mutator subject parameter", "02D"),
  /** 2201C: data exception: null row not permitted in table */
  DATA_EXCEPTION_NULL_ROW_NOT_PERMITTED_IN_TABLE(Category.X, "data exception", "22",
      "null row not permitted in table", "01C"),
  /** 2200E: data exception: null value in array target */
  DATA_EXCEPTION_NULL_VALUE_IN_ARRAY_TARGET(Category.X, "data exception", "22",
      "null value in array target", "00E"),
  /** 22002: data exception: null value, no indicator parameter */
  DATA_EXCEPTION_NULL_VALUE_NO_INDICATOR_PARAMETER(Category.X, "data exception", "22",
      "null value, no indicator parameter", "002"),
  /** 22004: data exception: null value not allowed */
  DATA_EXCEPTION_NULL_VALUE_NOT_ALLOWED(Category.X, "data exception", "22",
      "null value not allowed", "004"),
  /** 22003: data exception: numeric value out of range */
  DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE(Category.X, "data exception", "22",
      "numeric value out of range", "003"),
  /** 2200H: data exception: sequence generator limit exceeded */
  DATA_EXCEPTION_SEQUENCE_GENERATOR_LIMIT_EXCEEDED(Category.X, "data exception", "22",
      "sequence generator limit exceeded", "00H"),
  /** 2202K: data exception: skip to non-existent row */
  DATA_EXCEPTION_SKIP_TO_NON_EXISTENT_ROW(Category.X, "data exception", "22",
      "skip to non-existent row", "02K"),
  /** 2202L: data exception: skip to first row of match */
  DATA_EXCEPTION_SKIP_TO_FIRST_ROW_OF_MATCH(Category.X, "data exception", "22",
      "skip to first row of match", "02L"),
  /** 22026: data exception: string data, length mismatch */
  DATA_EXCEPTION_STRING_DATA_LENGTH_MISMATCH(Category.X, "data exception", "22",
      "string data, length mismatch", "026"),
  /** 22001: data exception: string data, right truncation */
  DATA_EXCEPTION_STRING_DATA_RIGHT_TRUNCATION(Category.X, "data exception", "22",
      "string data, right truncation", "001"),
  /** 22011: data exception: substring error */
  DATA_EXCEPTION_SUBSTRING_ERROR(Category.X, "data exception", "22", "substring error", "011"),
  /** 22027: data exception: trim error */
  DATA_EXCEPTION_TRIM_ERROR(Category.X, "data exception", "22", "trim error", "027"),
  /** 22024: data exception: unterminated C string */
  DATA_EXCEPTION_UNTERMINATED_C_STRING(Category.X, "data exception", "22", "unterminated C string",
      "024"),
  /** 2200F: data exception: zero-length character string */
  DATA_EXCEPTION_ZERO_LENGTH_CHARACTER_STRING(Category.X, "data exception", "22",
      "zero-length character string", "00F"),
  /** 2B000: dependent privilege descriptors still exist */
  DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST_NO_SUBCLASS(Category.X,
      "dependent privilege descriptors still exist", "2B", null, null),
  /** 0Z000: diagnostics exception */
  DIAGNOSTICS_EXCEPTION_NO_SUBCLASS(Category.X, "diagnostics exception", "0Z", null, null),
  /** 0Z001: diagnostics exception: maximum number of stacked diagnostics areas exceeded */
  DIAGNOSTICS_EXCEPTION_MAXIMUM_NUMBER_OF_DIAGNOSTICS_AREAS_EXCEEDED(Category.X,
      "diagnostics exception", "0Z", "maximum number of stacked diagnostics areas exceeded", "001"),
  /** 07000: dynamic SQL error */
  DYNAMIC_SQL_ERROR_NO_SUBCLASS(Category.X, "dynamic SQL error", "07", null, null),
  /** 07003: dynamic SQL error: cursor specification cannot be executed */
  DYNAMIC_SQL_ERROR_CURSOR_SPECIFICATION_CANNOT_BE_EXECUTED(Category.X, "dynamic SQL error", "07",
      "cursor specification cannot be executed", "003"),
  /** 0700B: dynamic SQL error: data type transform function violation */
  DYNAMIC_SQL_ERROR_DATA_TYPE_TRANSFORM_FUNCTION_VIOLATION(Category.X, "dynamic SQL error", "07",
      "data type transform function violation", "00B"),
  /** 0700D: dynamic SQL error: invalid DATA target */
  DYNAMIC_SQL_ERROR_INVALID_DATA_TARGET(Category.X, "dynamic SQL error", "07",
      "invalid DATA target", "00D"),
  /** 0700F: dynamic SQL error: invalid DATETIME_INTERVAL_CODE */
  DYNAMIC_SQL_ERROR_INVALID_DATETIME_INTERVAL_CODE(Category.X, "dynamic SQL error", "07",
      "invalid DATETIME_INTERVAL_CODE", "00F"),
  /** 07008: dynamic SQL error: invalid descriptor count */
  DYNAMIC_SQL_ERROR_INVALID_DESCRIPTOR_COUNT(Category.X, "dynamic SQL error", "07",
      "invalid descriptor count", "008"),
  /** 07009: dynamic SQL error: invalid descriptor index */
  DYNAMIC_SQL_ERROR_INVALID_DESCRIPTOR_INDEX(Category.X, "dynamic SQL error", "07",
      "invalid descriptor index", "009"),
  /** 0700E: dynamic SQL error: invalid LEVEL value */
  DYNAMIC_SQL_ERROR_INVALID_LEVEL_VALUE(Category.X, "dynamic SQL error", "07",
      "invalid LEVEL value", "00E"),
  /** 07005: dynamic SQL error: prepared statement not a cursor specification */
  DYNAMIC_SQL_ERROR_PREPARED_STATEMENT_NOT_A_CURSOR_SPECIFICATION(
      Category.X, "dynamic SQL error", "07",
      "prepared statement not a cursor specification", "005"),
  /** 07006: dynamic SQL error: restricted data type attribute violation */
  DYNAMIC_SQL_ERROR_RESTRICTED_DATA_TYPE_ATTRIBUTE_VIOLATION(
      Category.X, "dynamic SQL error", "07", "restricted data type attribute violation", "006"),
  /** 0700C: dynamic SQL error: undefined DATA value */
  DYNAMIC_SQL_ERROR_UNDEFINED_DATA_VALUE(Category.X, "dynamic SQL error", "07",
      "undefined DATA value", "00C"),
  /** 07001: dynamic SQL error: using clause does not match dynamic parameter specifications */
  DYNAMIC_SQL_ERROR_USING_CLAUSE_DOES_NOT_MATCH_DYNAMIC_PARAMETER_SPEC(Category.X,
      "dynamic SQL error", "07", "using clause does not match dynamic parameter specifications",
      "001"),
  /** 07002: dynamic SQL error: using clause does not match target specifications */
  DYNAMIC_SQL_ERROR_USING_CLAUSE_DOES_NOT_MATCH_TARGET_SPEC(Category.X,
      "dynamic SQL error", "07", "using clause does not match target specifications", "002"),
  /** 07004: dynamic SQL error: using clause required for dynamic parameters */
  DYNAMIC_SQL_ERROR_USING_CLAUSE_REQUIRED_FOR_DYNAMIC_PARAMETERS(Category.X, "dynamic SQL error",
      "07", "using clause required for dynamic parameters", "004"),
  /** 07007: dynamic SQL error: using clause required for result fields */
  DYNAMIC_SQL_ERROR_USING_CLAUSE_REQUIRED_FOR_RESULT_FIELDS(Category.X, "dynamic SQL error", "07",
      "using clause required for result fields", "007"),
  /** 38000: external routine exception */
  EXTERNAL_ROUTINE_EXCEPTION_NO_SUBCLASS(Category.X, "external routine exception", "38", null,
      null),
  /** 38001: external routine exception: containing SQL not permitted */
  EXTERNAL_ROUTINE_EXCEPTION_CONTAINING_SQL_NOT_PERMITTED(Category.X, "external routine exception",
      "38", "containing SQL not permitted", "001"),
  /** 38002: external routine exception: modifying SQL-data not permitted */
  EXTERNAL_ROUTINE_EXCEPTION_MODIFYING_SQL_DATA_NOT_PERMITTED(Category.X,
      "external routine exception", "38", "modifying SQL-data not permitted", "002"),
  /** 38003: external routine exception: prohibited SQL-statement attempted */
  EXTERNAL_ROUTINE_EXCEPTION_PROHIBITED_SQL_STATEMENT_ATTEMPTED(Category.X,
      "external routine exception", "38", "prohibited SQL-statement attempted", "003"),
  /** 38004: external routine exception: reading SQL-data not permitted */
  EXTERNAL_ROUTINE_EXCEPTION_READING_SQL_DATA_NOT_PERMITTED(Category.X,
      "external routine exception", "38", "reading SQL-data not permitted", "004"),
  /** 39000: external routine invocation exception */
  EXTERNAL_ROUTINE_INVOCATION_EXCEPTION_NO_SUBCLASS(Category.X,
      "external routine invocation exception", "39", null, null),
  /** 39004: external routine invocation exception: null value not allowed */
  EXTERNAL_ROUTINE_INVOCATION_EXCEPTION_NULL_VALUE_NOT_ALLOWED(Category.X,
      "external routine invocation exception", "39", "null value not allowed", "004"),
  /** 0A000: feature not supported */
  FEATURE_NOT_SUPPORTED_NO_SUBCLASS(Category.X, "feature not supported", "0A", null, null),
  /** 0A001: feature not supported: multiple server transactions */
  FEATURE_NOT_SUPPORTED_MULTIPLE_ENVIRONMENT_TRANSACTIONS(Category.X, "feature not supported", "0A",
      "multiple server transactions", "001"),
  /** 23000: integrity constraint violation */
  INTEGRITY_CONSTRAINT_VIOLATION_NO_SUBCLASS(Category.X, "integrity constraint violation", "23",
      null, null),
  /** 23001: integrity constraint violation: restrict violation */
  INTEGRITY_CONSTRAINT_VIOLATION_RESTRICT_VIOLATION(Category.X, "integrity constraint violation",
      "23", "restrict violation", "001"),
  /** 28000: invalid authorization specification */
  INVALID_AUTHORIZATION_SPECIFICATION_NO_SUBCLASS(Category.X, "invalid authorization specification",
      "28", null, null),
  /** 3D000: invalid catalog name */
  INVALID_CATALOG_NAME_NO_SUBCLASS(Category.X, "invalid catalog name", "3D", null, null),
  /** 2C000: invalid character set name */
  INVALID_CHARACTER_SET_NAME_NO_SUBCLASS(Category.X, "invalid character set name", "2C", null,
      null),
  /** 2C001: invalid character set name: cannot drop SQL-session default character set */
  INVALID_CHARACTER_SET_NAME_CANNOT_DROP_SQLSESSION_DEFAULT_CHARACTER_SET(Category.X,
      "invalid character set name", "2C", "cannot drop SQL-session default character set", "001"),
  /** 35000: invalid condition number */
  INVALID_CONDITION_NUMBER_NO_SUBCLASS(Category.X, "invalid condition number", "35", null, null),
  /** 2E000: invalid connection name */
  INVALID_CONNECTION_NAME_NO_SUBCLASS(Category.X, "invalid connection name", "2E", null, null),
  /** 34000: invalid cursor name */
  INVALID_CURSOR_NAME_NO_SUBCLASS(Category.X, "invalid cursor name", "34", null, null),
  /** 24000: invalid cursor state */
  INVALID_CURSOR_STATE_NO_SUBCLASS(Category.X, "invalid cursor state", "24", null, null),
  /** 0L000: invalid grantor */
  INVALID_GRANTOR_STATE_NO_SUBCLASS(Category.X, "invalid grantor", "0L", null, null),
  /** 0P000: invalid role specification */
  INVALID_ROLE_SPECIFICATION(Category.X, "invalid role specification", "0P", null,
      null),
  /** 3F000: invalid schema name */
  INVALID_SCHEMA_NAME_NO_SUBCLASS(Category.X, "invalid schema name", "3F", null, null),
  /** 0E000: invalid schema name list specification */
  INVALID_SCHEMA_NAME_LIST_SPECIFICATION_NO_SUBCLASS(Category.X,
      "invalid schema name list specification", "0E", null, null),
  /** 2H000: invalid collation name */
  INVALID_COLLATION_NAME_NO_SUBCLASS(Category.X, "invalid collation name", "2H", null, null),
  /** 33000: invalid SQL descriptor name */
  INVALID_SQL_DESCRIPTOR_NAME_NO_SUBCLASS(Category.X, "invalid SQL descriptor name", "33", null,
      null),
  /** 0M000: invalid SQL-invoked procedure reference */
  INVALID_SQL_INVOKED_PROCEDURE_REFERENCE_NO_SUBCLASS(Category.X,
      "invalid SQL-invoked procedure reference", "0M", null, null),
  /** 26000: invalid SQL statement name */
  INVALID_SQL_STATEMENT_NAME_NO_SUBCLASS(Category.X, "invalid SQL statement name", "26", null,
      null),
  /** 30000: invalid SQL statement identifier */
  INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS(Category.X, "invalid SQL statement identifier", "30",
      null, null),
  /** 0D000: invalid target type specification */
  INVALID_TARGET_TYPE_SPECIFICATION_NO_SUBCLASS(Category.X, "invalid target type specification",
      "0D", null, null),
  /** 25000: invalid transaction state */
  INVALID_TRANSACTION_STATE_NO_SUBCLASS(Category.X, "invalid transaction state", "25", null, null),
  /** 25001: invalid transaction state: active SQL-transaction */
  INVALID_TRANSACTION_STATE_ACTIVE_SQL_TRANSACTION(Category.X, "invalid transaction state", "25",
      "active SQL-transaction", "001"),
  /** 25002: invalid transaction state: branch transaction already active */
  INVALID_TRANSACTION_STATE_BRANCH_TRANSACTION_ALREADY_ACTIVE(Category.X,
      "invalid transaction state", "25", "branch transaction already active", "002"),
  /** 25008: invalid transaction state: held cursor requires same isolation level */
  INVALID_TRANSACTION_STATE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL(Category.X,
      "invalid transaction state", "25", "held cursor requires same isolation level", "008"),
  /** 25003: invalid transaction state: inappropriate access mode for branch transaction */
  INVALID_TRANSACTION_STATE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION(Category.X,
      "invalid transaction state", "25", "inappropriate access mode for branch transaction", "003"),
  /** 25004: invalid transaction state: inappropriate isolation level for branch transaction */
  INVALID_TRANSACTION_STATE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION(Category.X,
      "invalid transaction state", "25", "inappropriate isolation level for branch transaction",
      "004"),
  /** 25005: invalid transaction state: no active SQL-transaction for branch transaction */
  INVALID_TRANSACTION_STATE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION(Category.X,
      "invalid transaction state", "25", "no active SQL-transaction for branch transaction", "005"),
  /** 25006: invalid transaction state: read-only SQL-transaction */
  INVALID_TRANSACTION_STATE_READ_ONLY_SQL_TRANSACTION(Category.X, "invalid transaction state", "25",
      "read-only SQL-transaction", "006"),
  /** 25007: invalid transaction state: schema and data statement mixing not supported */
  INVALID_TRANSACTION_STATE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED(Category.X,
      "invalid transaction state", "25", "schema and data statement mixing not supported", "007"),
  /** 2D000: invalid transaction termination */
  INVALID_TRANSACTION_TERMINATION_NO_SUBCLASS(Category.X, "invalid transaction termination", "2D",
      null, null),
  /** 0S000: invalid transform group name specification */
  INVALID_TRANSFORM_GROUP_NAME_SPECIFICATION_NO_SUBCLASS(Category.X,
      "invalid transform group name specification", "0S", null, null),
  /** 0F000: locator exception */
  LOCATOR_EXCEPTION_NO_SUBCLASS(Category.X, "locator exception", "0F", null, null),
  /** 0F001: locator exception: invalid specification */
  LOCATOR_EXCEPTION_INVALID_SPECIFICATION(Category.X, "locator exception", "0F",
      "invalid specification", "001"),
  /** 02000: no data */
  NO_DATA_NO_SUBCLASS(Category.N, "no data", "02", null, null),
  /** 02001: no data: no additional result sets returned */
  NO_DATA_NO_ADDITIONAL_RESULT_SETS_RETURNED(Category.N, "no data", "02",
      "no additional result sets returned", "001"),
  /** 0W000: prohibited statement encountered during trigger execution */
  PROHIBITED_STATEMENT_DURING_TRIGGER_EXECUTION_NO_SUBCLASS(Category.X,
      "prohibited statement encountered during trigger execution", "0W", null, null),
  /** 0W001: prohibited statement encountered during trigger execution: modify table modified by
   * data change delta table */
  PROHIBITED_STATEMENT_DURING_TRIGGER_EXECUTION_MODIFY_TABLE_MODIFIED_BY_DATA_CHANGE_DELTA_TABLE(
      Category.X, "prohibited statement encountered during trigger execution", "0W",
      "modify table modified by data change delta table", "001"),
  /** HZ: Remote Database Access
   *
   * <p>(See Table 12, 'SQLSTATE class and subclass values for RDA-specific conditions' in
   * [ISO9579], Subclause 8.1, 'Exception codes for RDA-specific Conditions', for the definition of
   * protocol subconditions and subclass code values.) */
  REMOTE_DATABASE_ACCESS_NO_SUBCLASS(Category.X, "Remote Database Access", "HZ", null, null),
  /** 3B000: savepoint exception */
  SAVEPOINT_EXCEPTION_NO_SUBCLASS(Category.X, "savepoint exception", "3B", null, null),
  /** 3B001: savepoint exception: invalid specification */
  SAVEPOINT_EXCEPTION_INVALID_SPECIFICATION(Category.X, "savepoint exception", "3B",
      "invalid specification", "001"),
  /** 3B002: savepoint exception: too many */
  SAVEPOINT_EXCEPTION_TOO_MANY(Category.X, "savepoint exception", "3B", "too many", "002"),
  /** 2F000: SQL routine exception */
  SQL_ROUTINE_EXCEPTION_NO_SUBCLASS(Category.X, "SQL routine exception", "2F", null, null),
  /** 2F005: SQL routine exception: function executed no return statement */
  SQL_ROUTINE_EXCEPTION_FUNCTION_EXECUTED_NO_RETURN_STATEMENT(Category.X, "SQL routine exception",
      "2F", "function executed no return statement", "005"),
  /** 2F002: SQL routine exception: modifying SQL-data not permitted */
  SQL_ROUTINE_EXCEPTION_MODIFYING_SQL_DATA_NOT_PERMITTED(Category.X, "SQL routine exception", "2F",
      "modifying SQL-data not permitted", "002"),
  /** 2F003: SQL routine exception: prohibited SQL-statement attempted */
  SQL_ROUTINE_EXCEPTION_PROHIBITED_SQL_STATEMENT_ATTEMPTED(Category.X, "SQL routine exception",
      "2F", "prohibited SQL-statement attempted", "003"),
  /** 2F004: SQL routine exception: reading SQL-data not permitted */
  SQL_ROUTINE_EXCEPTION_READING_SQL_DATA_NOT_PERMITTED(Category.X, "SQL routine exception", "2F",
      "reading SQL-data not permitted", "004"),
  /** 00000: successful completion */
  SUCCESSFUL_COMPLETION_NO_SUBCLASS(Category.S, "successful completion", "00", null, null),
  /** 42000: syntax error or access rule violation */
  SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_NO_SUBCLASS(Category.X,
      "syntax error or access rule violation", "42", null, null),
  /** 2A000: syntax error or access rule violation */
  SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DIRECT_STATEMENT_NO_SUBCLASS(Category.X,
      "syntax error or access rule violation in direct statement", "2A", null, null),
  /** 37000: syntax error or access rule violation */
  SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DYNAMIC_STATEMENT_NO_SUBCLASS(Category.X,
      "syntax error or access rule violation in dynamic statement", "37", null, null),
  /** 0T000: target table disagrees with cursor specification */
  TARGET_TABLE_DISAGREES_WITH_CURSOR_SPECIFICATION_NO_SUBCLASS(Category.X,
      "target table disagrees with cursor specification", "0T", null, null),
  /** 40000: transaction rollback */
  TRANSACTION_ROLLBACK_NO_SUBCLASS(Category.X, "transaction rollback", "40", null, null),
  /** 40002: transaction rollback: integrity constraint violation */
  TRANSACTION_ROLLBACK_INTEGRITY_CONSTRAINT_VIOLATION(Category.X, "transaction rollback", "40",
      "integrity constraint violation", "002"),
  /** 40001: transaction rollback: serialization failure */
  TRANSACTION_ROLLBACK_SERIALIZATION_FAILURE(Category.X, "transaction rollback", "40",
      "serialization failure", "001"),
  /** 40003: transaction rollback: statement completion unknown */
  TRANSACTION_ROLLBACK_STATEMENT_COMPLETION_UNKNOWN(Category.X, "transaction rollback", "40",
      "statement completion unknown", "003"),
  /** 40004: transaction rollback: triggered action exception */
  TRANSACTION_ROLLBACK_TRIGGERED_ACTION_EXCEPTION(Category.X, "transaction rollback", "40",
      "triggered action exception", "004"),
  /** 09000: triggered action exception */
  TRIGGERED_ACTION_EXCEPTION_NO_SUBCLASS(Category.X, "triggered action exception", "09", null,
      null),
  /** 27000: triggered data change violation */
  TRIGGERED_DATA_CHANGE_VIOLATION_NO_SUBCLASS(Category.X, "triggered data change violation", "27",
      null, null),
  /** 27001: triggered data change violation: modify table modified by data change delta table */
  TRIGGERED_DATA_CHANGE_VIOLATION_MODIFY_TABLE_MODIFIED_BY_DATA_CHANGE_DELTA_TABLE(Category.X,
      "triggered data change violation", "27", "modify table modified by data change delta table",
      "001"),
  /** 01000: warning */
  WARNING_NO_SUBCLASS(Category.W, "warning", "01", null, null),
  /** 0100D: warning: additional result sets returned */
  WARNING_ADDITIONAL_RESULT_SETS_RETURNED(Category.W, "warning", "01",
      "additional result sets returned", "00D"),
  /** 0102F: warning: array data, right truncation */
  WARNING_ARRAY_DATA_RIGHT_TRUNCATION(Category.W, "warning", "01", "array data, right truncation",
      "02F"),
  /** 0100E: warning: attempt to return too many result sets */
  WARNING_ATTEMPT_TO_RETURN_TOO_MANY_RESULT_SETS(Category.W, "warning", "01",
      "attempt to return too many result sets", "00E"),
  /** 01001: warning: cursor operation conflict */
  WARNING_CURSOR_OPERATION_CONFLICT(Category.W, "warning", "01", "cursor operation conflict",
      "001"),
  /** 0100B: warning: default value too long for information schema */
  WARNING_DEFAULT_VALUE_TOO_LONG_FOR_INFORMATION_SCHEMA(Category.W, "warning", "01",
      "default value too long for information schema", "00B"),
  /** 01002: warning: disconnect error */
  WARNING_DISCONNECT_ERROR(Category.W, "warning", "01", "disconnect error", "002"),
  /** 01005: warning: insufficient item descriptor areas */
  WARNING_INSUFFICIENT_ITEM_DESCRIPTOR_AREAS(Category.W, "warning", "01",
      "insufficient item descriptor areas", "005"),
  /** 01012: warning: invalid number of conditions */
  WARNING_INVALID_NUMBER_OF_CONDITIONS(Category.W, "warning", "01", "invalid number of conditions",
      "012"),
  /** 01003: warning: null value eliminated in set function */
  WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION(Category.W, "warning", "01",
      "null value eliminated in set function", "003"),
  /** 01007: warning: privilege not granted */
  WARNING_PRIVILEGE_NOT_GRANTED(Category.W, "warning", "01", "privilege not granted", "007"),
  /** 01006: warning: privilege not revoked */
  WARNING_PRIVILEGE_NOT_REVOKED(Category.W, "warning", "01", "privilege not revoked", "006"),
  /** 0100A: warning: query expression too long for information schema */
  WARNING_QUERY_EXPRESSION_TOO_LONG_FOR_INFORMATION_SCHEMA(Category.W, "warning", "01",
      "query expression too long for information schema", "00A"),
  /** 0100C: warning: result sets returned */
  WARNING_DYNAMIC_RESULT_SETS_RETURNED(Category.W, "warning", "01", "result sets returned", "00C"),
  /** 01009: warning: search condition too long for information schema */
  WARNING_SEARCH_CONDITION_TOO_LONG_FOR_INFORMATION_SCHEMA(Category.W, "warning", "01",
      "search condition too long for information schema", "009"),
  /** 0100F: warning: statement too long for information schema */
  WARNING_STATEMENT_TOO_LONG_FOR_INFORMATION_SCHEMA(Category.W, "warning", "01",
      "statement too long for information schema", "00F"),
  /** 01004: warning: string data, right truncation */
  WARNING_STRING_DATA_RIGHT_TRUNCATION_WARNING(Category.W, "warning", "01",
      "string data, right truncation", "004"),
  /** 44000: with check option violation */
  WITH_CHECK_OPTION_VIOLATION_NO_SUBCLASS(Category.X, "with check option violation", "44", null,
      null);

  public final Category category;
  public final String condition;
  public final String klass;
  public final String subCondition;
  public final String subClass;
  public final String code;

  /** Alias for backwards compatibility with previous versions of SQL spec. */
  public static final SqlState INVALID_SQL_STATEMENT =
      INVALID_SQL_STATEMENT_IDENTIFIER_NO_SUBCLASS;

  public static final Map<String, SqlState> BY_CODE;

  static {
    Map<String, SqlState> m = new HashMap<>();
    for (SqlState s : values()) {
      m.put(s.code, s);
    }
    BY_CODE = Collections.unmodifiableMap(m);
  }

  SqlState(Category category, String condition, String klass, String subCondition,
      String subClass) {
    this.category = category;
    this.condition = condition;
    this.klass = klass;
    this.subCondition = subCondition;
    this.subClass = subClass;
    this.code = klass + (subClass == null ? "000" : subClass);
  }

  /** Validates the data, and generates the HTML table. */
  private static void main(String[] args) {
    PrintWriter pw = new PrintWriter(
        new BufferedWriter(
            new OutputStreamWriter(System.out, StandardCharsets.UTF_8)));
    pw.println(" * <table>");
    SqlState parent = null;
    for (SqlState s : values()) {
      assert s.klass.length() == 2;
      assert s.subClass == null || s.subClass.length() == 3;
      if (s.subClass == null) {
        assert s.subCondition == null;
        parent = s;
      } else {
        assert parent != null;
        assert s.subCondition != null;
        assert s.category == parent.category;
        assert s.klass.equals(parent.klass);
        assert s.condition.equals(parent.condition);
      }
      pw.println(" * <tr>");
      pw.println(" *   <td>" + (parent == s ? s.category : "&nbsp;") + "</td>");
      pw.println(" *   <td>" + (parent == s ? s.condition : "&nbsp;") + "</td>");
      pw.println(" *   <td>" + (parent == s ? s.klass : "&nbsp;") + "</td>");
      pw.println(" *   <td>" + (s.subCondition == null ? "(no subclass)" : s.subCondition)
          + "</td>");
      pw.println(" *   <td>" + (s.subCondition == null ? "000" : s.subClass) + "</td>");
      pw.println(" * </tr>");
    }
    pw.println(" * </table>");
    pw.close();
  }

  /** Severity types. */
  enum Category {
    /** Success (class 00). */
    S,
    /** Warning (class 01). */
    W,
    /** No data (class 02). */
    N,
    /** Exception (all other classes). */
    X,
  }

}

// End SqlState.java
