# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Creates EMP and DEPT in GoogleSQL, with the same rows as the scott schema,
# for pipe_iq_googlesql.py.
CREATE TEMP TABLE emp AS SELECT * FROM UNNEST([
  STRUCT(7369 AS empno, 'SMITH' AS ename, 'CLERK' AS job,
    CAST(7902 AS INT64) AS mgr, DATE '1980-12-17' AS hiredate,
    CAST(800.00 AS NUMERIC) AS sal, CAST(NULL AS NUMERIC) AS comm,
    20 AS deptno),
  (7499, 'ALLEN', 'SALESMAN', 7698, DATE '1981-02-20', 1600.00, 300.00, 30),
  (7521, 'WARD', 'SALESMAN', 7698, DATE '1981-02-22', 1250.00, 500.00, 30),
  (7566, 'JONES', 'MANAGER', 7839, DATE '1981-02-04', 2975.00, NULL, 20),
  (7654, 'MARTIN', 'SALESMAN', 7698, DATE '1981-09-28', 1250.00, 1400.00, 30),
  (7698, 'BLAKE', 'MANAGER', 7839, DATE '1981-01-05', 2850.00, NULL, 30),
  (7782, 'CLARK', 'MANAGER', 7839, DATE '1981-06-09', 2450.00, NULL, 10),
  (7788, 'SCOTT', 'ANALYST', 7566, DATE '1987-04-19', 3000.00, NULL, 20),
  (7839, 'KING', 'PRESIDENT', NULL, DATE '1981-11-17', 5000.00, NULL, 10),
  (7844, 'TURNER', 'SALESMAN', 7698, DATE '1981-09-08', 1500.00, 0.00, 30),
  (7876, 'ADAMS', 'CLERK', 7788, DATE '1987-05-23', 1100.00, NULL, 20),
  (7900, 'JAMES', 'CLERK', 7698, DATE '1981-12-03', 950.00, NULL, 30),
  (7902, 'FORD', 'ANALYST', 7566, DATE '1981-12-03', 3000.00, NULL, 20),
  (7934, 'MILLER', 'CLERK', 7782, DATE '1982-01-23', 1300.00, NULL, 10)
]);
CREATE TEMP TABLE dept AS SELECT * FROM UNNEST([
  STRUCT(10 AS deptno, 'ACCOUNTING' AS dname, 'NEW YORK' AS loc),
  (20, 'RESEARCH', 'DALLAS'),
  (30, 'SALES', 'CHICAGO'),
  (40, 'OPERATIONS', 'BOSTON')
]);
