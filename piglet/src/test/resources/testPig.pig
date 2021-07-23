-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to you under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

---------------------------------------------------------------------
-- Top N Queries.
--   Counting how many times a query happened
---------------------------------------------------------------------
%DECLARE n 5
data =
    LOAD '$input'
    USING PigStorage(' ')
    AS (query:CHARARRAY, count:INT);

queries_group =
    GROUP data
    BY query;

queries_sum =
     FOREACH queries_group
     GENERATE
         group AS query,
         SUM(data.count) AS count;

queries_ordered =
    ORDER queries_sum
    BY count DESC;

queries_limit = LIMIT queries_ordered $n;

STORE queries_limit INTO '$output';

-- End testPig.pig
