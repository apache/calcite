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
select to_char(timestamp '2022-06-03 12:15:48.678', 'YYYY-MM-DD HH24:MI:SS.MS');
select to_char(timestamp '2022-06-03 12:15:48.678', 'HH');
select to_char(timestamp '2022-06-03 13:15:48.678', 'HH12');
select to_char(timestamp '2022-06-03 13:15:48.678', 'HH24');
select to_char(timestamp '2022-06-03 13:15:48.678', 'MI');
select to_char(timestamp '2022-06-03 13:15:48.678', 'SS');
select to_char(timestamp '2022-06-03 13:15:48.678', 'MS');
select to_char(timestamp '2022-06-03 12:15:48.678', 'US');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF1');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF2');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF3');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF4');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF5');
select to_char(timestamp '2022-06-03 12:15:48.678', 'FF6');
select to_char(timestamp '2022-06-03 12:15:48.678', 'SSSS');
select to_char(timestamp '2022-06-03 12:15:48.678', 'SSSSS');
select to_char(timestamp '2022-06-03 12:15:48.678', 'AM');
select to_char(timestamp '2022-06-03 12:15:48.678', 'am');
select to_char(timestamp '2022-06-03 02:15:48.678', 'PM');
select to_char(timestamp '2022-06-03 02:15:48.678', 'pm');
select to_char(timestamp '2022-06-03 12:15:48.678', 'A.M.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'a.m.');
select to_char(timestamp '2022-06-03 02:15:48.678', 'P.M.');
select to_char(timestamp '2022-06-03 02:15:48.678', 'p.m.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Y,YYY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'YYYY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'YYY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'YY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Y');
select to_char(timestamp '2023-01-01 12:15:48.678', 'IYYY');
select to_char(timestamp '2023-01-01 12:15:48.678', 'IYY');
select to_char(timestamp '2023-01-01 12:15:48.678', 'IY');
select to_char(timestamp '2023-01-01 12:15:48.678', 'I');
select to_char(timestamp '2022-06-03 12:15:48.678', 'BC');
select to_char(timestamp '2022-06-03 12:15:48.678', 'bc');
select to_char(timestamp '2022-06-03 12:15:48.678', 'AD');
select to_char(timestamp '2022-06-03 12:15:48.678', 'ad');
select to_char(timestamp '2022-06-03 12:15:48.678', 'B.C.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'b.c.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'A.D.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'a.d.');
select to_char(timestamp '2022-06-03 12:15:48.678', 'MONTH');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Month');
select to_char(timestamp '2022-06-03 12:15:48.678', 'month');
select to_char(timestamp '2022-06-03 12:15:48.678', 'MON');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Mon');
select to_char(timestamp '2022-06-03 12:15:48.678', 'mon');
select to_char(timestamp '2022-06-03 12:15:48.678', 'DAY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Day');
select to_char(timestamp '2022-06-03 12:15:48.678', 'day');
select to_char(timestamp '2022-06-03 12:15:48.678', 'DY');
select to_char(timestamp '0001-01-01 00:00:00.000', 'DY');
select to_char(timestamp '2022-06-03 12:15:48.678', 'Dy');
select to_char(timestamp '2022-06-03 12:15:48.678', 'dy');
select to_char(timestamp '2022-06-03 12:15:48.678', 'DDD');
select to_char(timestamp '2022-06-03 12:15:48.678', 'IDDD');
select to_char(timestamp '2022-06-03 12:15:48.678', 'DD');
select to_char(timestamp '2022-06-03 12:15:48.678', 'D');
select to_char(timestamp '2022-06-03 12:15:48.678', 'ID');
select to_char(timestamp '2022-06-03 12:15:48.678', 'W');
select to_char(timestamp '2022-06-03 12:15:48.678', 'WW');
select to_char(timestamp '2022-06-03 13:15:48.678', 'IW');
select to_char(timestamp '2022-06-03 12:15:48.678', 'CC');
select to_char(timestamp '2022-06-03 12:15:48.678', 'J');
select to_char(timestamp '2022-06-03 13:15:48.678', 'Q');
select to_char(timestamp '2022-06-03 13:15:48.678', 'RM');
select to_char(timestamp '2022-06-03 13:15:48.678', 'rm');
