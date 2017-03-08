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
!connect jdbc:calcite:model=target/test-classes/wiki.json admin admin

values 'What are the largest cities in California?';
select c."Rank", c."City", c."State", c."Population" "City Population", s."Population" "State Population", (100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" and s."State" = 'California';

values 'What percentage of California residents live in big cities?';
select count(*) "City Count", sum(100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" and s."State" = 'California';

values 'What cities comprise the largest percentage of state population?';
select c."Rank", c."City", c."State", c."Population" "City Population", s."Population" "State Population", (100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" order by "Pct State Population" desc limit 10;
