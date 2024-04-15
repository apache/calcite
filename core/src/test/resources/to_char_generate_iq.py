#!/usr/bin/env python3
#
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

# Generates an IQ file using an input file with queries. The expected
# results are obtained by running the queries against a PostgreSQL
# server.
#
# Usage: to_char_generate_iq.py <PSQL_COMMAND> [PSQL_ARGS] <QUERIES_FILE> <IQ_FILENAME>
#
# ex: to_char_generate_iq.py psql postgres pg_to_char_queries.sql sql/pg_to_char.iq

import subprocess
import sys

if len(sys.argv) < 4:
  print(f'Usage: {sys.argv[0]} <PSQL_COMMAND> [PSQL_ARGS] <QUERIES_FILE> <IQ_FILENAME>', file=sys.stderr)
  exit(1)

pg_args = sys.argv[1:-2]
pg_args.insert(1, '-q')

queries_filename = sys.argv[-2]
iq_filename = sys.argv[-1]

with subprocess.Popen(pg_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE) as pg_process:
  iq_file = open(iq_filename, 'w')
  print("""# pg_to_char.iq - expressions using the to_char function for PostgreSQL
#
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
!use post-postgresql
!set outputformat psql

""", file=iq_file)

  with open(queries_filename, 'r') as queries_file:
    query_lines = queries_file.readlines()
    offset = 0
    while offset < len(query_lines):
      if len(query_lines[offset]) > 0 and query_lines[offset][0] != '#':
        break
      offset += 1
    query_lines = query_lines[offset:]

    output = pg_process.communicate(input=str.encode(''.join(query_lines)))[0]
    output = output.decode('utf-8')

    results = output.split('\n\n')
    for i in range(len(query_lines)):
      print(query_lines[i].rstrip(), file=iq_file)

      result_lines = results[i].split('\n')
      print(' EXPR$0', file=iq_file)
      print('-' * max(8, (len(result_lines[2].rstrip()) + 1)), file=iq_file)
      print(result_lines[2].rstrip(), file=iq_file)
      for i in range(len(result_lines) - 3):
        print(result_lines[i + 3], file=iq_file)

      print('\n!ok\n', file=iq_file)

  iq_file.close()
