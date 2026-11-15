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

# Checks the '!ok' and '!error' queries of an IQ file (such as sql/pipe.iq)
# against GoogleSQL's execute_query binary, from a release of
# https://github.com/google/googlesql. SETUP_SQL runs before each query. As in
# Quidem, rows are compared in order only if the query's last ORDER BY is not
# in parentheses.
#
# Usage: pipe_iq_googlesql.py <EXECUTE_QUERY> <SETUP_SQL> <IQ_FILENAME>
#
# ex: pipe_iq_googlesql.py ./execute_query_linux pipe_iq_googlesql_setup.sql \
#       sql/pipe.iq

import decimal
import re
import subprocess
import sys


def table(text, sep, ordered):
  """Parses a table, upper-casing its header, and normalizing NULLs and
  numbers so that GoogleSQL's 2450 matches Quidem's 2450.00."""
  def value(v):
    try:
      return str(decimal.Decimal(v).normalize())
    except decimal.InvalidOperation:
      return '' if v == 'NULL' else v
  # Each row (and the header) is a line that starts with the separator, such
  # as '| a | b |'; other lines, such as '+---+---+', are borders.
  rows = [[value(v.strip()) for v in line.strip()[1:-1].split(sep)]
          for line in text.split('\n') if line.startswith(sep)]
  return [[h.upper() for h in row] for row in rows[:1]] \
      + (rows[1:] if ordered else sorted(rows[1:]))


binary, setup, iq = sys.argv[1:]
setup, iq = open(setup).read(), open(iq).read()
failed = False
# Each match is a query (a line that is not blank, a comment or a command,
# through a line ending in ';'), its expected output, and the command after it.
for m in re.finditer(r'^(?![#!\s])(.*?);\n(.*?)^(!\S+)', iq, re.M | re.S):
  sql, expected, command = m.groups()
  if command not in ('!ok', '!error'):
    continue
  # Run the query on GoogleSQL, after the statements that create the tables.
  out = subprocess.run([binary, '--enabled_language_features=MAXIMUM',
                        '--mode=execute', '-'], input=f'{setup}\n{sql};',
                       capture_output=True, text=True)
  out = out.stdout + out.stderr
  # Compare in order only if Quidem would; a port of
  # Quidem.isProbablyDeterministic.
  _, order_by, tail = sql.upper().rpartition('ORDER BY')
  ordered = order_by and tail.count(')') <= tail.count('(')
  # GoogleSQL prints tables with box-drawing characters; Quidem uses '|'.
  ok = 'ERROR:' in out if command == '!error' \
      else table(out, '│', ordered) == table(expected, '|', ordered)
  print('PASS' if ok else 'FAIL', 'line', iq.count('\n', 0, m.start()) + 1,
        sql.split('\n')[0])
  if not ok:
    print(expected + out)
    failed = True
sys.exit(failed)
