# arrow_data.py - Generate .arrow data files for testing
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

import pyarrow as pa
import pandas as pd
import random
import string
import decimal

def generate_large_arrow_file():
  """
  Generates an arrow file with large data.
  Change the range size for all the fields to generate more data.
  """
  # generate a list containing 100 integers
  fieldOne = [i for i in range(100)]

  # generate a list containing 100 strings of 3 charachters each
  fieldTwo = []
  for i in range(100):
    letters = string.ascii_lowercase
    fieldTwo.append(''.join(random.choice(letters) for i in range(3)))

  # generate a list containing 100 float values
  fieldThree = [float(decimal.Decimal(random.randrange(10, 300))/100) for i in range(100)]

  # create a dataframe with three columns
  df = pd.DataFrame({'fieldOne': fieldOne,
        'fieldTwo': fieldTwo,
        'fieldThree': fieldThree})

  # create arrow table from dataframe
  table = pa.Table.from_pandas(df)

  # write arrow date to arrow file
  with pa.OSFile('test.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, table.schema) as writer:
        writer.write_table(table)

def generate_small_arrow_file():
  """
  Generates a small arrow file with two batches.
  """
  # create two dataframes
  df = pd.DataFrame({'fieldOne': [1, 2, 3, 4, 5, 6],
        'fieldTwo': ["one", "two", "three", "four", "five", "six"],
        'fieldThree': [1.2, 3.4, 5.6, 1.22, 3.45, 5.67]})
  df2 = pd.DataFrame({'fieldOne': [7, 8, 9],
        'fieldTwo': ["seven", "eight", "nine"],
        'fieldThree': [1.22, 3.45, 5.67]})

  # create two arrow batches from the dataframes
  batch = pa.RecordBatch.from_pandas(df)
  batch2 = pa.RecordBatch.from_pandas(df2)

  # write arrow batches to arrow file
  with pa.OSFile('test2.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, batch.schema) as stream_writer:
        stream_writer.write_batch(batch)
        stream_writer.write_batch(batch2)

if __name__ == '__main__':
  # generate_large_arrow_file()
  # generate_small_arrow_file()

