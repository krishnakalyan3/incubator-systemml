#!/usr/bin/env python3
# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
import pandas as pd
import argparse

def get_dataframe(path):
    current_perf = pd.read_csv(path, sep=',', skiprows=1, skipfooter=1, engine='python')
    return current_perf


if __name__ == '__main__':
    cparser = argparse.ArgumentParser(description='System-ML Statistics Script')
    cparser.add_argument('--current', help='Location of the current perf test outputs',
                         required=True, metavar='')
    cparser.add_argument('--old', help='Location of the old perf test outputs',
                         required=True, metavar='')
    cparser.add_argument('--write', help='Location to write statistics',
                         required=True, metavar='')

    args = cparser.parse_args()
    arg_dict = vars(args)
    print(arg_dict)
    current_perf = get_dataframe(arg_dict.current)
    old_perf = get_dataframe(arg_dict.old)

    delta_time = current_perf['time_sec'] - old_perf['time_sec']
    change = (delta_time / old_perf['time_sec']) * 100

    stats_df = {'algo': current_perf.ix[:,0], 'delta': delta_time, 'change': change}
    df = pd.DataFrame(stats_df)

    print(df)
    df.to_csv(write_path, index=False)
