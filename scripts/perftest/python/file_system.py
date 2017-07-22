#!/usr/bin/env python3
#-------------------------------------------------------------
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
#-------------------------------------------------------------
import os
from os.path import join
import os
import json
import subprocess
import shlex
import re
import logging
import sys
import glob
from functools import reduce


def create_dir_local(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def write_success(time, config_path, action_mode, fs):

    if action_mode == 'data-gen':
        if fs.startswith('hdfs') and len(time.split('.')) == 2:
            full_path = join(fs, action_mode, config_path.split('/')[-1], '_SUCCESS')
            cmd = ['hdfs', 'dfs', '-touchz', full_path]
            os.system(' '.join(cmd))
        else:
            # Write a _SUCCESS file only if time is found and in data-gen action_mode
            if len(time.split('.')) == 2:
                full_path = join(config_path, '_SUCCESS')
                open(full_path, 'w').close()


def get_existence(path, action_mode, fs):
    """
    Check SUCCESS file is present in the input path

    path: String
    Input folder path

    action_mode : String
    Type of action data-gen, train ...

    return: Boolean check if the file _SUCCESS exists
    """

    if action_mode == 'data-gen':

        if fs.startswith('hdfs'):
            full_path = join(fs, action_mode, path.split('/')[-1], '_SUCCESS')
            cmd = ['hdfs', 'dfs', '-test', '-e', full_path]
            return_code = os.system(' '.join(cmd))
            if return_code == 0:
                return True
        else:
            full_path = join(path, '_SUCCESS')
            exist = os.path.isfile(full_path)
            return exist
