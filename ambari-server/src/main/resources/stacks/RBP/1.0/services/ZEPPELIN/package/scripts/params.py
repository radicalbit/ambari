#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
from resource_management import *
import sys, os, re

# server configurations
config = Script.get_config()

# params from zeppelin-config
zeppelin_port = str(config['configurations']['zeppelin-env']['zeppelin_port'])

# params from zeppelin-env
zeppelin_user= config['configurations']['zeppelin-env']['zeppelin_user']
user_group = config['configurations']['cluster-env']['user_group']
zeppelin_log_dir = config['configurations']['zeppelin-env']['zeppelin_log_dir']
zeppelin_pid_dir = config['configurations']['zeppelin-env']['zeppelin_pid_dir']
zeppelin_hdfs_user_dir = format("/user/{zeppelin_user}")

zeppelin_dir = '/usr/lib/zeppelin'
conf_dir = zeppelin_dir + '/conf'

#zeppelin-env.sh
# zeppelin_env_content = config['configurations']['zeppelin-env']['content']


#detect configs
java64_home = config['hostLevelParams']['java_home']
