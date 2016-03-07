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
#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.functions import conf_select
from resource_management.libraries.script.script import Script
import sys, os, glob

# server configurations
config = Script.get_config()

# usefull dirs
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
flink_install_dir = '/usr/lib/flink'
conf_dir =  flink_install_dir + '/conf'
bin_dir =  flink_install_dir + '/bin'

# params from flink-config
flink_numcontainers = config['configurations']['flink-config']['flink_numcontainers']
flink_jobmanager_memory = config['configurations']['flink-config']['flink_jobmanager_memory']
flink_container_memory = config['configurations']['flink-config']['flink_container_memory']
flink_appname = config['configurations']['flink-config']['flink_appname']
flink_queue = config['configurations']['flink-config']['flink_queue']
flink_streaming = config['configurations']['flink-config']['flink_streaming']

# params from flink-env.yaml
flink_user = config['configurations']['flink-env']['flink_user']
flink_group = config['configurations']['flink-env']['flink_group']
flink_pid_dir = config['configurations']['flink-env']['flink_pid_dir']
flink_log_dir = config['configurations']['flink-env']['flink_log_dir']
flink_log_file = os.path.join(flink_log_dir,'flink-setup.log')
#flink_yaml_content = config['configurations']['flink-env']['content']

#
zookeeper_quorum = ''
zookeeper_port = config['configurations']['zoo.cfg']['clientPort']
if 'zookeeper_hosts' in config['clusterHostInfo']:
  zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
  if len(zookeeper_hosts_list) > 0:
    zookeeper_quorum = ':' + zookeeper_port + ','.join(zookeeper_hosts_list) + ':' + zookeeper_port
