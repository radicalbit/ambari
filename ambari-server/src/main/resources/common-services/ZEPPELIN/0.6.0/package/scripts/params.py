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
from resource_management.libraries.functions import conf_select
import sys, os, re

# server configurations
config = Script.get_config()

# params from zeppelin-config
#zeppelin_port = str(config['configurations']['zeppelin-env']['zeppelin_port'])

# params from zeppelin-env
zeppelin_user= config['configurations']['zeppelin-env']['zeppelin_user']
user_group = config['configurations']['cluster-env']['user_group']
zeppelin_log_dir = config['configurations']['zeppelin-env']['zeppelin_log_dir']
zeppelin_pid_dir = config['configurations']['zeppelin-env']['zeppelin_pid_dir']
zeppelin_hdfs_user_dir = format("/user/{zeppelin_user}")

zeppelin_dir = '/usr/lib/zeppelin'
conf_dir = zeppelin_dir + '/conf'

hadoop_conf_dir = conf_select.get_hadoop_conf_dir()

flink_hosts = default('/clusterHostInfo/flink_master_hosts', None)
alluxio_master_hosts = default('/clusterHostInfo/alluxio_master_hosts', None)
cassandra_seeds = default('/clusterHostInfo/cassandra_seed_hosts', None)
cassandra_nodes = default('/clusterHostInfo/cassandra_node_hosts', None)

if flink_hosts is not None:
  has_flink_master = True
  flink_conf_dir = '/etc/flink/conf'
  flink_user = config['configurations']['flink-env']['flink_user']
else:
  has_flink_master = False

if alluxio_master_hosts is not None:
  alluxio_master_host = alluxio_master_hosts[0]
else:
  alluxio_master_host = 'localhost'

if cassandra_seeds is not None:
  if cassandra_nodes is not None:
    cassandra_all_hosts = cassandra_seeds + cassandra_nodes
    cassandra_hosts = ','.join(cassandra_all_hosts)
  else:
    cassandra_hosts = ','.join(cassandra_seeds)
else:
  cassandra_hosts = 'localhost'

#zeppelin-env.sh
# zeppelin_env_content = config['configurations']['zeppelin-env']['content']


#detect configs
java64_home = config['hostLevelParams']['java_home']
