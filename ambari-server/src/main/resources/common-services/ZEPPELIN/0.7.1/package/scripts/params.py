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


# params from zeppelin-env
zeppelin_user= config['configurations']['zeppelin-env']['zeppelin_user']
zeppelin_pid_dir = config['configurations']['zeppelin-env']['zeppelin_pid_dir']
user_group = config['configurations']['cluster-env']['user_group']
zeppelin_log_dir = config['configurations']['zeppelin-env']['zeppelin_log_dir']
zeppelin_hdfs_user_dir = format("/user/{zeppelin_user}")
zeppelin_restore_interpreters = config['configurations']['zeppelin-env']['zeppelin_restore_interpreter_defaults']

ui_ssl_enabled = config['configurations']['zeppelin-site']['zeppelin.ssl']
zeppelin_port = str(config['configurations']['zeppelin-site']['zeppelin.server.port'])

zeppelin_dir = '/usr/lib/zeppelin'
conf_dir = zeppelin_dir + '/conf'

hadoop_conf_dir = conf_select.get_hadoop_conf_dir()

flink_hosts = default('/clusterHostInfo/flink_jobmanager_hosts', None)
alluxio_master_hosts = default('/clusterHostInfo/alluxio_master_hosts', None)
namenode_host = default("/clusterHostInfo/namenode_host", None)
zeppelin_host = str(config['clusterHostInfo']['zeppelin_master_hosts'][0])

#if 'postgres_hosts' not in config['clusterHostInfo']:
#  postgres_url = "jdbc:postgresql://localhost:5432/"

# Set configuration properties for flink in high availability mode
hdfs_default_name = config['configurations']['core-site']['fs.defaultFS']

if flink_hosts is not None:
  zookeeper_port = str(config['configurations']['zoo.cfg']['clientPort'])
  flink_version = config['configurations']['flink-env']['flink_version']
  flink_jobmanager_port = config['configurations']['flink-conf']['jobmanager.rpc.port']

  if flink_version == '1.1.2':
    recovery_mode = config['configurations']['flink-conf']['recovery.mode']
    recovery_zookeeper_path_root = '/flink/recovery'
  else:
    recovery_mode = config['configurations']['flink-conf']['high-availability']
    recovery_zookeeper_path_root = '/flink/high-availability'

  zookeeper_quorum = ''
  recovery_zookeeper_storage_dir = ''
  if 'zookeeper_hosts' in config['clusterHostInfo']:
    zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
    if len(zookeeper_hosts_list) > 0:
      zookeeper_quorum = (':' + zookeeper_port + ',').join(zookeeper_hosts_list) + ':' + zookeeper_port

    recovery_zookeeper_storage_dir = format('{hdfs_default_name}{recovery_zookeeper_path_root}')

  has_flink_jobmanager = True
  flink_conf_dir = '/etc/flink/conf'
  flink_user = config['configurations']['flink-env']['flink_user']
  flink_host = config['clusterHostInfo']['flink_jobmanager_hosts'][0]
else:
  has_flink_jobmanager = False
  flink_host = 'local'
  flink_jobmanager_port = '6123'
  flink_version = '1.3.0'

if alluxio_master_hosts is not None:
  alluxio_master_host = alluxio_master_hosts[0]
else:
  alluxio_master_host = 'localhost'

if namenode_host is not None:
  namenode_address = config['configurations']['hdfs-site']['dfs.namenode.http-address']
  hdfs_url = 'http://' + namenode_address + '/webhdfs/v1/'
  hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
else:
  hdfs_url = "http://localhost:50070/webhdfs/v1/"
  hdfs_user = 'hdfs'

#zeppelin-env.sh
# zeppelin_env_content = config['configurations']['zeppelin-env']['content']


#detect configs
java64_home = config['hostLevelParams']['java_home']
