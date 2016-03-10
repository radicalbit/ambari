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
hdfs_default_name = config['configurations']['core-site']['fs.defaultFS']
flink_install_dir = '/usr/lib/flink'
conf_dir =  flink_install_dir + '/conf'
bin_dir =  flink_install_dir + '/bin'

# params from flink-config

jobmanager_rpc_address = config['configurations']['flink-config']['jobmanager.rpc.address']
jobmanager_rpc_port = config['configurations']['flink-config']['jobmanager.rpc.port']
jobmanager_heap_mb = config['configurations']['flink-config']['jobmanager.heap.mb']
taskmanager_heap_mb = config['configurations']['flink-config']['taskmanager.heap.mb']
taskmanager_numberOfTaskSlots = config['configurations']['flink-config']['taskmanager.numberOfTaskSlots']
parallelism_default = config['configurations']['flink-config']['parallelism.default']
fs_default_scheme = hdfs_default_name
fs_hdfs_hadoopconf = hadoop_conf_dir


taskmanage_memory_size = config['configurations']['flink-config']['taskmanager.memory.size']
taskmanager_memory_fraction = config['configurations']['flink-config']['taskmanager.memory.fraction']
taskmanager_memory_segment_size = config['configurations']['flink-config']['taskmanager.memory.segment-size']
taskmanager_memory_preallocate = config['configurations']['flink-config']['taskmanager.memory.preallocate']
taskmanager_tmp_dirs = config['configurations']['flink-config']['taskmanager.tmp.dirs']
jobmanager_web_port = config['configurations']['flink-config']['jobmanager.web.port']
fs_output_always_create_directory = config['configurations']['flink-config']['fs.output.always-create-directory']
taskmanager_network_numberOfBuffers = config['configurations']['flink-config']['taskmanager.network.numberOfBuffers']
state_backend = config['configurations']['flink-config']['state.backend']
blob_storage_directory = config['configurations']['flink-config']['blob.storage.directory']
blob_server_port = config['configurations']['flink-config']['blob.server.port']
state_backend_fs_checkpointdir = format('{fs_default_scheme}/flink/checkpoint')


jobmanager_web_port = config['configurations']['flink-config']['jobmanager.web.port']
jobmanager_web_history = config['configurations']['flink-config']['jobmanager.web.history']
jobmanager_web_checkpoints_disable = config['configurations']['flink-config']['jobmanager.web.checkpoints.disable']
jobmanager_web_checkpoints_history = config['configurations']['flink-config']['jobmanager.web.checkpoints.history']


yarn_application_master_port = config['configurations']['flink-config']['yarn.application-master.port']


recovery_mode = config['configurations']['flink-config']['recovery.mode']
recovery_zookeeper_path_root = format('{fs_default_scheme}/flink/recovery')

# log4j configs
log4j_props = config['configurations']['flink-log4j']['content']

# params from flink-env.yaml
flink_user = config['configurations']['flink-env']['flink_user']
user_group = config['configurations']['cluster-env']['user_group']
#flink_group = config['configurations']['flink-env']['flink_group']
#flink_pid_dir = config['configurations']['flink-env']['flink_pid_dir']
flink_log_dir = config['configurations']['flink-env']['flink_log_dir']
flink_log_file = os.path.join(flink_log_dir,'flink.log')

#
zookeeper_quorum = ''
zookeeper_port = str(config['configurations']['zoo.cfg']['clientPort'])
if 'zookeeper_hosts' in config['clusterHostInfo']:
  zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
  if len(zookeeper_hosts_list) > 0:
    zookeeper_quorum = ':' + zookeeper_port + ','.join(zookeeper_hosts_list) + ':' + zookeeper_port
