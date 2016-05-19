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
import multiprocessing

# server configurations
config = Script.get_config()

# usefull dirs
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hdfs_default_name = config['configurations']['core-site']['fs.defaultFS']

nodes_number = len(config['clusterHostInfo']['all_hosts'])
cores_number = config['configurations']['yarn-site']['yarn.scheduler.maximum-allocation-vcores']
#cores_number = multiprocessing.cpu_count()

security_enabled = config['configurations']['cluster-env']['security_enabled']

hostname = config['hostname']
flink_masters = config['clusterHostInfo']['flink_master_hosts']
flink_slaves = config['clusterHostInfo']['flink_slave_hosts']
flink_master = flink_masters[0]
custer_hosts = config['clusterHostInfo']['all_hosts']

if security_enabled:
  kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
  #kdestroy_path_local = kinit_path_local.replace('kinit', 'kdestroy')
  _flink_principal_name = config['configurations']['flink-env']['flink_principal_name']
  flink_jaas_principal = _flink_principal_name.replace('_HOST',hostname.lower())
  flink_client_jass_path = "/etc/flink/conf.dist/flink_client_jaas.conf"
  flink_keytab = "/etc/security/keytabs/flink.service.keytab"
  krb5_conf_path = "/etc/krb5.conf"
  flink_krb_ticket_renew_window = config['configurations']['flink-env']['flink_krb_ticket_renew_window']

alluxio_master = ''
alluxio_default_name = 'file:///'
if 'alluxio_master_hosts' in config['clusterHostInfo']:
  alluxio_master = config['clusterHostInfo']['alluxio_master_hosts'][0]
  alluxio_default_name = 'alluxio://' + alluxio_master + ':19998/'

flink_install_dir = '/usr/lib/flink'
conf_dir = flink_install_dir + '/conf'
bin_dir = flink_install_dir + '/bin'
flink_lib = flink_install_dir + '/lib'

# params from flink-config

jobmanager_rpc_address = flink_master
jobmanager_rpc_port = config['configurations']['flink-config']['jobmanager.rpc.port']
jobmanager_heap_mb = config['configurations']['flink-config']['jobmanager.heap.mb']
taskmanager_heap_mb = config['configurations']['flink-config']['taskmanager.heap.mb']
taskmanager_numberOfTaskSlots = config['configurations']['flink-config']['taskmanager.numberOfTaskSlots']
parallelism_default = config['configurations']['flink-config']['parallelism.default']
taskmanager_memory_preallocate = config['configurations']['flink-config']['taskmanager.memory.preallocate']

fs_hdfs_hadoopconf = hadoop_conf_dir

# advanced configurations
taskmanager_memory_size = config['configurations']['flink-advanced']['taskmanager.memory.size']
taskmanager_memory_fraction = config['configurations']['flink-advanced']['taskmanager.memory.fraction']
taskmanager_memory_segment_size = config['configurations']['flink-advanced']['taskmanager.memory.segment-size']
taskmanager_memory_preallocate = config['configurations']['flink-advanced']['taskmanager.memory.preallocate']
taskmanager_tmp_dirs = config['configurations']['flink-advanced']['taskmanager.tmp.dirs']
taskmanager_network_numberOfBuffers = config['configurations']['flink-advanced']['taskmanager.network.numberOfBuffers']
state_backend = config['configurations']['flink-advanced']['state.backend']
state_backend_checkpointdir = '/flink/checkpoint'
state_backend_fs_checkpointdir = format('{hdfs_default_name}{state_backend_checkpointdir}')
blob_storage_directory = config['configurations']['flink-advanced']['blob.storage.directory']
blob_server_port = config['configurations']['flink-advanced']['blob.server.port']
fs_output_always_create_directory = config['configurations']['flink-advanced']['fs.output.always-create-directory']

# web
jobmanager_web_port = config['configurations']['flink-config']['jobmanager.web.port']
jobmanager_web_history = config['configurations']['flink-config']['jobmanager.web.history']
jobmanager_web_checkpoints_disable = config['configurations']['flink-config']['jobmanager.web.checkpoints.disable']
jobmanager_web_checkpoints_history = config['configurations']['flink-config']['jobmanager.web.checkpoints.history']

# params from flink-env.yaml
flink_user = config['configurations']['flink-env']['flink_user']
user_group = config['configurations']['cluster-env']['user_group']
flink_pid_dir = config['configurations']['flink-config']['env.pid.dir']
flink_log_dir = config['configurations']['flink-config']['env.log.dir']
flink_cluster_log_file = os.path.join(flink_log_dir,'flink-cluster.log')

recovery_mode = config['configurations']['flink-config']['recovery.mode']

zookeeper_quorum = ''
zookeeper_port = str(config['configurations']['zoo.cfg']['clientPort'])
if 'zookeeper_hosts' in config['clusterHostInfo']:
  zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
  if len(zookeeper_hosts_list) > 0:
    zookeeper_quorum = (':' + zookeeper_port + ',').join(zookeeper_hosts_list) + ':' + zookeeper_port

recovery_zookeeper_path_root = '/flink/recovery'
recovery_zookeeper_storage_dir = format('{hdfs_default_name}{recovery_zookeeper_path_root}')