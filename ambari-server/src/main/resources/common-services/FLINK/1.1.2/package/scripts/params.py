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
hadoop_conf_dir = conf_select.get_hadoop_conf_dir() + '/'
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_default_name = config['configurations']['core-site']['fs.defaultFS']
flink_install_dir = '/usr/lib/flink'
conf_dir = flink_install_dir + '/conf'
bin_dir = flink_install_dir + '/bin'
flink_lib = flink_install_dir + '/lib'

# params from flink-env.xml
flink_user = config['configurations']['flink-env']['flink_user']
user_group = config['configurations']['cluster-env']['user_group']
flink_pid_dir = config['configurations']['flink-env']['env.pid.dir']
flink_log_dir = config['configurations']['flink-env']['env.log.dir']

flink_version = config['configurations']['flink-env']['flink_version']

security_enabled = config['configurations']['cluster-env']['security_enabled']

hostname = config['hostname']
flink_jobmanagers = config['clusterHostInfo']['flink_jobmanager_hosts']
flink_taskmanagers = config['clusterHostInfo']['flink_taskmanager_hosts']
flink_jobmanager = flink_jobmanagers[0]

if security_enabled:
  kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
  kdestroy_path_local = kinit_path_local.replace('kinit', 'kdestroy')
  _flink_principal_name = config['configurations']['flink-env']['flink_principal_name']
  flink_jaas_principal = _flink_principal_name.replace('_HOST',hostname.lower())
  flink_client_jass_path = "/etc/flink/conf/flink_client_jaas.conf"
  flink_keytab = "/etc/security/keytabs/flink.headless.keytab"
  krb5_conf_path = "/etc/krb5.conf"
  flink_krb_ticket_renew_window = config['configurations']['flink-env']['flink_krb_ticket_renew_window']
  hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
  hdfs_principal_name = default('/configurations/hadoop-env/hdfs_principal_name', None)

is_alluxio_installed = False
# fs_default_scheme = 'file:///'
if 'alluxio_master_hosts' in config['clusterHostInfo']:
  is_alluxio_installed = True
  alluxio_master = config['clusterHostInfo']['alluxio_master_hosts'][0]
  alluxio_version = config['configurations']['alluxio-env']['alluxio_version']
  # alluxio jar params
  jar_url = 'https://public-repo.radicalbit.io/jars'
  alluxio_jar_name = format('alluxio-core-client-{alluxio_version}-jar-with-dependencies.jar')



state_backend_checkpointdir = "/flink/checkpoint"

if flink_version == '1.1.2':
  recovery_mode = config['configurations']['flink-conf']['recovery.mode']
  recovery_zookeeper_path_root = '/flink/recovery'
else
  recovery_mode = config['configurations']['flink-conf']['high-availability']
  recovery_zookeeper_path_root = '/flink/high-availability'

zookeeper_quorum = ''
recovery_zookeeper_path_root = ''
recovery_zookeeper_storage_dir = ''

if recovery_mode == 'zookeeper':
  zookeeper_port = str(config['configurations']['zoo.cfg']['clientPort'])
  if 'zookeeper_hosts' in config['clusterHostInfo']:
    zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
    if len(zookeeper_hosts_list) > 0:
      zookeeper_quorum = (':' + zookeeper_port + ',').join(zookeeper_hosts_list) + ':' + zookeeper_port

  recovery_zookeeper_storage_dir = format('{hdfs_default_name}{recovery_zookeeper_path_root}')
