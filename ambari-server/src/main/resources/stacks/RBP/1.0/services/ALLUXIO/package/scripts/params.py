#!/usr/bin/config python
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
#import os
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import conf_select

# config object that holds the configurations declared in the -config.xml file
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

# alluxio installation dir
base_dir = '/usr/lib/alluxio'

# alluxio config dir
alluxio_config_dir = '/etc/alluxio/conf'

# alluxio underfs address
underfs_addr = config['configurations']['core-site']['fs.defaultFS']
#fs_root = config['configurations']['core-site']['fs.defaultFS']

# hadoop core-site.xml dir
hadoop_core_site = conf_select.get_hadoop_conf_dir() + '/core-site.xml'

# alluxio master journal relative path
journal_relative_path = '/alluxio/journal'

# alluxio master journal folder
journal_addr = config['configurations']['core-site']['fs.defaultFS'] + journal_relative_path

# alluxio worker evictor class
evictor_class = config['configurations']['alluxio-config']['alluxio.worker.evictor.class']

# alluxio worker memory alotment
worker_mem = config['configurations']['alluxio-config']['alluxio.worker.memory']

# alluxio worker tieredstore levels
tieredstore_levels = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.levels']

# alluxio worker tieredstore level0 alias
tieredstore_level0_alias = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level0.alias']

# alluxio worker tieredstore level0 dirs path
tieredstore_level0_dirs_path = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level0.dirs.path']

# alluxio worker tieredstore level0 dirs quota
tieredstore_level0_dirs_quota = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level0.dirs.quota']

# alluxio worker tieredstore level0 reserved ratio
tieredstore_level0_reserved_ratio = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level0.reserved.ratio']

# alluxio worker tieredstore level1 alias
tieredstore_level1_alias = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level1.alias']

# alluxio worker tieredstore level1 dirs path
tieredstore_level1_dirs_path = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level1.dirs.path']

# alluxio worker tieredstore level1 dirs quota
tieredstore_level1_dirs_quota = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level1.dirs.quota']

# alluxio worker tieredstore level1 reserved ratio
tieredstore_level1_reserved_ratio = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level1.reserved.ratio']

# alluxio worker tieredstore level2 alias
tieredstore_level2_alias = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level2.alias']

# alluxio worker tieredstore level2 dirs path
tieredstore_level2_dirs_path = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level2.dirs.path']

# alluxio worker tieredstore level2 dirs quota
tieredstore_level2_dirs_quota = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level2.dirs.quota']

# alluxio worker tieredstore level2 reserved ratio
tieredstore_level2_reserved_ratio = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.level2.reserved.ratio']

# alluxio worker tieredstore reserver enabled
tieredstore_reserver_enabled = config['configurations']['alluxio-config']['alluxio.worker.tieredstore.reserver.enabled']

# alluxio user file readtype default
readtype = config['configurations']['alluxio-config']['alluxio.user.file.readtype.default']

# alluxio user file write location policy class
write_location_policy = config['configurations']['alluxio-config']['alluxio.user.file.write.location.policy.class']

# alluxio user file writetype default
writetype = config['configurations']['alluxio-config']['alluxio.user.file.writetype.default']

# alluxio log dir
log_dir = config['configurations']['alluxio-env']['alluxio.log.dir']

# alluxio pid dir
pid_dir = config['configurations']['alluxio-env']['alluxio.pid.dir']

root_user = config['configurations']['alluxio-env']['root_user']
alluxio_user = config['configurations']['alluxio-env']['alluxio_user']
user_group = config['configurations']['cluster-env']['user_group']

ambari_server = config['clusterHostInfo']['ambari_server_host']

# alluxio addresses

alluxio_master = config['clusterHostInfo']['alluxio_master_hosts'][0]
# if config['clusterHostInfo']['hostname'] in config['clusterHostInfo']['alluxio_master_hosts']:
#   alluxio_master = config['clusterHostInfo']['hostname']
# else:
#   alluxio_master = config['clusterHostInfo']['alluxio_master_hosts'][0]

alluxio_workers = config['clusterHostInfo']['alluxio_slave_hosts']

# zookeeper infos
zookeeper_hosts = ''
zookeeper_port = str(config['configurations']['zoo.cfg']['clientPort'])
if 'zookeeper_hosts' in config['clusterHostInfo']:
  zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
  if len(zookeeper_hosts_list) > 0:
      zookeeper_hosts = ':' + zookeeper_port + ','.join(zookeeper_hosts_list) + ':' + zookeeper_port

use_zookeeper = False
if zookeeper_hosts != '' and config['configurations']['alluxio-env']['alluxio.usezookeeper'] == 'True':
  use_zookeeper = True

# alluxio master keytab file
master_keytab = config['configurations']['alluxio-config']['alluxio.master.keytab.file']

# alluxio master principal
master_principal = config['configurations']['alluxio-config']['alluxio.master.principal']

# alluxio worker keytab file
worker_keytab = config['configurations']['alluxio-config']['alluxio.worker.keytab.file']

# alluxio worker principal
worker_principal = config['configurations']['alluxio-config']['alluxio.worker.principal']
