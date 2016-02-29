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

# config object that holds the configurations declared in the -config.xml file
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()


# alluxio underfs address
underfs_addr = config['configurations']['alluxio-config']['alluxio.underfs.address']

# alluxio worker memory alotment
worker_mem = config['configurations']['alluxio-config']['alluxio.worker.memory']

# alluxio log dir
log_dir = config['configurations']['alluxio-env']['alluxio.log.dir']

# alluxio pid dir
pid_dir = config['configurations']['alluxio-env']['alluxio.pid.dir']

# alluxio installation dir
base_dir = '/usr/lib/alluxio'

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
if 'zookeeper_hosts' in config['clusterHostInfo']:
  zookeeper_hosts_list = config['clusterHostInfo']['zookeeper_hosts']
  if len(zookeeper_hosts_list) > 0:
      zookeeper_hosts = ':2181,'.join(zookeeper_hosts_list) + ':2181'

use_zookeeper = False
if zookeeper_hosts != '' and config['configurations']['alluxio-env']['alluxio.usezookeeper'] == 'True':
  use_zookeeper = True

