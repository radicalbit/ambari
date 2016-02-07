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
import os
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default

config = Script.get_config()

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
user_group = config['configurations']['cluster-env']['user_group']

java_home = config['hostLevelParams']['java_home']

binary_file_md5 = 'c442bd89b29cab9151b5987793b94041'

service_packagedir = os.path.realpath(__file__).split('/scripts')[0]
hdfs_archive_file = service_packagedir + '/files/hadoop-2.7.2.tar.gz'

hadoop_base_dir = config['configurations']['hadoop-env']['hadoop.base.dir']
hadoop_conf_dir = hadoop_base_dir + '/etc/hadoop'

hadoop_tmp_dir = config['configurations']['hadoop-env']['hadoop.tmp.dir']
hadoop_pid_dir = config['configurations']['hadoop-env']['hadoop.pid.dir']

dfs_repication = config['configurations']['hadoop-env']['dfs.repication']
dfs_namenode_dir = config['configurations']['hadoop-env']['dfs.namenode.name.dir']
dfs_datanode_dir = config['configurations']['hadoop-env']['dfs.datanode.data.dir']

hdfs_namenode = ''
slave_hosts = default("/clusterHostInfo/slave_hosts", [])
