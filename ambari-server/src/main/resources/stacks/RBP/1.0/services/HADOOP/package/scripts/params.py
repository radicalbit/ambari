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
from resource_management.libraries.functions.default import default

config = Script.get_config()

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
yarn_user = config['configurations']['yarn-env']['yarn_user']
user_group = config['configurations']['cluster-env']['user_group']

java_home = config['hostLevelParams']['java_home']
current_host = config['hostname']

binary_file_md5 = 'c442bd89b29cab9151b5987793b94041'
hadoop_download_link = "https://dist.apache.org/repos/dist/release/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz"
hadoop_tmp_file = "/tmp/hadoop-2.7.2.tar.gz"

#service_packagedir = os.path.realpath(__file__).split('/scripts')[0]
#hdfs_archive_file = service_packagedir + '/files/hadoop-2.7.2.tar.gz'

hadoop_base_dir = config['configurations']['hadoop-env']['hadoop.base.dir']
hadoop_conf_dir = hadoop_base_dir + '/etc/hadoop'

hadoop_tmp_dir = config['configurations']['hadoop-env']['hadoop.tmp.dir']
hadoop_pid_dir = config['configurations']['hadoop-env']['hadoop.pid.dir']

dfs_nameservices = config['configurations']['hadoop-env']['dfs.nameservices']
dfs_repication = config['configurations']['hadoop-env']['dfs.repication']
dfs_namenode_dir = config['configurations']['hadoop-env']['dfs.namenode.name.dir']
dfs_datanode_dir = config['configurations']['hadoop-env']['dfs.datanode.data.dir']
dfs_journalnode_dir = config['configurations']['hadoop-env']['dfs.journalnode.edits.dir']

hdfs_primary_namenode = default("/clusterHostInfo/namenode_host", [])[0]
hdfs_primary_zkfc = default("/clusterHostInfo/zkfc_hosts", [])[0]
hdfs_namenodes = default("/clusterHostInfo/namenode_host", [])
hdfs_snamenode = default("/clusterHostInfo/snamenode_host", [])[0]
hdfs_datanode = default("/clusterHostInfo/slave_hosts", [])
hdfs_journalnodes = default("/clusterHostInfo/journalnode_hosts", [])
hdfs_zkfc = default("/clusterHostInfo/zkfc_hosts", [])

zookeeper_hosts = default("/clusterHostInfo/zookeeper_hosts", [])
dfs_namenodes = 'nn' + ",nn".join(str(host) for host in range(1, len(hdfs_namenodes)+1))

yarn_resourcemanager = default("/clusterHostInfo/yarn_resourcemanager_hosts", [])[0]


