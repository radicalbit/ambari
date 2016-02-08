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
from resource_management.libraries.script.script import Script
import sys, os, glob
from resource_management.libraries.functions.version import format_hdp_stack_version
from resource_management.libraries.functions.default import default


    
# server configurations
config = Script.get_config()

binary_file_md5 = '0efef9dc038834823e62f9d305300c2c'
flink_download_url = 'https://dist.apache.org/repos/dist/release/flink/flink-0.10.1/flink-0.10.1-bin-hadoop27-scala_2.11.tgz'
flink_tmp_file = '/tmp/flink-0.10.1-bin-hadoop27-scala_2.11.tgz'

service_packagedir = os.path.realpath(__file__).split('/scripts')[0]
flink_archive_file = service_packagedir + '/files/flink-0.10.1-bin-hadoop27-scala_2.11.tgz'
#flink_archive_file = '/var/lib/ambari-agent/cache/stacks/RBP/1.0/services/FLINK/package/files/flink-0.10.1-bin-hadoop27-scala_2.10.tgz'
    
# params from flink-ambari-config
flink_install_dir = config['configurations']['flink-ambari-config']['flink_install_dir']
flink_numcontainers = config['configurations']['flink-ambari-config']['flink_numcontainers']
flink_jobmanager_memory = config['configurations']['flink-ambari-config']['flink_jobmanager_memory']
flink_container_memory = config['configurations']['flink-ambari-config']['flink_container_memory']
setup_prebuilt = config['configurations']['flink-ambari-config']['setup_prebuilt']
flink_appname = config['configurations']['flink-ambari-config']['flink_appname']
flink_queue = config['configurations']['flink-ambari-config']['flink_queue']
flink_streaming = config['configurations']['flink-ambari-config']['flink_streaming']

hadoop_conf_dir = config['configurations']['flink-ambari-config']['hadoop_conf_dir']
#flink_download_url = config['configurations']['flink-ambari-config']['flink_download_url']
 

conf_dir=''
bin_dir=''

# params from flink-conf.yaml
flink_yaml_content = config['configurations']['flink-env']['content']
flink_user = config['configurations']['flink-env']['flink_user']
flink_group = config['configurations']['flink-env']['flink_group']
flink_log_dir = config['configurations']['flink-env']['flink_log_dir']
flink_log_file = os.path.join(flink_log_dir,'flink-setup.log')



temp_file='/tmp/flink.tgz'
