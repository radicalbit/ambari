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

from resource_management.libraries.functions.version import format_hdp_stack_version, compare_versions
from resource_management import *
import status_params

# server configurations
config = Script.get_config()

elastic_home = '/etc/elasticsearch/'
elastic_bin = '/usr/share/elasticsearch/bin/'

conf_dir = "/etc/elasticsearch/conf.dist/"
elastic_user = config['configurations']['elastic-env']['elastic_user']
user_group = config['configurations']['cluster-env']['user_group']
pid_dir = config['configurations']['elastic-env']['elastic_pid_dir']
pid_file = format("{pid_dir}/elasticsearch.pid")
hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
es_insicure_allow_root = config['configurations']['elastic-env']['es_insicure_allow_root']
elastic_env_sh_template = config['configurations']['elastic-env']['content']

http_port = config['configurations']['elastic-site']['http_port']
transport_tcp_port = config['configurations']['elastic-site']['transport_tcp_port']

cluster_name = config['configurations']['elastic-site']['cluster_name']
cluster_seeds = config['clusterHostInfo']['elasticsearch_master_hosts']
cluster_slaves = default('clusterHostInfo/elasticsearch_slave_hosts', [])
cluster_nodes = cluster_seeds + cluster_slaves
discovery_zen_ping_unicast_hosts = '"' + (':' + str(transport_tcp_port) + '","').join(cluster_nodes) + ':' + str(transport_tcp_port) + '"'
cluster_seed_head = cluster_seeds[0]

path_data = config['configurations']['elastic-site']['path_data']
log_dir = config['configurations']['elastic-site']['elastic_log_dir']
plugins_dir = config['configurations']['elastic-site']['elastic_plugins_dir']
