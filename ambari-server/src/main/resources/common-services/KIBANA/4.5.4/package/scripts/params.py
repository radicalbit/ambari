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

# server configurations
config = Script.get_config()

conf_dir = '/etc/kibana/conf.dist'
optimize_dir = '/usr/lib/kibana/optimize'

hostname = config['hostname']

kibana_user = config['configurations']['kibana-env']['kibana_user']
user_group = config['configurations']['cluster-env']['user_group']

log_dir = config['configurations']['kibana-env']['kibana_log_dir']
log_file = format("{log_dir}/kibana.log")
pid_dir = config['configurations']['kibana-env']['kibana_pid_dir']
pid_file = format("{pid_dir}/kibana.pid")

server_port = config['configurations']['kibana-site']['server_port']
kibana_index = config['configurations']['kibana-site']['kibana_index']

elasticsearch_ping_timeout = config['configurations']['kibana-site']['elasticsearch_ping_timeout']
elasticsearch_request_timeout = config['configurations']['kibana-site']['elasticsearch_request_timeout']
elasticsearch_shard_timeout = config['configurations']['kibana-site']['elasticsearch_shard_timeout']
elasticsearch_startup_timeout = config['configurations']['kibana-site']['elasticsearch_startup_timeout']

es_host = cassandra_seeds = config['clusterHostInfo']['elasticsearch_master_hosts'][0]
es_port = config['configurations']['elastic-site']['http_port']