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
cluster_seeds = cassandra_seeds = config['clusterHostInfo']['elasticsearch_master_hosts']
cluster_seed_head = cluster_seeds[0]
discovery_zen_ping_unicast_hosts = '"' + (':' + str(transport_tcp_port) + '","').join(cluster_seeds) + ':' + str(transport_tcp_port) + '"'
# seed_node1 = config['configurations']['elastic-site']['seed_node1']
# seed_node2 = config['configurations']['elastic-site']['seed_node2']
# seed_node3 = config['configurations']['elastic-site']['seed_node3']

path_data = config['configurations']['elastic-site']['path_data']
log_dir = config['configurations']['elastic-site']['elastic_log_dir']
plugins_dir = config['configurations']['elastic-site']['elastic_plugins_dir']

# recover_after_time = config['configurations']['elastic-site']['recover_after_time']
# recover_after_data_nodes = config['configurations']['elastic-site']['recover_after_data_nodes']
# expected_data_nodes = config['configurations']['elastic-site']['expected_data_nodes']
# discovery_zen_ping_multicast_enabled = config['configurations']['elastic-site']['discovery_zen_ping_multicast_enabled']
# index_merge_scheduler_max_thread_count = config['configurations']['elastic-site']['index_merge_scheduler_max_thread_count']
# index_translog_flush_threshold_size = config['configurations']['elastic-site']['index_translog_flush_threshold_size']
# index_refresh_interval = config['configurations']['elastic-site']['index_refresh_interval']
# index_store_throttle_type = config['configurations']['elastic-site']['index_store_throttle_type']
# index_number_of_shards = config['configurations']['elastic-site']['index_number_of_shards']
# index_number_of_replicas = config['configurations']['elastic-site']['index_number_of_replicas']
# index_buffer_size = config['configurations']['elastic-site']['index_buffer_size']
# mlockall = config['configurations']['elastic-site']['mlockall']
# threadpool_bulk_queue_size = config['configurations']['elastic-site']['threadpool_bulk_queue_size']
# cluster_routing_allocation_node_concurrent_recoveries = config['configurations']['elastic-site']['cluster_routing_allocation_node_concurrent_recoveries']
# cluster_routing_allocation_disk_watermark_low = config['configurations']['elastic-site']['cluster_routing_allocation_disk_watermark_low']
# cluster_routing_allocation_disk_threshold_enabled = config['configurations']['elastic-site']['cluster_routing_allocation_disk_threshold_enabled']
# cluster_routing_allocation_disk_watermark_high = config['configurations']['elastic-site']['cluster_routing_allocation_disk_watermark_high']
# indices_fielddata_cache_size = config['configurations']['elastic-site']['indices_fielddata_cache_size']
# indices_cluster_send_refresh_mapping = config['configurations']['elastic-site']['indices_cluster_send_refresh_mapping']
# threadpool_index_queue_size = config['configurations']['elastic-site']['threadpool_index_queue_size']
#
# discovery_zen_ping_timeout = config['configurations']['elastic-site']['discovery_zen_ping_timeout']
# discovery_zen_fd_ping_interval = config['configurations']['elastic-site']['discovery_zen_fd_ping_interval']
# discovery_zen_fd_ping_timeout = config['configurations']['elastic-site']['discovery_zen_fd_ping_timeout']
# discovery_zen_fd_ping_retries = config['configurations']['elastic-site']['discovery_zen_fd_ping_retries']