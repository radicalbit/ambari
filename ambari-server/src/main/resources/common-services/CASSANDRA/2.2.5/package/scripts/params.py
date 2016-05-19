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
from utils import get_property_value
from resource_management.libraries.script.script import Script

config = Script.get_config()

configurations = config['configurations']
cassandra_env = configurations['cassandra-env']
cassandra_conf = configurations['cassandra-conf']

# alluxio jar params
jar_url = 'https://public-repo.radicalbit.io/jars'
cassandra_jar_name = 'cassandra‐kerberosauthentication‐1.0.jar'

security_enabled = configurations['cluster-env']['security_enabled']

def get_host_ip(name, host_names, host_ips):
  host_ip = ''
  counter = 0
  for hostname in host_names:
    if name == hostname:
      host_ip = host_ips[counter]
    counter = counter + 1
  return host_ip

nodes_hostname = config['clusterHostInfo']['all_hosts']
nodes_ip = config['clusterHostInfo']['all_ipv4_ips']

hostname = config['hostname']
host_ip = get_host_ip(hostname, nodes_hostname, nodes_ip)

cassandra_user = cassandra_env['cassandra_user']
user_group = config['configurations']['cluster-env']['user_group']

cassandra_install_dir = '/usr/lib/cassandra'
cassandra_conf_dir = cassandra_install_dir + '/conf'
cassandra_bin_dir = cassandra_install_dir + '/bin'

cassandra_log_dir = cassandra_env['cassandra_log_dir']
cassandra_pid_dir = cassandra_env['cassandra_pid_dir']

cluster_name = cassandra_conf['cluster_name']

authorizer = cassandra_conf['authorizer']
commitlog_directory = cassandra_conf['commitlog_directory']
data_file_directories = cassandra_conf['data_file_directories']
saved_caches_directory = cassandra_conf['saved_caches_directory']

listen_address = hostname
rpc_address = hostname

cassandra_seeds = config['clusterHostInfo']['cassandra_seed_hosts']
cassandra_nodes = config['clusterHostInfo']['cassandra_node_hosts']

cassandra_nodes_number = len(cassandra_seeds) + len(cassandra_nodes)

seed_node_head = cassandra_seeds[0]

seeds = ",".join(cassandra_seeds)

max_heap_size = cassandra_env['max_heap_size']
heap_new_size = cassandra_env['heap_new_size']

if security_enabled:
  realm = 'EXAMPLE.COM'
  krb5_conf_data = get_property_value(configurations, 'krb5-conf')
  kerberos_env = get_property_value(configurations, "kerberos-env")
  if kerberos_env is not None:
    realm = get_property_value(kerberos_env, "realm", None, True, None)
  if krb5_conf_data is not None:
    realm = get_property_value(krb5_conf_data, 'realm', realm)

  authenticator = 'io.radicalbit.cassandra.kerberosauthentication.KerberosAuthenticator'

  if hostname in cassandra_seeds:
    cassandra_keytab = config['configurations']['cassandra-env']['cassandra_seed_keytab']
  else:
    cassandra_keytab = config['configurations']['cassandra-env']['cassandra_node_keytab']

else:
  authenticator = 'AllowAllAuthenticator'