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
from resource_management import *

def cassandra(action = None):
  import params

  if action == 'install':
    security_folder = '/etc/security/limits.d'

    File(
        format("{security_folder}/{cassandra_user}.conf"),
        owner='root',
        mode=0644,
        content=Template('cassandra.conf.j2', conf_dir=security_folder)
    )

    # Execute(format('echo "* - nproc 32768" >> {security_folder}/90-nproc.conf'), user='root')
    #
    # Execute('echo "vm.max_map_count = 131072" >> /etc/sysctl.conf', user='root')
    #
    # Execute('sysctl -p', user='root')
    #
    # Execute('swapoff --all', user='root')

  else :

    Directory(
        [params.commitlog_directory, params.data_file_directories, params.saved_caches_directory],
        owner=params.cassandra_user,
        group=params.user_group,
        recursive=True
    )

    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.commitlog_directory}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.data_file_directories}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.saved_caches_directory}'), user='root')

    # File(
    #     format("{params.cassandra_conf_dir}/cassandra.yaml"),
    #     owner=params.cassandra_user,
    #     mode=0644,
    #     content=Template('cassandra.yaml.j2', conf_dir=params.cassandra_conf_dir)
    # )

    File(
        format("{params.cassandra_conf_dir}/cassandra-env.sh"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra-env.sh.j2', conf_dir=params.cassandra_conf_dir)
    )

    File(
        format("{params.cassandra_bin_dir}/cassandra"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra.j2', conf_dir=params.cassandra_bin_dir)
    )