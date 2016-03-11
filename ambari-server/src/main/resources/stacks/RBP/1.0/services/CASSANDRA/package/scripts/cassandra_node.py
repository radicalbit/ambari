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

class CassandraNode(Script):

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)

    security_folder = '/etc/security/limits.d'

    File(
        format("{security_folder}/{cassandra_user}.conf"),
        owner='root',
        mode=0644,
        content=Template('cassandra.conf.j2', conf_dir=security_folder)
    )

    Execute(format('echo "* - nproc 32768" >> {security_folder}/90-nproc.conf'), user='root')

    Execute('echo "vm.max_map_count = 131072" >> /etc/sysctl.conf', user='root')

    Execute('sysctl -p', user='root')

    Execute('swapoff --all', user='root')


  def configure(self, env):
    import params
    env.set_params(params)

    File(
        format("{params.cassandra_conf_dir}/cassandra.yaml"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra.yaml.j2', conf_dir=params.cassandra_conf_dir)
    )

    File(
        format("{params.cassandra_conf_dir}/cassandra-env.sh"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra-env.sh.j2', conf_dir=params.cassandra_conf_dir)
    )

  def start(self, env):
    import params
    self.configure(env)
    Execute(
        format('{params.cassandra_bin_dir}/cassandra -p {params.cassandra_pid_dir}/cassandra.pid'),
        user=params.cassandra_user
    )
    # cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep CassandraDaemon | awk '{print $1}'`> " + params.cassandra_pid_file + "/cassandra.pid"
    # Execute(cmd, user=params.cassandra_user)

  def stop(self, env):
    import params
    Execute(format('kill `cat {params.cassandra_pid_dir}/cassandra.pid`'), user=params.cassandra_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{cassandra_pid_dir}/cassandra.pid")
    check_process_status(pid_file)


if __name__ == '__main__':
  CassandraNode().execute()
