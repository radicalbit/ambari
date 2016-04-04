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
from cassandra import cassandra
from join_check import join_check

class CassandraNode(Script):

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)
    cassandra('install')


  def configure(self, env):
    import params
    env.set_params(params)
    cassandra()

  def start(self, env):
    import params
    self.configure(env)

    if join_check(params.host_ip, params.seed_node_head) == True:
      Execute(
          format('{params.cassandra_bin_dir}/cassandra -p {params.cassandra_pid_dir}/cassandra.pid'),
          user=params.cassandra_user
      )

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
