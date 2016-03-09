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
import sys, os, pwd, signal, time, hashlib
from resource_management import *
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.core import sudo
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
from subprocess import call
import yaml

class CassandraNode(Script):

  def install(self, env):
    # import params
    self.install_packages(env)

    #
    # if not os.path.exists(params.cassandra_tmp_file):
    #   Execute(
    #       'wget '+params.cassandra_download_link+' -O '+params.cassandra_tmp_file+' -a /tmp/cassandra_download.log',
    #       user=params.cassandra_user
    #   )
    # else:
    #   hadoop_tmp_file_md5 = hashlib.md5(open(params.cassandra_tmp_file, "rb").read()).hexdigest()
    #
    #   if not hadoop_tmp_file_md5 == params.binary_file_md5:
    #     Execute(
    #         'rm -f '+params.cassandra_tmp_file,
    #         user=params.cassandra_user
    #     )
    #
    #     Execute(
    #         'wget '+params.cassandra_download_link+' -O '+params.cassandra_tmp_file+' -a /tmp/cassandra_download.log',
    #         user=params.cassandra_user
    #     )
    #
    # Directory(
    #     [params.cassandra_install_dir, params.cassandra_pid_file, params.commitlog_directory,
    #      params.data_file_directories, params.saved_caches_directory],
    #     owner=params.cassandra_user,
    #     group=params.user_group,
    #     recursive=True
    # )
    #
    # Execute(
    #     '/bin/tar -zxvf ' + params.cassandra_tmp_file + ' --strip 1 -C ' + params.cassandra_install_dir,
    #     user=params.cassandra_user
    # )

  def configure(self, env):
    import params
    env.set_params(params)

    # File(
    #     os.path.join(params.cassandra_conf_dir, 'cassandra.yaml'),
    #     content=yaml.safe_dump(params.cassandra_configs),
    #     mode=0644,
    #     owner=params.cassandra_user
    # )

    File(
        format("{cassandra_conf_dir}/cassandra.yaml"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra.yaml', conf_dir=cassandra_conf_dir)
    )

    File(
        format("{cassandra_conf_dir}/cassandra-env.sh"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra-env.sh', conf_dir=cassandra_conf_dir)
    )

  def start(self, env):
    import params
    self.configure(env)
    Execute(
        format('{params.cassandra_bin_dir}/cassandra -p {params.cassandra_pid_file}/cassandra.pid'),
        user=params.cassandra_user
    )
    # cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep CassandraDaemon | awk '{print $1}'`> " + params.cassandra_pid_file + "/cassandra.pid"
    # Execute(cmd, user=params.cassandra_user)

  def stop(self, env):
    import params
    Execute('kill `cat ' + params.cassandra_pid_file + '/cassandra.pid`', user=params.cassandra_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{cassandra_pid_dir}/cassandra.pid")
    check_process_status(pid_file)


if __name__ == '__main__':
  CassandraNode().execute()
