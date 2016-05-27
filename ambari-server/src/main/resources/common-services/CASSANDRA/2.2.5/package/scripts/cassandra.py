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
import os
from resource_management import *

class CassandraComponent(Script):

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

    if not os.path.isfile(format('{security_folder}/90-nproc.conf.pre_cassandra.bak')):
      if os.path.isfile(format('{security_folder}/90-nproc.conf')):
        Execute(format('cp {security_folder}/90-nproc.conf {security_folder}/90-nproc.conf.pre_cassandra.bak'))
      Execute(format('echo "* - nproc 32768" >> {security_folder}/90-nproc.conf'), user='root')

    if not os.path.isfile(format('/etc/sysctl.conf.pre_cassandra.bak')):
      Execute(format('cp /etc/sysctl.conf /etc/sysctl.conf.pre_cassandra.bak'))
      Execute('echo "vm.max_map_count = 131072" >> /etc/sysctl.conf', user='root')

    Execute('sysctl -p', user='root')

    Execute('swapoff --all', user='root')

  def configure(self, env):
    import params
    env.set_params(params)

    Directory(
        [params.commitlog_directory, params.data_file_directories, params.saved_caches_directory, params.cassandra_pid_dir],
        owner=params.cassandra_user,
        group=params.user_group,
        recursive=True
    )

    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.cassandra_log_dir}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.commitlog_directory}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.data_file_directories}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.saved_caches_directory}'), user='root')
    Execute(format('chown -R {params.cassandra_user}:{params.user_group} {params.cassandra_pid_dir}'), user='root')

    File(
        format("{params.cassandra_conf_dir}/cassandra.yaml"),
        owner=params.cassandra_user,
        mode=0644,
        content=Template('cassandra.yaml.j2', conf_dir=params.cassandra_conf_dir)
    )

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

    File(
        format("{params.cassandra_bin_dir}/cassandra.in.sh"),
        owner=params.cassandra_user,
        mode=0700,
        content=Template('cassandra.in.sh.j2', conf_dir=params.cassandra_bin_dir)
    )

    if params.security_enabled:
      File(
          format("{params.cassandra_conf_dir}/kerberos.conf"),
          owner=params.cassandra_user,
          mode=0644,
          content=Template('kerberos.conf.j2', conf_dir=params.cassandra_conf_dir)
      )
      if not os.path.exists(format("{params.cassandra_lib_dir}/{params.cassandra_jar_name}")):
        Execute(
            format('wget {params.jar_url}/{params.cassandra_jar_name} -O {params.cassandra_lib_dir}/{params.cassandra_jar_name} -a /tmp/cassandra_download.log'),
            user='root',
            not_if=format('test -d /tmp/{jar_name}')
        )

  def start(self, env):
    import params
    env.set_params(params)

    filename = 'CASSANDRA_CHANGED'
    file = os.path.join(params.tmp_dir, filename)

    if params.security_enabled:
      if not os.path.exists(file):
        self.run(env)

        cmdfile=format("/tmp/cmds")
        File(cmdfile,
             mode=0600,
             content=InlineTemplate("ALTER KEYSPACE \"system_auth\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : {{ cassandra_nodes_number }};")
             )
        Execute(format("{cassandra_bin_dir}/cqlsh {hostname} 9042 -f {cmdfile}"), logoutput=True)
        Execute(format("{cassandra_bin_dir}/nodetool repair system_auth"), logoutput=True)

        self.stop(env)
        self.configure(env)
        self.run(env)

        open(file, 'a').close()
      else:
        self.configure(env)
        self.run(env)

    else:
      if os.path.exists(file):
        Execute('rm -f {file}', user=params.cassandra_user)

      self.run(env)

  def stop(self, env):
    import params
    env.set_params(params)
    Execute(format('kill `cat {params.cassandra_pid_dir}/cassandra.pid`'), user=params.cassandra_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{cassandra_pid_dir}/cassandra.pid")
    check_process_status(pid_file)

  def run(self, env):
    import params
    env.set_params(params)

    Execute(
        format('{params.cassandra_bin_dir}/cassandra'),
        user=params.cassandra_user
    )
    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep CassandraDaemon | awk '{print $1}'`> " + params.cassandra_pid_dir + "/cassandra.pid"
    Execute(cmd, user=params.cassandra_user)
