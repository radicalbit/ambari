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
import sys, os, pwd, grp, signal, time, glob, hashlib, subprocess
from resource_management import *

class FlinkMaster(Script):
  def install(self, env):

    import params
    import status_params

    self.install_packages(env)

    Directory([status_params.flink_pid_dir, params.flink_log_dir],
              owner=params.flink_user,
              group=params.user_group,
              recursive=True
              )

    alluxio_jar_name = 'alluxio-core-client-1.0.1-jar-with-dependencies.jar'
    self.download_alluxio_client_jar(alluxio_jar_name)
    Execute(format('cp /tmp/{alluxio_jar_name} {params.flink_lib}/'), user='root')

    self.configure(env, True)

  def configure(self, env, isInstall=False):
    import params
    env.set_params(params)

    Directory(
        [params.flink_log_dir,params.flink_pid_dir],
        owner=params.flink_user,
        group=params.user_group,
        recursive=True
    )

    Execute('chmod 777 ' + params.flink_log_dir, user='root')

    File(
        format("{conf_dir}/flink-conf.yaml"),
        owner=params.flink_user,
        mode=0644,
        content=Template('flink-conf.yaml.j2', conf_dir=params.conf_dir)
    )

    Execute(
        format("scp {alluxio_master}:/etc/alluxio/conf/alluxio-site.properties /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(
        format("zip -j /tmp/alluxio-site.jar /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(
        format("cp /tmp/alluxio-site.jar {params.flink_lib}"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )


  def start(self, env):
    import params
    import status_params
    self.configure(env)
    self.create_hdfs_user(params.flink_user)

    # try:
    #   subprocess.check_output('test -f ' + status_params.flink_pid_file + ' && yarn application -status `cat ' + status_params.flink_pid_file + '` | grep "State : RUNNING"', shell=True)
    # except subprocess.CalledProcessError as grepexc:
    #   self.stop(env)

    check_cmd = as_sudo(["test", "-f", status_params.flink_pid_file]) + " && " + as_sudo(["pgrep", "-F", status_params.flink_pid_file])
    longRunningCmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; nohup {bin_dir}/yarn-session.sh -n {flink_numcontainers} -s {cores_number} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")
    if params.flink_streaming:
      longRunningCmd = longRunningCmd + ' -st '
    Execute (longRunningCmd, user=params.flink_user, not_if=check_cmd)

    Execute(format("yarn application -list | grep {flink_appname} | grep -o '\\bapplication_\w*' >") + status_params.flink_pid_file, user=params.flink_user)

  def stop(self, env):
    import params
    import status_params
    Execute('yarn application -kill `cat ' + status_params.flink_pid_file + '`', user=params.flink_user)
    Execute('rm -f ' + status_params.flink_pid_file, user=params.flink_user)

  def status(self, env):
    import status_params
    Execute('yarn application -status `cat ' + status_params.flink_pid_file + '` | grep "State : RUNNING"')


  def create_hdfs_user(self, user):
    import params

    Execute('hdfs dfs -mkdir -p /user/' + user, user='hdfs', not_if ='hdfs dfs -ls /user/' + user)
    Execute('hdfs dfs -mkdir -p /' + user, user='hdfs', not_if ='hdfs dfs -ls /' + user)
    Execute('hdfs dfs -mkdir -p ' + params.recovery_zookeeper_path_root, user='hdfs', not_if ='hdfs dfs -ls ' + params.recovery_zookeeper_path_root)
    Execute('hdfs dfs -mkdir -p ' + params.state_backend_checkpointdir, user='hdfs', not_if ='hdfs dfs -ls ' + params.state_backend_checkpointdir)

    Execute('hdfs dfs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hdfs dfs -chown ' + user + ' /'+user, user='hdfs')
    Execute('hdfs dfs -chown ' + user + ' ' + params.recovery_zookeeper_path_root, user='hdfs')
    Execute('hdfs dfs -chown ' + user + ' ' + params.state_backend_checkpointdir, user='hdfs')

    Execute('hdfs dfs -chgrp ' + user + ' /user/'+user, user='hdfs')
    Execute('hdfs dfs -chgrp ' + user + ' /'+user, user='hdfs')
    Execute('hdfs dfs -chgrp ' + user + ' ' + params.recovery_zookeeper_path_root, user='hdfs')
    Execute('hdfs dfs -chgrp ' + user + ' ' + params.state_backend_checkpointdir, user='hdfs')

  def download_alluxio_client_jar(self, jar_name):
    jar_url = 'http://public-repo.radicalbit.io/jars'

    if not os.path.exists(format('/tmp/{jar_name}')):
      Execute(
          format('wget {jar_url}/{jar_name} -O /tmp/{jar_name} -a /tmp/alluxio_download.log'),
          user='root'
      )

if __name__ == "__main__":
  FlinkMaster().execute()
