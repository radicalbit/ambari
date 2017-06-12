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
from flink_service import FlinkService

class FlinkJobManager(FlinkService):


  def start(self, env):
    import params
    self.configure(env)

    if params.security_enabled:
      if params.flink_version == '1.1.2':
        Execute(format("{kinit_path_local} -kt {hdfs_user_keytab} {hdfs_principal_name}"),
                user = params.hdfs_user)
      #TODO: add security configurations for 1.2.0 and above

    self.create_hdfs_user(params.flink_user)

    if params.security_enabled:
      if params.flink_version == '1.1.2':
        self.start_krb_session(env)
      #TODO: add security configurations for 1.2.0 and above

    Execute(format("rm -f {flink_pid_dir}/flink-{flink_user}-jobmanager.pid"),
            only_if=format("test -f {flink_pid_dir}/flink-{flink_user}-jobmanager.pid"),
            user=params.flink_user)

    Logger.info('Starting Flink JobManager...')
    Execute(format("export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/jobmanager.sh start cluster"), user=params.flink_user)
    Logger.info('Flink JobManager Started...')

  def stop(self, env):
    import params
    env.set_params(params)

    Execute(format("{bin_dir}/jobmanager.sh stop"), user=params.flink_user)

    if params.security_enabled:
      if params.flink_version == '1.1.2':
        self.stop_krb_session(env)
      #TODO: add security configurations for 1.2.0 and above

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{flink_pid_dir}/flink-{flink_user}-jobmanager.pid")
    check_process_status(pid_file)

  def create_hdfs_user(self, user):
    import params

    Execute('hdfs dfs -mkdir -p /user/' + user, user=params.hdfs_user, not_if ='hdfs dfs -ls /user/' + user)
    Execute('hdfs dfs -mkdir -p /' + user, user=params.hdfs_user, not_if ='hdfs dfs -ls /' + user)
    Execute('hdfs dfs -mkdir -p ' + params.recovery_zookeeper_path_root, user=params.hdfs_user, not_if ='hdfs dfs -ls ' + params.recovery_zookeeper_path_root)
    Execute('hdfs dfs -mkdir -p ' + params.state_backend_checkpointdir, user=params.hdfs_user, not_if ='hdfs dfs -ls ' + params.state_backend_checkpointdir)

    Execute('hdfs dfs -chown ' + user + ' /user/'+user, user=params.hdfs_user)
    Execute('hdfs dfs -chown ' + user + ' /'+user, user=params.hdfs_user)
    Execute('hdfs dfs -chown ' + user + ' ' + params.recovery_zookeeper_path_root, user=params.hdfs_user)
    Execute('hdfs dfs -chown ' + user + ' ' + params.state_backend_checkpointdir, user=params.hdfs_user)

    Execute('hdfs dfs -chgrp ' + user + ' /user/'+user, user=params.hdfs_user)
    Execute('hdfs dfs -chgrp ' + user + ' /'+user, user=params.hdfs_user)
    Execute('hdfs dfs -chgrp ' + user + ' ' + params.recovery_zookeeper_path_root, user=params.hdfs_user)
    Execute('hdfs dfs -chgrp ' + user + ' ' + params.state_backend_checkpointdir, user=params.hdfs_user)

    if params.flink_version != '1.1.2':
      Execute('hdfs dfs -mkdir -p ' + params.state_savepoints_dir, user=params.hdfs_user, not_if ='hdfs dfs -ls ' + params.state_savepoints_dir)
      Execute('hdfs dfs -chown ' + user + ' ' + params.state_savepoints_dir, user=params.hdfs_user)
      Execute('hdfs dfs -chgrp ' + user + ' ' + params.state_savepoints_dir, user=params.hdfs_user)

    if params.flink_version == '1.3.0':
      Execute('hdfs dfs -mkdir -p ' + params.jobmanager_archive_fs_dir, user=params.hdfs_user, not_if ='hdfs dfs -ls ' + params.jobmanager_archive_fs_dir)
      Execute('hdfs dfs -chown ' + user + ' ' + params.jobmanager_archive_fs_dir, user=params.hdfs_user)
      Execute('hdfs dfs -chgrp ' + user + ' ' + params.jobmanager_archive_fs_dir, user=params.hdfs_user)

if __name__ == "__main__":
  FlinkJobManager().execute()
