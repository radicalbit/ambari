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

class FlinkMaster(FlinkService):


  def start(self, env):
    import params
    self.configure(env)
    self.create_hdfs_user(params.flink_user)

    Execute(format("export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/jobmanager.sh start cluster {hostname} {jobmanager_web_port}"), user=params.flink_user)

  def stop(self, env):
    import params
    env.set_params(params)
    Execute(format("nohup {bin_dir}/jobmanager.sh stop"), user=params.flink_user)
    Execute(format("rm -f {flink_pid_dir}/flink-{flink_user}-jobmanager.pid"), user=params.flink_user)

  def status(self, env):
    # import status_params as params
    # env.set_params(params)
    # pid_file = format("{flink_pid_dir}/flink-{flink_user}-jobmanager.pid")
    # check_process_status(pid_file)
    pid_file = "/var/run/flink/flink-flink-jobmanager.pid"
    check_process_status(pid_file)


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

if __name__ == "__main__":
  FlinkMaster().execute()
