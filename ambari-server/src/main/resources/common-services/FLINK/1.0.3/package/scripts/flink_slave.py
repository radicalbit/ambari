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

class FlinkSlave(FlinkService):


  def start(self, env):
    import params
    import status_params
    self.configure(env)

    Execute(format("export HADOOP_CONF_DIR={hadoop_conf_dir}; export FLINK_PID_DIR={flink_pid_dir}; nohup {bin_dir}/taskmanager.sh start"), user=params.flink_user)

    # cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep org.apache.flink.runtime.taskmanager.TaskManager | awk '{print $1}'`> " + params.flink_pid_dir + "/flink_master.pid"
    # Execute(cmd, user=params.flink_user)

  def stop(self, env):
    import params
    Execute(format("nohup {bin_dir}/taskmanager.sh stop"), user=params.flink_user)

  def status(self, env):
    import status_params
    env.set_params(status_params)
    pid_file = format("{status_params.flink_pid_dir}/flink-{flink_user}-taskmanager.pid")
    check_process_status(pid_file)

if __name__ == "__main__":
  FlinkSlave().execute()
