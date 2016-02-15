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
from hadoop import Hadoop

class JournalNode(Hadoop):

  def install(self, env):
    self.base_install(env)

  def configure(self, env):
    self.base_configure(env)

  def start(self, env):
    import params
    self.configure(env)

    Execute(params.hadoop_base_dir + '/bin/hdfs journalnode &', user=params.hdfs_user)

    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | org.apache.hadoop.hdfs.qjournal.server.JournalNode | awk '{print $1}'`> " + params.hadoop_pid_dir + "/journalnode.pid"
    Execute(cmd, user=params.hdfs_user)

  def stop(self, env):
    import params
    Execute('kill `cat ' + params.hadoop_pid_dir + '/journalnode.pid`', user=params.hdfs_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{hadoop_pid_dir}/journalnode.pid")
    check_process_status(pid_file)

if __name__ == "__main__":
  JournalNode().execute()