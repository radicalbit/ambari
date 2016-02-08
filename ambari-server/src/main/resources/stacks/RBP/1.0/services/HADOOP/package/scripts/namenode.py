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
from hdfs import HDFS

class Namenode(HDFS):

  def install(self, env):
    import params
    self.base_install(env)
    Execute(
        'cp ' + params.hadoop_conf_dir + '/mapred-site.xml.template ' + params.hadoop_conf_dir + '/mapred-site.xml',
        user=params.hdfs_user
    )
    Execute(params.hadoop_base_dir + '/bin/hdfs namenode -format', user=params.hdfs_user)

  def configure(self, env):
    import params
    self.base_configure(env)
    env.set_params(params)

    File(
        format("{hadoop_conf_dir}/mapred-site.xml"),
        owner=params.hdfs_user,
        mode=0644,
        content=Template('mapred-site.xml.j2', conf_dir=params.hadoop_conf_dir)
    )

    File(
        format("{tachyon_config_dir}/slaves"),
        owner=params.hdfs_user,
        mode=0644,
        content='\n'.join(params.slave_hosts)
    )

  def start(self, env):
    import params
    self.configure(env)

    Execute(params.hadoop_base_dir + '/bin/hdfs namenode', user=params.hdfs_user)

    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep NameNode | awk '{print $1}'`> " + params.hadoop_pid_dir + "/namenode.pid"
    Execute(cmd, user=params.hdfs_user)

  def stop(self, env):
    import params
    Execute('kill `cat ' + params.hadoop_pid_dir + '/namenode.pid`', user=params.hdfs_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{hadoop_pid_dir}/namenode.pid")
    check_process_status(pid_file)

if __name__ == "__main__":
  Namenode().execute()