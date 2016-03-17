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
import sys, os, pwd, signal, time
from resource_management.core.resources.system import File, Execute
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.core.logger import Logger
from alluxio import Alluxio

class Master(Alluxio):

  def install(self, env):
    import params
    Logger.info('Installing the Alluxio master...')
    self.base_install(env)
    self.configure(env)
    Logger.info('Alluxio master installed')

  def start(self, env):
    import params
    self.configure(env)
    env.set_params(params)

    self.alluxio_master_format_marker = os.path.join(params.pid_dir, 'ALLUXIO_MASTER_FORMATTED')
    if not os.path.exists(self.alluxio_master_format_marker):

      Logger.info('Formatting the Alluxio master...')

      # the following steps are needed to format correctly the journal of alluxio
      # 1-create as hdfs the journal folder
      folders = params.journal_relative_path.split('/')[1:]
      Execute('hdfs dfs -mkdir /' + folders[0], user='hdfs')
      Execute('hdfs dfs -mkdir ' + params.journal_relative_path, user='hdfs')
      # 2-change owner to root
      Execute('hdfs dfs -chown -R ' + params.root_user + ':' + params.user_group + ' /' + folders[0], user='hdfs')
      # 3-format the cluster as root
      Execute(params.base_dir + '/bin/alluxio format', user=params.root_user)
      # 4-change owner to alluxio
      Execute('hdfs dfs -chown -R ' + params.alluxio_user + ':' + params.user_group + ' /' + folders[0], user='hdfs')

      # update permissions on log folder
      Execute('chown -R ' + params.alluxio_user + ':' + params.user_group + ' ' + params.log_dir, user=params.root_user)

      # update permissions on user.log file
      Execute('chmod u=rw,g=rw,o=r ' + params.log_dir + '/user.log', user=params.root_user)

      # create marker
      open(self.alluxio_master_format_marker, 'a').close()
      Logger.info('Alluxio master formatted')

    Logger.info('Starting Alluxio master...')
    Execute(params.base_dir + '/bin/alluxio-start.sh master', user=params.alluxio_user)
    Logger.info('Alluxio master started...')

    Logger.info('Creating pid file for Alluxio master...')
    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioMaster | awk '{print $1}'`> " + params.pid_dir + "/alluxio-master.pid"
    Execute(cmd, user=params.alluxio_user)
    Logger.info('Pid file created for Alluxio master')

  #Called to stop the service using alluxio provided stop
  def stop(self, env):
    import params
    Execute(params.base_dir + '/bin/alluxio-stop.sh ' + 'master', user=params.alluxio_user)

  #Called to get status of the service using the pidfile
  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{pid_dir}/alluxio-master.pid")
    check_process_status(pid_file)

if __name__ == "__main__":
  Master().execute()
