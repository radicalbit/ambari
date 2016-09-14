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
from resource_management.core.exceptions import ComponentIsNotRunning
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

    #Format cluster if required
    if params.current_host == params.alluxio_master_head:
      self.format_marker(env)
      if not os.path.exists(self.alluxio_master_format_marker):
        self.format_cluster(env)

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

  def format_marker(self, env):
    import params
    env.set_params(params)
    if params.security_enabled:
      tmp_alluxio_file = 'ALLUXIO_MASTER_FORMATTED_SECURED'
    else:
      tmp_alluxio_file = 'ALLUXIO_MASTER_FORMATTED'

    self.alluxio_master_format_marker = os.path.join(params.tmp_dir, tmp_alluxio_file)

  def format_cluster(self, env):
    import params
    #env.set_params(params)

    try:
      check_process_status(params.pid_dir + '/alluxio-master.pid')
      Logger.error('Format cluster not possible while Alluxio is running')
      raise Exception('Format cluster not possible while Alluxio is running')
    except ComponentIsNotRunning:
      Logger.info('Formatting the Alluxio master...')

      self.format_marker(env)

      # the following steps are needed to format correctly the journal of alluxio
      # 1-create as hdfs the journal folder
      folders = params.journal_relative_path.split('/')[1:]
      if params.security_enabled:
        Execute(params.kinit_path_local+' -kt '+params.hdfs_user_keytab+' '+params.hdfs_principal_name, user = 'hdfs')

      Execute('hdfs dfs -mkdir -p /' + folders[0], user='hdfs', not_if ='hdfs dfs -ls /' + folders[0])
      Execute('hdfs dfs -mkdir -p ' + params.journal_relative_path, user='hdfs', not_if ='hdfs dfs -ls ' + params.journal_relative_path)
      # 2-change owner to root
      Execute('hdfs dfs -chown -R ' + params.root_user + ':' + params.user_group + ' /' + folders[0], user='hdfs')
      # 3-format the cluster as root
      Execute(params.base_dir + '/bin/alluxio format', user='hdfs')
      # 4-change owner to alluxio
      Execute('hdfs dfs -chown -R ' + params.alluxio_user + ':' + params.user_group + ' /' + folders[0], user='hdfs')

      #update permissions on alluxio folder on hdfs
      Execute('hdfs dfs -chmod -R 775 /' + folders[0], user='hdfs')

      # create marker
      open(self.alluxio_master_format_marker, 'a').close()
      Logger.info('Alluxio master formatted')


if __name__ == "__main__":
  Master().execute()
