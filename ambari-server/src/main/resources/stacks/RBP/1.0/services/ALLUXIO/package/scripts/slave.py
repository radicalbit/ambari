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
from resource_management import *
from resource_management.core.logger import Logger
from alluxio import Alluxio

class Slave(Alluxio):

  def install(self, env):
    import params
    Logger.info('Installing the Alluxio worker...')
    self.base_install(env)
    self.configure(env)
    Logger.info('Alluxio worker installed')

  def start(self, env):
    import params
    self.configure(env)
    env.set_params(params)

    self.alluxio_worker_format_marker = os.path.join(params.pid_dir, 'ALLUXIO_WORKER_FORMATTED')
    if not os.path.exists(self.alluxio_worker_format_marker):
      Logger.info('Formatting the Alluxio worker...')
      Execute(params.base_dir + '/bin/alluxio formatWorker', user=params.root_user)

      # update permissions on log folder
      Execute('chown -R ' + params.alluxio_user + ':' + params.user_group + ' ' + params.log_dir, user=params.root_user)

      # create marker
      open(self.alluxio_worker_format_marker, 'a').close()
      Logger.info('Alluxio worker formatted.')

    Logger.info('Starting Alluxio worker...')
    Execute(params.base_dir + '/bin/alluxio-start.sh worker SudoMount', user=params.alluxio_user)
    Logger.info('Alluxio worker started...')

    Logger.info('Creating pid file for Alluxio worker...')
    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioWorker | awk '{print $1}'`> " + params.pid_dir + "/alluxio-worker.pid"
    Execute(cmd, user=params.alluxio_user)
    Logger.info('Pid file created for Alluxio worker.')

  def stop(self, env):
    import params
    Execute(params.base_dir + '/bin/alluxio-stop.sh ' + 'worker', user=params.alluxio_user)

  def status(self, env):
    import status_params as params
    env.set_params(params)
    pid_file = format("{pid_dir}/alluxio-worker.pid")
    check_process_status(pid_file)


if __name__ == "__main__":
  Slave().execute()
