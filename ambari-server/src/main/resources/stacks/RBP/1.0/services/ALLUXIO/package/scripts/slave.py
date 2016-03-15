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
from alluxio import Alluxio

class Slave(Alluxio):

  def __init__(self):
    self.first_start = False

  def install(self, env):
    import params
    self.base_install(env)
    self.configure(env)
    self.first_start = True

  def start(self, env):
    import params
    self.configure(env)
    env.set_params(params)

    if self.first_start:
      print 'Formatting the worker...\n'
      Execute(params.base_dir + '/bin/alluxio formatWorker', user=params.root_user)
      print 'Worker formatted.\n'

    print 'Starting worker...\n'
    Execute(params.base_dir + '/bin/alluxio-start.sh worker SudoMount', user=params.alluxio_user)
    print 'Worker started...\n'

    print 'Creating pid file for worker...\n'
    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioWorker | awk '{print $1}'`> " + params.pid_dir + "/alluxio-worker.pid"
    Execute(cmd, user=params.alluxio_user)
    print 'Pid file created for worker.\n'

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
