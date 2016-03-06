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
from alluxio import Alluxio

class Master(Alluxio):

  def install(self, env):
    import params
    self.base_install(env)
    self.configure(env)

    Execute(params.base_dir + '/bin/alluxio format', user=params.alluxio_user)

  def start(self, env):
    import params
    self.configure(env)
    env.set_params(params)

    Execute(params.base_dir + '/bin/alluxio-start.sh master', user=params.alluxio_user)

    cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioMaster | awk '{print $1}'`> " + params.pid_dir + "/alluxio-master.pid"
    Execute(cmd, user=params.alluxio_user)

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
