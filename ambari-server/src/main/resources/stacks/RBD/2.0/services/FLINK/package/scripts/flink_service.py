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
import os
from resource_management import *
from flink import flink

class FlinkService(Script):

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)
    flink('install')

  def configure(self, env):
    import params
    env.set_params(params)
    flink('configure')


  def start_krb_session(self, env):
    import params
    env.set_params(params)
    Execute(format("{kinit_path_local} {flink_jaas_principal} -kt {flink_keytab}"), user=params.flink_user)
    Execute(format("crontab {conf_dir}/cron-kinit-flink.sh"), user=params.flink_user)

  def stop_krb_session(self, env):
    import params
    if not os.path.isfile(format("{params.flink_pid_dir}/flink-{params.flink_user}-jobmanager.pid")) and \
        not os.path.isfile(format("{params.flink_pid_dir}/flink-{params.flink_user}-taskmanager.pid")):
      Execute(params.kdestroy_path_local, user=params.flink_user)
      Execute("crontab -r", user=params.flink_user)
