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
from resource_management import Script
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import hdp_select
from resource_management.libraries.functions import Direction
from resource_management.libraries.functions.version import compare_versions, format_hdp_stack_version
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.check_process_status import check_process_status
from kafka import ensure_base_directories

from kafka import kafka

class KafkaBroker(Script):

  def get_stack_to_component(self):
    return {"HDP": "kafka-broker"}

  def install(self, env):
    self.install_packages(env)

  def configure(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    kafka(upgrade_type=upgrade_type)

  def start(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    self.configure(env, upgrade_type=upgrade_type)
    daemon_cmd = format('nohup {params.kafka_home}/bin/kafka-server-start.sh {params.conf_dir}/server.properties >>{params.conf_dir}/kafka.out 2>>{params.conf_dir}/kafka.err & echo $! > {params.kafka_pid_file}')
    no_op_test = format('ls {params.kafka_pid_file} >/dev/null 2>&1 && ps -p `cat {params.kafka_pid_file}` >/dev/null 2>&1')
    Execute(daemon_cmd,
            user=params.kafka_user,
            not_if=no_op_test
    )

  def stop(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    # Kafka package scripts change permissions on folders, so we have to
    # restore permissions after installing repo version bits
    # before attempting to stop Kafka Broker
    ensure_base_directories()
    daemon_cmd = format('nohup {params.kafka_home}/bin/kafka-server-stop.sh')
    #daemon_cmd = format('source {params.conf_dir}/kafka-env.sh; nohup {params.kafka_bin} stop')
    Execute(daemon_cmd,
            user=params.kafka_user,
    )
    File(params.kafka_pid_file,
          action = "delete"
    )


  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.kafka_pid_file)

if __name__ == "__main__":
  KafkaBroker().execute()
