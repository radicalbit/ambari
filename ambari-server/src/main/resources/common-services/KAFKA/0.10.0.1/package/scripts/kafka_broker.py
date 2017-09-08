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
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.check_process_status import check_process_status
import os, time
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
    while is_kafka_logs_locked():
      time.sleep(2)
    self.configure(env, upgrade_type=upgrade_type)
    daemon_cmd = format('{params.kafka_home}/bin/kafka-server-start.sh {params.conf_dir}/server.properties >/dev/null & echo $! > {params.kafka_pid_file}')
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
    #time.sleep(2)


  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.kafka_pid_file)

  def is_kafka_logs_locked(self):
    import params
    kafka_logs_dir = params.config['configurations']['kafka-broker']['log.dirs']
    locked = False
    if os.path.exists(kafka_logs_dir):
      for filename in os.listdir(kafka_logs_dir):
        file_object = None
        try:
          print "Trying to open %s." % filename
          buffer_size = 0
          file_object = open(filename, 'a', buffer_size)
          if file_object:
            print "%s is not locked." % filename
            pass
        except IOError, message:
          print "File is locked (unable to open in append mode). %s." % \
                message
          locked = True
        finally:
          if file_object:
            file_object.close()
            print "%s closed." % filename

      return locked
    else:
      return locked

if __name__ == "__main__":
  KafkaBroker().execute()
