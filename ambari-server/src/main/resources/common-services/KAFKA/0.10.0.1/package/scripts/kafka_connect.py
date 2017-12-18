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
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.resources.properties_file import PropertiesFile
from resource_management.core.resources.system import Execute, File
from kafka import mutable_config_dict

class KafkaConnect(Script):

    def install(self, env):
        self.install_packages(env)

    def configure(self, env):
        import params
        env.set_params(params)
        kafka_connect_config = mutable_config_dict(params.config['configurations']['kafka-connect'])
        kafka_connect_config['rest.host.name'] = params.hostname
        kafka_connect_config['bootstrap.servers'] = params.bootstrap_servers
        PropertiesFile(
            "connect-distributed.properties",
            dir=params.conf_dir,
            properties=kafka_connect_config,
            owner=params.kafka_user,
            group=params.user_group,
        )

        if (params.kafka_connect_log4j_props != None):
            File(format("{conf_dir}/connect-log4j.properties"),
                 mode=0644,
                 group=params.user_group,
                 owner=params.kafka_user,
                 content=params.kafka_connect_log4j_props
                 )

    def start(self, env):
        import params
        env.set_params(params)
        Execute(
            format('export KAFKA_HEAP_OPTS="-Xmx{params.kafka_connect_heapsize}G";{params.kafka_home}/bin/connect-distributed.sh {params.conf_dir}/connect-distributed.properties '
                   '>{params.kafka_log_dir}/kafka-connect.log '
                   '2>{params.kafka_log_dir}/kafka-connect.out & '
                   'echo $! > {params.kafka_connect_pid_file}'),
            user=params.kafka_user
        )
        self.configure(env)

    def stop(self, env):
        import params
        env.set_params(params)
        Execute(format('kill `cat {params.kafka_connect_pid_file}`'), user=params.kafka_user)
        Execute(format('rm -f {params.kafka_connect_pid_file}'), user=params.kafka_user)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.kafka_connect_pid_file)

if __name__ == "__main__":
    KafkaConnect().execute()