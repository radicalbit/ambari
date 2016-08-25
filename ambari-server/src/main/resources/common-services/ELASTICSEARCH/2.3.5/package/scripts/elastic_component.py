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
import signal
import sys
import os
from os.path import isfile

from elasticsearch  import elasticsearch

class ElasticComponent(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

    def base_config(self, env):
        import params
        env.set_params(params)
        elasticsearch()

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        # TODO: use script instead of service
        # TODO: set -Des.insecure.allow.root=???
        # start_cmd = format("service elasticsearch start")
        start_cmd = format(
            "bin/elasticsearch -d -p {params.pid_file} -Des.insecure.allow.root={params.es_insicure_allow_root}",
            user=params.elastic_user
        )
        Execute(start_cmd)

    def stop(self, env):
        import params
        env.set_params(params)
        # TODO: use script instead of service
        # stop_cmd = format("service elasticsearch stop")
        stop_cmd = Execute(format('kill `cat {params.pid_file}`'), user=params.elastic_user)
        Execute(stop_cmd)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        # TODO: use pid file instead of service
        # status_cmd = format("service elasticsearch status")
        # Execute(status_cmd)
        check_process_status(status_params.pid_file)
