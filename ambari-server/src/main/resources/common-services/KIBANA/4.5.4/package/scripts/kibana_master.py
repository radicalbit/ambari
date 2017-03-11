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

from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import File, Execute
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.core.logger import Logger

from kibana import kibana

class Kibana(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

    def configure(self, env):
        import params
        env.set_params(params)
        kibana()

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        Execute("/bin/kibana &", user=params.kibana_user)

    def stop(self, env):
        import params
        env.set_params(params)
        Execute('kill `cat ' + params.pid_file + '`', user=params.kibana_user)

    def status(self, env):
        import status_params as params
        env.set_params(params)
        pid_file = format("{pid_dir}/kibana.pid")
        check_process_status(pid_file)

if __name__ == "__main__":
    Kibana().execute()