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
#!/usr/bin/env python

from __future__ import print_function
from resource_management import *
import  sys,subprocess,os
import requests
import time

class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        print("Running Kibana service check", file=sys.stdout)
        time.sleep(20)
        r = requests.get(format('http://{params.hostname}:{params.server_port}/'))

        if r.status_code == 200:
            print("Kibana service is running", file=sys.stdout)
            sys.exit(0)
        else:
            print("Kibana service is not running", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    ServiceCheck().execute()
