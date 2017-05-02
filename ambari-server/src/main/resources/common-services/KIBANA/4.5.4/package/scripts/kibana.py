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

from resource_management import *
import sys
from copy import deepcopy

def kibana():
    import params

    Directory([params.log_dir, params.pid_dir],
              owner=params.kibana_user,
              group=params.user_group,
              recursive=True
          )

    Execute(format('chmod -R 777 {optimize_dir}'))

    configurations = params.config['configurations']['kibana-site']

    File(format("{conf_dir}/kibana.yml"),
       content=Template(
                        "kibana.yml.j2",
                        configurations = configurations),
       owner=params.kibana_user,
       group=params.user_group
    )
