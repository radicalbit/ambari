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

def elasticsearch():
    import params

    params.path_data = params.path_data.replace('"','')
    data_path = params.path_data.replace(' ','').split(',')
    data_path[:]=[x.replace('"','') for x in data_path]
    
    directories = [params.log_dir, params.pid_dir, params.conf_dir, params.plugins_dir]
    directories = directories+data_path;

    Directory(directories,
              owner=params.elastic_user,
              group=params.user_group,
              create_parents=True
          )
    
    File(format("{conf_dir}/elastic-env.sh"),
          owner=params.elastic_user,
          content=InlineTemplate(params.elastic_env_sh_template)
     )

    configurations = params.config['configurations']['elastic-site']

    File(format("{conf_dir}/elasticsearch.yml"),
       content=Template(
                        "elasticsearch.yaml.j2",
                        configurations = configurations),
       owner=params.elastic_user,
       group=params.user_group
    )

    # TODO: understand if possible in other way
    # File(format("/etc/sysconfig/elasticsearch"),
    #    content=Template(
    #                     "elasticsearch.sysconfig.j2",
    #                     configurations = configurations),
    #    owner="root",
    #    group="root"
    # )