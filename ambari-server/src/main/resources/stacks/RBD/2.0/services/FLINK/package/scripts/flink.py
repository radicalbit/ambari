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

def flink(action = None):
  import params

  if action == 'configure':

    # configure folders and permissions

    Directory(
        [params.flink_log_dir,params.flink_pid_dir],
        owner=params.flink_user,
        group=params.user_group,
        recursive=True
    )

    Execute('chmod 777 ' + params.flink_log_dir, user='root')

    # add flink node configurations

    File(
        format("{conf_dir}/flink-conf.yaml"),
        owner=params.flink_user,
        mode=0644,
        content=Template('flink-conf.yaml.j2', conf_dir=params.conf_dir)
    )
    File(
        format("{conf_dir}/slaves"),
        owner=params.flink_user,
        mode=0644,
        content=Template('slaves.j2', conf_dir=params.conf_dir)
    )
    File(
        format("{conf_dir}/masters"),
        owner=params.flink_user,
        mode=0644,
        content=Template('masters.j2', conf_dir=params.conf_dir)
    )

    # add security configurations if required

    if params.security_enabled:
      File(
          format("{conf_dir}/flink_client_jaas.conf"),
          owner=params.flink_user,
          mode=0644,
          content=Template('flink_client_jaas.conf.j2', conf_dir=params.conf_dir)
      )
      File(
          format("{conf_dir}/cron-kinit-flink.sh"),
          owner=params.flink_user,
          mode=0700,
          content=Template('cron-kinit-flink.sh.j2', conf_dir=params.conf_dir)
      )

  else:
    pass