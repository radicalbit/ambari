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

Ambari Agent

"""
import os
from resource_management import *

def flink():
  import params

  if not os.path.exists(format('{flink_install_dir}/ship/')):
    Directory([format('{flink_install_dir}/ship')],
        owner='root',
        group='root',
        recursive=True
    )

  alluxio_jar_name = 'alluxio-core-client-1.0.0-jar-with-dependencies.jar'

  # if not os.path.exists(format('{flink_install_dir}/lib/{alluxio_jar_name}')):
  #   download_alluxio_client_jar(alluxio_jar_name)
  #   Execute(format('cp /tmp/{alluxio_jar_name} {flink_install_dir}/lib/'), user='root')

  if not os.path.exists(format('{flink_install_dir}/ship/{alluxio_jar_name}')):
    download_alluxio_client_jar(alluxio_jar_name)
    Execute(format('cp /tmp/{alluxio_jar_name} {flink_install_dir}/ship/'), user='root')

  Directory([params.flink_log_dir],
      owner=params.flink_user,
      group=params.user_group,
      recursive=True
  )

  File(params.flink_log_file,
      mode=0644,
      owner=params.flink_user,
      group=params.user_group,
      content=''
  )

  File(
      format("{params.conf_dir}/flink-conf.yaml"),
      owner=params.flink_user,
      mode=0644,
      content=Template('flink-conf.yaml.j2', conf_dir=params.conf_dir)
  )

  if not is_empty(params.log4j_props):
    File(format("{params.conf_dir}/log4j.properties"),
         mode=0644,
         group=params.user_group,
         owner=params.flink_user,
         content=params.log4j_props
         )
  elif (os.path.exists(format("{params.conf_dir}/log4j.properties"))):
    File(format("{params.conf_dir}/log4j.properties"),
         mode=0644,
         group=params.user_group,
         owner=params.flink_user
         )

def download_alluxio_client_jar(jar_name):
  jar_url = 'http://public-repo.readicalbit.io/jars'

  if not os.path.exists(format('/tmp/{jar_name}')):
    Execute(
        format('wget {jar_url}/{jar_name} -O /tmp/{jar_name} -a /tmp/alluxio_download.log'),
        user='root'
    )