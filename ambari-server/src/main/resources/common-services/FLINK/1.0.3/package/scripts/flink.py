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

  def download_alluxio_client_jar(jar_url, jar_name):
    Execute(
        format('wget {jar_url}/{jar_name} -O /tmp/{jar_name} -a /tmp/alluxio_download.log'),
        user='root',
        not_if=format('test -d /tmp/{jar_name}')
    )


  if action == 'install':
    download_alluxio_client_jar(params.jar_url, params.alluxio_jar_name)
    Execute(
        format('cp /tmp/{alluxio_jar_name} {params.flink_lib}/'),
        user='root',
        not_if=format('test -d /{params.flink_lib}/{alluxio_jar_name}')
    )

  elif action == 'configure':
    Directory(
        [params.flink_log_dir,params.flink_pid_dir],
        owner=params.flink_user,
        group=params.user_group,
        recursive=True
    )

    Execute('chmod 777 ' + params.flink_log_dir, user='root')

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
    File(
        format("{conf_dir}/kafka_client_jaas.properties"),
        owner=params.flink_user,
        mode=0644,
        content=Template('kafka_client_jaas.properties.j2', conf_dir=params.conf_dir)
    )

    Execute(
        format("scp -o StrictHostKeyChecking=no {alluxio_master}:/etc/alluxio/conf/alluxio-site.properties /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(
        format("zip -j /tmp/alluxio-site.jar /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(
        format("cp /tmp/alluxio-site.jar {params.flink_lib}"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )

  else:
    pass