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
    if params.is_alluxio_installed:
        download_alluxio_client_jar(params.jar_url, params.alluxio_jar_name)
        Execute(
            format('cp /tmp/{alluxio_jar_name} {params.flink_lib}/'),
            user='root',
            not_if=format('test -d /{params.flink_lib}/{alluxio_jar_name}')
        )

  elif action == 'configure':

    # configure folders and permissions

    Directory(
        [params.flink_log_dir,params.flink_pid_dir],
        owner=params.flink_user,
        group=params.user_group,
        recursive=True
    )

    Execute('chmod 777 ' + params.flink_log_dir, user='root')

    # add flink node configurations

    configs = {}
    configs.update(params.config['configurations']['flink-conf'])
    configs["jobmanager.rpc.address"] = params.flink_jobmanager
    configs["recovery.zookeeper.quorum"] = params.zookeeper_quorum
    configs["recovery.zookeeper.path.root"] = params.recovery_zookeeper_path_root
    configs["recovery.zookeeper.storageDir"] = params.recovery_zookeeper_storage_dir
    configs["fs.hdfs.hadoopconf"] = params.hadoop_conf_dir
    configs["state.backend.fs.checkpointdir"] = params.state_backend_checkpointdir
    configs["env.log.dir"] = params.flink_log_dir
    configs["env.pid.dir"] = params.flink_pid_dir

    if params.security_enabled:
        configs["krb5.conf.path"] = params.krb5_conf_path
        configs["krb5.jaas.path"] = params.flink_client_jass_path

    PropertiesFile("flink-conf.yaml",
        dir=params.conf_dir,
        properties=configs,
        owner=params.flink_user,
        group=params.user_group,
        mode=0644,
        key_value_delimiter=": "
    )

    File(
        format("{conf_dir}/slaves"),
        owner=params.flink_user,
        group=params.user_group,
        mode=0644,
        content=Template('slaves.j2', conf_dir=params.conf_dir)
    )

    File(
        format("{conf_dir}/masters"),
        owner=params.flink_user,
        group=params.user_group,
        mode=0644,
        content=Template('masters.j2', conf_dir=params.conf_dir)
    )

    # add security configurations if required

    if params.security_enabled:
      File(
          format("{conf_dir}/flink_client_jaas.conf"),
          owner=params.flink_user,
          group=params.user_group,
          mode=0644,
          content=Template('flink_client_jaas.conf.j2', conf_dir=params.conf_dir)
      )
      File(
          format("{conf_dir}/cron-kinit-flink.sh"),
          owner=params.flink_user,
          group=params.user_group,
          mode=0700,
          content=Template('cron-kinit-flink.sh.j2', conf_dir=params.conf_dir)
      )

    if params.is_alluxio_installed:
        # Create and add alluxio-site.properties jar
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