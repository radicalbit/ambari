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
import os, hashlib
from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import File, Execute, Directory
from resource_management.core.logger import Logger

class Alluxio(Script):

  def base_install(self, env):
    import params

    self.install_packages(env)

    Logger.info('Checking sudoers...')
    if not os.path.isfile('/etc/sudoers.pre_alluxio.bak'):
      Execute('cp /etc/sudoers /etc/sudoers.pre_alluxio.bak')
      Execute('echo "'+params.alluxio_user+'    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers')

  def configure(self, env):
    import params

    env.set_params(params)

    Logger.info('Creating Alluxio pid dir...')
    if not os.path.exists(params.pid_dir):
      Directory(
          [params.pid_dir],
          owner=params.root_user,
          group=params.user_group,
          create_parents=True
      )
      Logger.info('Created Alluxio pid dir ' + params.pid_dir)

    if not os.path.isfile(params.alluxio_config_dir + '/hdfs-site.xml'):
      Execute('ln -s ' + params.hadoop_hdfs_site + ' ' + params.alluxio_config_dir + '/hdfs-site.xml')

    if not os.path.isfile(params.alluxio_config_dir + '/core-site.xml'):
      Execute('ln -s ' + params.hadoop_core_site + ' ' + params.alluxio_config_dir + '/core-site.xml')

    Execute('chown -R ' + params.alluxio_user + ':' + params.user_group + ' ' + params.pid_dir, user=params.root_user)
    Execute('chown -R ' + params.alluxio_user + ':' + params.user_group + ' ' + params.log_dir, user=params.root_user)
    Execute('chmod -R 777 ' + params.log_dir, user=params.root_user)

    File(
        format("{params.alluxio_config_dir}/workers"),
        owner=params.alluxio_user,
        mode=0644,
        content='\n'.join(params.alluxio_workers)
    )

    File(
        format("{params.alluxio_config_dir}/alluxio-site.properties"),
        owner=params.alluxio_user,
        mode=0644,
        content=Template('alluxio-site.properties.j2', conf_dir=params.alluxio_config_dir)
    )

    Execute('ln -s /var/log/alluxio/ /usr/lib/alluxio/logs', not_if="test -d /usr/lib/alluxio/logs")

    Directory(params.tieredstore_level1_dirs_path,
              owner=params.alluxio_user,
              group=params.user_group,
              create_parents=True
              )

    if params.is_tiredstore_level2_enabled:
        Directory(params.tieredstore_level2_dirs_path,
                  owner=params.alluxio_user,
                  group=params.user_group,
                  create_parents=True
                  )

    # update permissions on alluxio-env.sh file
    Execute('chmod u=rw,g=rx,o=r ' + params.alluxio_config_dir + '/alluxio-env.sh', user=params.root_user)