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
from resource_management.core.resources.system import File, Execute, Directory, PropertiesFile
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
          recursive=True
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

    configs = {}
    configs.update(params.config['configurations']['alluxio-site'])

    # Common properties
    configs["alluxio.logs.dir"] = params.log_dir
    configs["alluxio.underfs.address"] = params.underfs_addr
    configs["alluxio.underfs.hdfs.configuration"] = params.hadoop_core_site
    configs["alluxio.zookeeper.enabled"] = "true"
    configs["alluxio.zookeeper.address"] = params.zookeeper_hosts

    # Master properties
    configs["alluxio.master.hostname"] = params.alluxio_master
    configs["alluxio.master.journal.folder"] = params.journal_addr

    # Worker properties
    configs["alluxio.worker.memory"] = params.worker_mem
    configs["alluxio.worker.tieredstore.level0.dirs.quota"] = params.tieredstore_level0_dirs_quota
    configs["alluxio.worker.tieredstore.level1.dirs.quota"] = params.tieredstore_level1_dirs_quota
    if params.is_tiredstore_level2_enabled:
        configs["alluxio.worker.tieredstore.levels"] = 3
        configs["alluxio.worker.tieredstore.level2.alias"] = params.tieredstore_level2_alias
        configs["alluxio.worker.tieredstore.level2.dirs.path"] = params.tieredstore_level2_dirs_path
        configs["alluxio.worker.tieredstore.level2.dirs.quota"] = params.tieredstore_level2_dirs_quota
        configs["alluxio.worker.tieredstore.level2.reserved.ratio"] = params.tieredstore_level2_reserved_ratio
    else:
        configs["alluxio.worker.tieredstore.levels"] = 2

    # User properties
    configs["alluxio.user.block.size.bytes.default"] = params.block_size_default

    PropertiesFile("alluxio-site.properties.tmp",
                   dir=params.alluxio_config_dir,
                   properties=configs,
                   owner=params.alluxio_user,
                   group=params.user_group,
                   mode=0644,
                   key_value_delimiter="="
                   )

    Execute('ln -s /var/log/alluxio/ /usr/lib/alluxio/logs', not_if="test -d /usr/lib/alluxio/logs")

    Directory(params.tieredstore_level1_dirs_path,
              owner=params.alluxio_user,
              group=params.user_group,
              recursive=True
              )

    if params.is_tiredstore_level2_enabled:
        Directory(params.tieredstore_level2_dirs_path,
                  owner=params.alluxio_user,
                  group=params.user_group,
                  recursive=True
                  )

    # update permissions on alluxio-env.sh file
    Execute('chmod u=rw,g=rx,o=r ' + params.alluxio_config_dir + '/alluxio-env.sh', user=params.root_user)