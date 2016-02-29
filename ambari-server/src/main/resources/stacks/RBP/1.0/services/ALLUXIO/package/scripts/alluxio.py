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


class Alluxio(Script):

  def base_install(self, env):
    import params

    self.install_packages(env)

    if not os.path.isfile('/etc/sudoers.pre_alluxio.bak'):
      Execute('cp /etc/sudoers /etc/sudoers.pre_alluxio.bak')
      Execute('echo "'+params.alluxio_user+'    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers')

    if not os.path.exists(params.pid_dir):
      Directory(
          [params.pid_dir],
          owner=params.alluxio_user,
          group=params.user_group,
          recursive=True
      )

  def configure(self, env):
    import params

    env.set_params(params)

    alluxio_config_dir = '/etc/alluxio/conf'
    alluxio_libexec_dir = params.base_dir + '/libexec'

    File(
        format("{alluxio_config_dir}/alluxio-env.sh"),
        owner=params.alluxio_user,
        mode=0700,
        content=Template('alluxio-env.sh.j2', conf_dir=alluxio_config_dir)
    )

    File(
        format("{alluxio_config_dir}/workers"),
        owner=params.alluxio_user,
        mode=0644,
        content='\n'.join(params.alluxio_workers)
    )

    File(
        format("{alluxio_libexec_dir}/alluxio-config.sh"),
        owner=params.alluxio_user,
        mode=0700,
        content=Template('alluxio-config.sh.j2', conf_dir=alluxio_libexec_dir)
    )