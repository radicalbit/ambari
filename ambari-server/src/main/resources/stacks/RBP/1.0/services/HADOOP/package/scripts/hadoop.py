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

class Hadoop(Script):

  def base_install(self, env):
    import params

    self.install_packages(env)

    if not os.path.exists(params.hadoop_base_dir):

      if not os.path.exists(params.hadoop_tmp_file):
        Execute(
            'wget '+params.hadoop_download_link+' -O '+params.hadoop_tmp_file+' -a /tmp/hadoop_download.log',
            user=params.hdfs_user
        )
      else:
        hadoop_tmp_file_md5 = hashlib.md5(open(params.hadoop_tmp_file, "rb").read()).hexdigest()

        if not hadoop_tmp_file_md5 == params.binary_file_md5:
          Execute(
              'rm -f '+params.hadoop_tmp_file,
              user=params.hdfs_user
          )

          Execute(
              'wget '+params.hadoop_download_link+' -O '+params.hadoop_tmp_file+' -a /tmp/hadoop_download.log',
              user=params.hdfs_user
          )


      Directory(
          [params.hadoop_base_dir, params.hadoop_tmp_dir, params.hadoop_pid_dir, params.dfs_datanode_dir, params.dfs_namenode_dir],
          owner=params.hdfs_user,
          group=params.user_group,
          recursive=True
      )

      Execute(
          '/bin/tar -zxf ' + params.hadoop_tmp_file + ' --strip 1 -C ' + params.hadoop_base_dir,
          user=params.hdfs_user
      )

      # Set HADOOP_HOME
      Execute(
          "echo 'export HADOOP_HOME=" + params.hadoop_base_dir + "' >>/home/" + params.hdfs_user + "/.bash_profile",
          user=params.hdfs_user
      )
      # Set JAVA_HOME
      Execute(
          "echo 'export JAVA_HOME=" + params.java_home + "' >>/home/" + params.hdfs_user + "/.bash_profile",
          user=params.hdfs_user
      )
      # Add Hadoop bin and sbin directory to PATH
      Execute(
          "echo 'export PATH=$PATH:$HADOOP_HOME/bin/;$HADOOP_HOME/sbin/' >>/home/" + params.hdfs_user + "/.bash_profile",
          user=params.hdfs_user
      )

  def base_configure(self, env):
    import params
    env.set_params(params)

    File(
        format("{hadoop_conf_dir}/hadoop-env.sh"),
        owner=params.hdfs_user,
        mode=0700,
        content=Template('hadoop-env.sh.j2', conf_dir=params.hadoop_conf_dir)
    )

    File(
        format("{hadoop_conf_dir}/core-site.xml"),
        owner=params.hdfs_user,
        mode=0644,
        content=Template('core-site.xml.j2', conf_dir=params.hadoop_conf_dir)
    )

    File(
        format("{hadoop_conf_dir}/hdfs-site.xml"),
        owner=params.hdfs_user,
        mode=0644,
        content=Template('hdfs-site.xml.j2', conf_dir=params.hadoop_conf_dir)
    )

    File(
        format("{hadoop_conf_dir}/yarn-site.xml"),
        owner=params.hdfs_user,
        mode=0644,
        content=Template('yarn-site.xml.j2', conf_dir=params.hadoop_conf_dir)
    )