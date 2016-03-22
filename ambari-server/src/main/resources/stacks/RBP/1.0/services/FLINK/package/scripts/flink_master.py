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
import sys, os, pwd, grp, signal, time, glob, hashlib
from resource_management import *
from subprocess import call

class FlinkMaster(Script):
  def install(self, env):

    import params
    import status_params

    self.install_packages(env)

    Directory([status_params.flink_pid_dir, params.flink_log_dir],
              owner=params.flink_user,
              group=params.user_group,
              recursive=True
    )

    # Everyone can read and write
    File(params.flink_log_file,
            mode=0666,
            owner=params.flink_user,
            group=params.user_group,
            content=''
    )

    self.configure(env, True)

  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    #write out nifi.properties
    #properties_content=InlineTemplate(params.flink_yaml_content)
    #File(format("{conf_dir}/flink-conf.yaml"), content=properties_content, owner=params.flink_user)

    File(
        format("{conf_dir}/flink-conf.yaml"),
        owner=params.flink_user,
        mode=0644,
        content=Template('flink-conf.yaml.j2', conf_dir=params.conf_dir)
    )

    # File(
    #     format("{conf_dir}/core-site.xml"),
    #     owner=params.flink_user,
    #     mode=0644,
    #     content=Template('core-site.xml', conf_dir=params.conf_dir)
    # )

    Execute(format("scp {alluxio_master}:/etc/alluxio/alluxio-site.properties /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(format("zip /tmp/alluxio-site.jar /tmp/alluxio-site.properties"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    Execute(format("cp /tmp/alluxio-site.jar {flink_lib}"),
        tries = 10,
        try_sleep=3,
        logoutput=True
    )
    
  def stop(self, env):
    import params
    import status_params
    cmd = format('pkill -f {params.flink_appname} & pkill -f org.apache.flink.yarn.ApplicationMaster')
    Execute (cmd, ignore_failures=True)
    Execute ('rm -f ' + status_params.flink_pid_file, ignore_failures=True)
 
      
  def start(self, env):
    import params
    import status_params
    self.configure(env)
    
    self.create_hdfs_user(params.flink_user)

    Execute('echo bin dir ' + params.bin_dir)        
    Execute('echo pid file ' + status_params.flink_pid_file)
    cmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; nohup {bin_dir}/yarn-session.sh -n {flink_numcontainers} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")

    if params.flink_streaming:
      cmd = cmd + ' -st '
    Execute (cmd + format(" >> {flink_log_file} & echo $! > {status_params.flink_pid_file}"), user=params.flink_user)

    #Execute("ps -ef | grep org.apache.flink.yarn.ApplicationMaster | awk {'print $2'} | head -n 1 > " + status_params.flink_pid_file, user=params.flink_user)


  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.flink_pid_file)


  def create_hdfs_user(self, user):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
          
if __name__ == "__main__":
  FlinkMaster().execute()
