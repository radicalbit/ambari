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
import sys, os, pwd, grp, signal, time, glob
from resource_management import *
from subprocess import call

class Master(Script):
  def install(self, env):

    import params
    import status_params
            
    Directory([params.flink_install_dir],
            owner=params.flink_user,
            group=params.flink_group,
            recursive=True
    )

    Directory([status_params.flink_pid_dir, params.flink_log_dir],
              owner=params.flink_user,
              group=params.flink_group,
              recursive=True
    )

    File(params.flink_log_file,
            mode=0644,
            owner=params.flink_user,
            group=params.flink_group,
            content=''
    )

    Execute('echo Installing packages')

    Execute(
        '/bin/tar -zxvf ' + params.flink_archive_file + ' --strip 1 -C ' + params.flink_install_dir,
        user=params.flink_user
    )
    self.configure(env, True)

  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)
    
    self.set_conf_bin(env)
        
    #write out nifi.properties
    properties_content=InlineTemplate(params.flink_yaml_content)
    File(format("{conf_dir}/flink-conf.yaml"), content=properties_content, owner=params.flink_user)
        
    
  def stop(self, env):
    import params
    import status_params    
    Execute ('pkill -f org.apache.flink.yarn.ApplicationMaster', ignore_failures=True)
    Execute ('rm -f ' + status_params.flink_pid_file, ignore_failures=True)
 
      
  def start(self, env):
    import params
    import status_params
    self.set_conf_bin(env)  
    self.configure(env) 
    
    self.create_hdfs_user(params.flink_user)

    Execute('echo bin dir ' + params.bin_dir)        
    Execute('echo pid file ' + status_params.flink_pid_file)
    cmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/yarn-session.sh -n {flink_numcontainers} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")
    if params.flink_streaming:
      cmd = cmd + ' -st '
    Execute (cmd + format(" >> {flink_log_file}"), user=params.flink_user)

    Execute("ps -ef | grep org.apache.flink.yarn.ApplicationMaster | awk {'print $2'} | head -n 1 > " + status_params.flink_pid_file, user=params.flink_user)

    if os.path.exists(params.temp_file):
      os.remove(params.temp_file)
    
  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.flink_pid_file)


  def set_conf_bin(self, env):
    import params
    params.conf_dir =  params.flink_install_dir+ '/conf'
    params.bin_dir =  params.flink_install_dir+ '/bin'

  def create_hdfs_user(self, user):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
          
if __name__ == "__main__":
  Master().execute()
