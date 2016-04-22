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
import glob
from resource_management import *

class Zeppelin(Script):

  def install(self, env):
    import params
    self.install_packages(env)
    #self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)

    Directory([params.zeppelin_pid_dir, params.zeppelin_log_dir],
            owner=params.zeppelin_user,
            group=params.user_group,
            recursive=True
    )

  # def create_hdfs_user(self, user, spark_jar_dir):
  #   Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
  #   Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
  #   Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
  #
  #   Execute('hadoop fs -mkdir -p '+spark_jar_dir, user='hdfs', ignore_failures=True)
  #   Execute('hadoop fs -chown ' + user + ' ' + spark_jar_dir, user='hdfs')
  #   Execute('hadoop fs -chgrp ' + user + ' ' + spark_jar_dir, user='hdfs')

  def configure(self, env):
    import params
    env.set_params(params)

    Execute(format('chown -R {zeppelin_user}:{user_group} {zeppelin_dir}'), user='root')

    Directory(
        [params.zeppelin_log_dir, params.zeppelin_pid_dir],
        owner=params.zeppelin_user,
        group=params.user_group,
        recursive=True
    )

    #write out zeppelin-site.xml
    XmlConfig("zeppelin-site.xml",
            conf_dir = params.conf_dir,
            configurations = params.config['configurations']['zeppelin-site'],
            owner=params.zeppelin_user,
            group=params.user_group
    )
    #write out zeppelin-env.sh
    File(
        format("{params.conf_dir}/zeppelin-env.sh"),
        owner=params.zeppelin_user,
        mode=0775,
        content=Template('zeppelin-env.sh.j2', conf_dir=params.conf_dir)
    )
    #write out interpreter.json
    File(
        format("{params.conf_dir}/interpreter.json"),
        owner=params.zeppelin_user,
        mode=0755,
        content=Template('interpreter.json', conf_dir=params.conf_dir)
    )
    # env_content=InlineTemplate(params.zeppelin_env_content)
    # File(format("{params.conf_dir}/zeppelin-env.sh"), content=env_content, owner=params.zeppelin_user, group=params.user_group) # , mode=0777)


  def stop(self, env):
    import params
    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh stop', user=params.zeppelin_user)


  def start(self, env):
    import params
    self.configure(env)

    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh start', user=params.zeppelin_user)
    Execute(
        "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep org.apache.zeppelin.server.ZeppelinServer | awk '{print $1}'`> " + params.zeppelin_pid_dir + "/zeppelin.pid",
        user=params.zeppelin_user
    )

  def status(self, env):
    import status_params
    env.set_params(status_params)
    pid_file = glob.glob(status_params.zeppelin_pid_dir + '/zeppelin.pid')[0]
    check_process_status(pid_file)


if __name__ == "__main__":
  Zeppelin().execute()
