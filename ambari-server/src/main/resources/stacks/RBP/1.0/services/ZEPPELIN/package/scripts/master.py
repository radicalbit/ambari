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

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def install(self, env):

    import params
    env.set_params(params)
    self.install_packages(env)
    self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)

    Directory([params.zeppelin_pid_dir, params.zeppelin_log_dir],
            owner=params.zeppelin_user,
            group=params.user_group,
            recursive=True
    )

  def create_hdfs_user(self, user, spark_jar_dir):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')

    Execute('hadoop fs -mkdir -p '+spark_jar_dir, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' ' + spark_jar_dir, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' ' + spark_jar_dir, user='hdfs')

  def configure(self, env):
    import params
    env.set_params(params)

    # #write out zeppelin-site.xml
    # XmlConfig("zeppelin-site.xml",
    #         conf_dir = params.conf_dir,
    #         configurations = params.config['configurations']['zeppelin-config'],
    #         owner=params.zeppelin_user,
    #         group=params.user_group
    # )
    # #write out zeppelin-env.sh
    # env_content=InlineTemplate(params.zeppelin_env_content)
    # File(format("{params.conf_dir}/zeppelin-env.sh"), content=env_content, owner=params.zeppelin_user, group=params.user_group) # , mode=0777)


  def stop(self, env):
    import params
    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh stop', user=params.zeppelin_user)


  def start(self, env):
    import params
    self.configure(env)

    # first_setup=False
    #
    # if glob.glob('/tmp/zeppelin-spark-dependencies-*.jar') and os.path.exists(glob.glob('/tmp/zeppelin-spark-dependencies-*.jar')[0]):
    #     first_setup=True
    #     self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)
    #     Execute ('hadoop fs -put /tmp/zeppelin-spark-dependencies-*.jar ' + params.spark_jar, user=params.zeppelin_user, ignore_failures=True)
    #     Execute ('rm /tmp/zeppelin-spark-dependencies-*.jar')

    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh start', user=params.zeppelin_user)

  def status(self, env):
    import status_params
    env.set_params(status_params)
    pid_file = glob.glob(status_params.zeppelin_pid_dir + '/zeppelin.pid')[0]
    check_process_status(pid_file)


if __name__ == "__main__":
  Master().execute()
