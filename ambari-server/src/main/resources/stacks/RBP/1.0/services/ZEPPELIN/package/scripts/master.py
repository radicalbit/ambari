# encoding=utf8

import sys, os, pwd, grp, signal, time, glob
from resource_management import *
from subprocess import call

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def install(self, env):

    import params
    import status_params

    env.set_params(params)

    self.install_packages(env)

    #Create user and group if they don't exist
    self.create_linux_user(params.zeppelin_user, params.zeppelin_group)
    #self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)


    #create the log, pid, zeppelin dirs
    Directory([params.zeppelin_pid_dir, params.zeppelin_log_dir],
            owner=params.zeppelin_user,
            group=params.zeppelin_group,
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
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    #write out zeppelin-site.xml
    XmlConfig("zeppelin-site.xml",
            conf_dir = params.conf_dir,
            configurations = params.config['configurations']['zeppelin-config'],
            owner=params.zeppelin_user,
            group=params.zeppelin_group
    )
    #write out zeppelin-env.sh
    env_content=InlineTemplate(params.zeppelin_env_content)
    File(format("{params.conf_dir}/zeppelin-env.sh"), content=env_content, owner=params.zeppelin_user, group=params.zeppelin_group) # , mode=0777)


  def stop(self, env):
    import params
    import status_params
    #self.configure(env)
    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh stop >> ' + params.zeppelin_log_file, user=params.zeppelin_user)


  def start(self, env):
    import params
    import status_params
    self.configure(env)

    first_setup=False

    #cleanup temp dirs
    note_osx_dir=params.notebook_dir+'/__MACOSX'
    if os.path.exists(note_osx_dir):
      Execute('rm -rf ' + note_osx_dir)


    if glob.glob('/tmp/zeppelin-spark-dependencies-*.jar') and os.path.exists(glob.glob('/tmp/zeppelin-spark-dependencies-*.jar')[0]):
        first_setup=True
        self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)
        Execute ('hadoop fs -put /tmp/zeppelin-spark-dependencies-*.jar ' + params.spark_jar, user=params.zeppelin_user, ignore_failures=True)
        Execute ('rm /tmp/zeppelin-spark-dependencies-*.jar')

    Execute (params.zeppelin_dir+'/bin/zeppelin-daemon.sh start >> ' + params.zeppelin_log_file, user=params.zeppelin_user)
    pidfile=glob.glob(status_params.zeppelin_pid_dir + '/zeppelin-'+params.zeppelin_user+'*.pid')[0]
    Execute('echo pid file is: ' + pidfile, user=params.zeppelin_user)
    contents = open(pidfile).read()
    Execute('echo pid is ' + contents, user=params.zeppelin_user)

    #if first_setup:
    import time
    time.sleep(5)
    self.update_zeppelin_interpreter()

  def status(self, env):
    import status_params
    env.set_params(status_params)

    pid_file = glob.glob(status_params.zeppelin_pid_dir + '/zeppelin-'+status_params.zeppelin_user+'*.pid')[0]
    check_process_status(pid_file)

  def install_mvn_repo(self):
    #for centos/RHEL 6/7 maven repo needs to be installed
    distribution = platform.linux_distribution()[0].lower()
    if distribution.startswith('centos') or distribution.startswith('red hat') and not os.path.exists('/etc/yum.repos.d/epel-apache-maven.repo'):
      Execute('curl -o /etc/yum.repos.d/epel-apache-maven.repo https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo')

  def update_zeppelin_interpreter(self):
    import params
    import json,urllib,urllib2
    zeppelin_int_url='http://'+params.zeppelin_host+':'+str(params.zeppelin_port)+'/api/interpreter/setting/'

    #fetch current interpreter settings for spark, hive, phoenix
    data = json.load(urllib2.urlopen(zeppelin_int_url))
    print data
    for body in data['body']:
      if body['group'] == 'spark':
        sparkbody = body
      elif  body['group'] == 'hive':
        hivebody = body
      elif  body['group'] == 'phoenix':
        phoenixbody = body

    #if hive installed, update hive settings and post to hive interpreter
    if (params.hive_server_host):
      hivebody['properties']['hive.hiveserver2.url']='jdbc:hive2://'+params.hive_server_host+':10000'
      self.post_request(zeppelin_int_url + hivebody['id'], hivebody)

    #if hbase installed, update hbase settings and post to phoenix interpreter
    if (params.zookeeper_znode_parent and params.hbase_zookeeper_quorum):
      phoenixbody['properties']['phoenix.jdbc.url']='jdbc:phoenix:'+params.hbase_zookeeper_quorum+':'+params.zookeeper_znode_parent
      self.post_request(zeppelin_int_url + phoenixbody['id'], phoenixbody)

  def post_request(self, url, body):
    import json,urllib,urllib2
    encoded_body=json.dumps(body)
    req = urllib2.Request(str(url),encoded_body)
    req.get_method = lambda: 'PUT'
    try:
      response = urllib2.urlopen(req, encoded_body).read()
    except urllib2.HTTPError, error:
        print 'Exception: ' + error.read()

    jsonresp=json.loads(response.decode('utf-8'))
    print jsonresp['status']


if __name__ == "__main__":
  Master().execute()
