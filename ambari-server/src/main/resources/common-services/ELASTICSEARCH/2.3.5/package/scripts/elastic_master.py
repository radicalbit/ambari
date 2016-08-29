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
import signal
import sys
import os
from os.path import isfile

from elastic_component import ElasticComponent

class ElasticMaster(ElasticComponent):

    def configure(self, env):
        import params
        env.set_params(params)
        # TODO: check if those plugins are required and use a configuration
        # TODO: change proxy port, it's already used by ambari web server
        # output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install mobz/elasticsearch-head")
        # print output
        # output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install elasticsearch/elasticsearch-repository-hdfs/2.1.0-hadoop2")
        # print output
        # output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install royrusso/elasticsearch-HQ")
        # print output
        self.base_config(env)

if __name__ == "__main__":
    ElasticMaster().execute()

# from elastic import elastic
#
#
# class Elasticsearch(Script):
#     def install(self, env):
#         import params
#         env.set_params(params)
#         print 'Install the Master'
#         self.install_packages(env)
#     def configure(self, env):
#         import params
#         env.set_params(params)
#         print 'Install plugins';
#         # TODO: check if those plugins are required and use a configuration
#         output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install mobz/elasticsearch-head")
#         print output
#         output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install elasticsearch/elasticsearch-repository-hdfs/2.1.0-hadoop2")
#         print output
#         output = os.system("/usr/share/elasticsearch/bin/plugin -DproxyHost=proxy.ash2.symcpe.net -DproxyPort=8080 --install royrusso/elasticsearch-HQ")
#         print output
#         elastic()
#     def stop(self, env):
#         import params
#         env.set_params(params)
#         stop_cmd = format("service elasticsearch stop")
#         Execute(stop_cmd)
#         print 'Stop the Master'
#     def start(self, env):
#         import params
#         env.set_params(params)
#         self.configure(env)
#         start_cmd = format("service elasticsearch start")
#         Execute(start_cmd)
#         print 'Start the Master'
#     def status(self, env):
#         import params
#         env.set_params(params)
#         status_cmd = format("service elasticsearch status")
#         Execute(status_cmd)
#         print 'Status of the Master'
# if __name__ == "__main__":
#     Elasticsearch().execute()


