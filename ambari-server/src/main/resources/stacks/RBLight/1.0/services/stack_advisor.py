#!/usr/bin/env ambari-python-wrap
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

import multiprocessing
from stack_advisor_023 import RBLight023StackAdvisor

class RBLight10StackAdvisor(RBLight023StackAdvisor):

  def getComponentLayoutValidations(self, services, hosts):
    parentItems = super(RBLight10StackAdvisor, self).getComponentLayoutValidations(services, hosts)

    childItems = []

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item for sublist in componentsListList for item in sublist]

    cassandraSeedHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "CASSANDRA_SEED"]
    cassandraNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "CASSANDRA_NODE"]

    if len(cassandraSeedHosts) > 0 and len(cassandraNodeHosts) > 0:
      commonHosts = [host for host in cassandraSeedHosts[0] if host in cassandraNodeHosts[0]]
      for host in commonHosts:
        message = "Cassandra Seed and Cassandra Node should not be deployed on the same host."
        childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'CASSANDRA_NODE', "host": host } )

    esMasterHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "ELASTICSEARCH_MASTER"]
    esSlaveHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "ELASTICSEARCH_SLAVE"]

    if len(esMasterHosts) > 0 and len(esSlaveHosts) > 0:
      commonHosts = [host for host in esMasterHosts[0] if host in esSlaveHosts[0]]
      for host in commonHosts:
        message = "Elasticsearch seed and Elasticsearch node should not be deployed on the same host."
        childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'ELASTICSEARCH_SLAVE', "host": host } )

    parentItems.extend(childItems)
    return parentItems

  def getComponentLayoutSchemes(self):
    parentSchemes = super(RBLight10StackAdvisor, self).getComponentLayoutSchemes()
    return parentSchemes

  def validateAmsHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    return []

  def validateHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    return []

  def validateStormConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    return []

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(RBLight10StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "FLINK": self.recommendFlinkConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendFlinkConfigurations(self, configurations, clusterData, services, hosts):
    putFlinkProperty = self.putProperty(configurations, "flink-conf")
    putFlinkProperty("taskmanager.numberOfTaskSlots", multiprocessing.cpu_count())