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


class RBP30StackAdvisor(HDP23StackAdvisor):

  def getComponentLayoutValidations(self, services, hosts):
    parentItems = super(RBP30StackAdvisor, self).getComponentLayoutValidations(services, hosts)

    # cassandraExists = "CASSANDRA" in [service["StackServices"]["service_name"] for service in services["services"]]

    childItems = []
    # hostsList = [host["Hosts"]["host_name"] for host in hosts["items"]]
    # hostsCount = len(hostsList)

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item for sublist in componentsListList for item in sublist]
    cassandraSeedHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "CASSANDRA_SEED"]
    cassandraNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "CASSANDRA_NODE"]

    # single node case is not analyzed because HAWQ Standby Master will not be present in single node topology due to logic in createComponentLayoutRecommendations()
    if len(cassandraSeedHosts) > 0 and len(cassandraNodeHosts) > 0:
      commonHosts = [host for host in cassandraSeedHosts[0] if host in cassandraNodeHosts[0]]
      for host in commonHosts:
        message = "Cassandra Seed and Cassandra Node should not be deployed on the same host."
        childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'CASSANDRA_NODE', "host": host } )

    parentItems.extend(childItems)
    return parentItems

    # hiveExists = "HIVE" in [service["StackServices"]["service_name"] for service in services["services"]]
    # sparkExists = "SPARK" in [service["StackServices"]["service_name"] for service in services["services"]]
    #
    # if not "HAWQ" in [service["StackServices"]["service_name"] for service in services["services"]] and not sparkExists:
    #   return parentItems
    #
    # childItems = []
    # hostsList = [host["Hosts"]["host_name"] for host in hosts["items"]]
    # hostsCount = len(hostsList)
    #
    # componentsListList = [service["components"] for service in services["services"]]
    # componentsList = [item for sublist in componentsListList for item in sublist]
    # hawqMasterHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HAWQMASTER"]
    # hawqStandbyHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HAWQSTANDBY"]
    #
    # # single node case is not analyzed because HAWQ Standby Master will not be present in single node topology due to logic in createComponentLayoutRecommendations()
    # if len(hawqMasterHosts) > 0 and len(hawqStandbyHosts) > 0:
    #   commonHosts = [host for host in hawqMasterHosts[0] if host in hawqStandbyHosts[0]]
    #   for host in commonHosts:
    #     message = "HAWQ Standby Master and HAWQ Master should not be deployed on the same host."
    #     childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'HAWQSTANDBY', "host": host } )
    #
    # if len(hawqMasterHosts) > 0 and hostsCount > 1:
    #   ambariServerHosts = [host for host in hawqMasterHosts[0] if self.isLocalHost(host)]
    #   for host in ambariServerHosts:
    #     message = "HAWQ Master and Ambari Server should not be deployed on the same host. " \
    #               "If you leave them collocated, make sure to set HAWQ Master Port property " \
    #               "to a value different from the port number used by Ambari Server database."
    #     childItems.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'HAWQMASTER', "host": host } )
    #
    # if len(hawqStandbyHosts) > 0 and hostsCount > 1:
    #   ambariServerHosts = [host for host in hawqStandbyHosts[0] if self.isLocalHost(host)]
    #   for host in ambariServerHosts:
    #     message = "HAWQ Standby Master and Ambari Server should not be deployed on the same host. " \
    #               "If you leave them collocated, make sure to set HAWQ Master Port property " \
    #               "to a value different from the port number used by Ambari Server database."
    #     childItems.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'HAWQSTANDBY', "host": host } )
    #
    # if "SPARK_THRIFTSERVER" in [service["StackServices"]["service_name"] for service in services["services"]]:
    #   if not "HIVE_SERVER" in [service["StackServices"]["service_name"] for service in services["services"]]:
    #     message = "SPARK_THRIFTSERVER requires HIVE services to be selected."
    #     childItems.append( {"type": 'host-component', "level": 'ERROR', "message": messge, "component-name": 'SPARK_THRIFTSERVER'} )
    #
    # hmsHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HIVE_METASTORE"][0] if hiveExists else []
    # sparkTsHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "SPARK_THRIFTSERVER"][0] if sparkExists else []
    #
    # # if Spark Thrift Server is deployed but no Hive Server is deployed
    # if len(sparkTsHosts) > 0 and len(hmsHosts) == 0:
    #   message = "SPARK_THRIFTSERVER requires HIVE_METASTORE to be selected/deployed."
    #   childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'SPARK_THRIFTSERVER' } )
    #
    # parentItems.extend(childItems)
    # return parentItems