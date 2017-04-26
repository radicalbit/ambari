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
import os
import fnmatch
import socket
import math
from math import floor
from urlparse import urlparse
import fnmatch
import re
import xml.etree.ElementTree as ET
import sys
from math import ceil, floor

from stack_advisor import DefaultStackAdvisor

class RBLight0206StackAdvisor(DefaultStackAdvisor):

  def getComponentLayoutValidations(self, services, hosts):
    """Returns array of Validation objects about issues with hostnames components assigned to"""
    items = []

    # Validating NAMENODE and SECONDARY_NAMENODE are on different hosts if possible
    hostsList = [host["Hosts"]["host_name"] for host in hosts["items"]]
    hostsCount = len(hostsList)

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item for sublist in componentsListList for item in sublist]
    nameNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "NAMENODE"]
    secondaryNameNodeHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "SECONDARY_NAMENODE"]

    # Validating cardinality
    for component in componentsList:
      if component["StackServiceComponents"]["cardinality"] is not None:
        componentName = component["StackServiceComponents"]["component_name"]
        componentDisplayName = component["StackServiceComponents"]["display_name"]
        componentHostsCount = 0
        if component["StackServiceComponents"]["hostnames"] is not None:
          componentHostsCount = len(component["StackServiceComponents"]["hostnames"])
        cardinality = str(component["StackServiceComponents"]["cardinality"])
        # cardinality types: null, 1+, 1-2, 1, ALL
        message = None
        if "+" in cardinality:
          hostsMin = int(cardinality[:-1])
          if componentHostsCount < hostsMin:
            message = "At least {0} {1} components should be installed in cluster.".format(hostsMin, componentDisplayName)
        elif "-" in cardinality:
          nums = cardinality.split("-")
          hostsMin = int(nums[0])
          hostsMax = int(nums[1])
          if componentHostsCount > hostsMax or componentHostsCount < hostsMin:
            message = "Between {0} and {1} {2} components should be installed in cluster.".format(hostsMin, hostsMax, componentDisplayName)
        elif "ALL" == cardinality:
          if componentHostsCount != hostsCount:
            message = "{0} component should be installed on all hosts in cluster.".format(componentDisplayName)
        else:
          if componentHostsCount != int(cardinality):
            message = "Exactly {0} {1} components should be installed in cluster.".format(int(cardinality), componentDisplayName)

        if message is not None:
          items.append({"type": 'host-component', "level": 'ERROR', "message": message, "component-name": componentName})

    # Validating host-usage
    usedHostsListList = [component["StackServiceComponents"]["hostnames"] for component in componentsList if not self.isComponentNotValuable(component)]
    usedHostsList = [item for sublist in usedHostsListList for item in sublist]
    nonUsedHostsList = [item for item in hostsList if item not in usedHostsList]
    for host in nonUsedHostsList:
      items.append( { "type": 'host-component', "level": 'ERROR', "message": 'Host is not used', "host": str(host) } )

    return items

  def getServiceConfigurationRecommenderDict(self):
    return {
      "YARN": self.recommendYARNConfigurations,
      "MAPREDUCE2": self.recommendMapReduce2Configurations,
      "HDFS": self.recommendHDFSConfigurations,
      "HBASE": self.recommendHbaseConfigurations,
      "STORM": self.recommendStormConfigurations,
      "AMBARI_METRICS": self.recommendAmsConfigurations,
      "RANGER": self.recommendRangerConfigurations
    }

  def putProperty(self, config, configType, services=None):
    userConfigs = {}
    changedConfigs = []
    # if services parameter, prefer values, set by user
    if services:
      if 'configurations' in services.keys():
        userConfigs = services['configurations']
      if 'changed-configurations' in services.keys():
        changedConfigs = services["changed-configurations"]

    if configType not in config:
      config[configType] = {}
    if"properties" not in config[configType]:
      config[configType]["properties"] = {}
    def appendProperty(key, value):
      # If property exists in changedConfigs, do not override, use user defined property
      if self.__isPropertyInChangedConfigs(configType, key, changedConfigs):
        config[configType]["properties"][key] = userConfigs[configType]['properties'][key]
      else:
        config[configType]["properties"][key] = str(value)
    return appendProperty

  def __isPropertyInChangedConfigs(self, configType, propertyName, changedConfigs):
    for changedConfig in changedConfigs:
      if changedConfig['type']==configType and changedConfig['name']==propertyName:
        return True
    return False

  def putPropertyAttribute(self, config, configType):
    if configType not in config:
      config[configType] = {}
    def appendPropertyAttribute(key, attribute, attributeValue):
      if "property_attributes" not in config[configType]:
        config[configType]["property_attributes"] = {}
      if key not in config[configType]["property_attributes"]:
        config[configType]["property_attributes"][key] = {}
      config[configType]["property_attributes"][key][attribute] = attributeValue if isinstance(attributeValue, list) else str(attributeValue)
    return appendPropertyAttribute

  def recommendYARNConfigurations(self, configurations, clusterData, services, hosts):
    putYarnProperty = self.putProperty(configurations, "yarn-site", services)
    putYarnEnvProperty = self.putProperty(configurations, "yarn-env", services)
    nodemanagerMinRam = 1048576 # 1TB in mb
    if "referenceNodeManagerHost" in clusterData:
      nodemanagerMinRam = min(clusterData["referenceNodeManagerHost"]["total_mem"]/1024, nodemanagerMinRam)
    putYarnProperty('yarn.nodemanager.resource.memory-mb', int(round(min(clusterData['containers'] * clusterData['ramPerContainer'], nodemanagerMinRam))))
    putYarnProperty('yarn.scheduler.minimum-allocation-mb', int(clusterData['ramPerContainer']))
    putYarnProperty('yarn.scheduler.maximum-allocation-mb', int(configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"]))
    putYarnEnvProperty('min_user_id', self.get_system_min_uid())
    containerExecutorGroup = 'hadoop'
    if 'cluster-env' in services['configurations'] and 'user_group' in services['configurations']['cluster-env']['properties']:
      containerExecutorGroup = services['configurations']['cluster-env']['properties']['user_group']
    putYarnProperty("yarn.nodemanager.linux-container-executor.group", containerExecutorGroup)

  def recommendMapReduce2Configurations(self, configurations, clusterData, services, hosts):
    putMapredProperty = self.putProperty(configurations, "mapred-site", services)
    putMapredProperty('yarn.app.mapreduce.am.resource.mb', int(clusterData['amMemory']))
    putMapredProperty('yarn.app.mapreduce.am.command-opts', "-Xmx" + str(int(round(0.8 * clusterData['amMemory']))) + "m")
    putMapredProperty('mapreduce.map.memory.mb', clusterData['mapMemory'])
    putMapredProperty('mapreduce.reduce.memory.mb', int(clusterData['reduceMemory']))
    putMapredProperty('mapreduce.map.java.opts', "-Xmx" + str(int(round(0.8 * clusterData['mapMemory']))) + "m")
    putMapredProperty('mapreduce.reduce.java.opts', "-Xmx" + str(int(round(0.8 * clusterData['reduceMemory']))) + "m")
    putMapredProperty('mapreduce.task.io.sort.mb', min(int(round(0.4 * clusterData['mapMemory'])), 1024))

  def recommendHadoopProxyUsers (self, configurations, services, hosts):
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    users = {}

    if 'forced-configurations' not in services:
      services["forced-configurations"] = []

    if "HDFS" in servicesList:
      hdfs_user = None
      if "hadoop-env" in services["configurations"] and "hdfs_user" in services["configurations"]["hadoop-env"]["properties"]:
        hdfs_user = services["configurations"]["hadoop-env"]["properties"]["hdfs_user"]
        if not hdfs_user in users and hdfs_user is not None:
          users[hdfs_user] = {"propertyHosts" : "*","propertyGroups" : "*", "config" : "hadoop-env", "propertyName" : "hdfs_user"}

    if "OOZIE" in servicesList:
      oozie_user = None
      if "oozie-env" in services["configurations"] and "oozie_user" in services["configurations"]["oozie-env"]["properties"]:
        oozie_user = services["configurations"]["oozie-env"]["properties"]["oozie_user"]
        oozieServerrHost = self.getHostWithComponent("OOZIE", "OOZIE_SERVER", services, hosts)
        if oozieServerrHost is not None:
          oozieServerHostName = oozieServerrHost["Hosts"]["public_host_name"]
          if not oozie_user in users and oozie_user is not None:
            users[oozie_user] = {"propertyHosts" : oozieServerHostName,"propertyGroups" : "*", "config" : "oozie-env", "propertyName" : "oozie_user"}

    if "HIVE" in servicesList:
      hive_user = None
      webhcat_user = None
      if "hive-env" in services["configurations"] and "hive_user" in services["configurations"]["hive-env"]["properties"] \
              and "webhcat_user" in services["configurations"]["hive-env"]["properties"]:
        hive_user = services["configurations"]["hive-env"]["properties"]["hive_user"]
        webhcat_user = services["configurations"]["hive-env"]["properties"]["webhcat_user"]
        hiveServerrHost = self.getHostWithComponent("HIVE", "HIVE_SERVER", services, hosts)
        if hiveServerrHost is not None:
          hiveServerHostName = hiveServerrHost["Hosts"]["public_host_name"]
          if not hive_user in users and hive_user is not None:
            users[hive_user] = {"propertyHosts" : hiveServerHostName,"propertyGroups" : "*", "config" : "hive-env", "propertyName" : "hive_user"}
          if not webhcat_user in users and webhcat_user is not None:
            users[webhcat_user] = {"propertyHosts" : hiveServerHostName,"propertyGroups" : "*", "config" : "hive-env", "propertyName" : "webhcat_user"}

    if "FALCON" in servicesList:
      falconUser = None
      if "falcon-env" in services["configurations"] and "falcon_user" in services["configurations"]["falcon-env"]["properties"]:
        falconUser = services["configurations"]["falcon-env"]["properties"]["falcon_user"]
        if not falconUser in users and falconUser is not None:
          users[falconUser] = {"propertyHosts" : "*","propertyGroups" : "*", "config" : "falcon-env", "propertyName" : "falcon_user"}

    putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
    putCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, "core-site")

    for user_name, user_properties in users.iteritems():
      # Add properties "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" to core-site for all users
      putCoreSiteProperty("hadoop.proxyuser.{0}.hosts".format(user_name) , user_properties["propertyHosts"])
      putCoreSiteProperty("hadoop.proxyuser.{0}.groups".format(user_name) , user_properties["propertyGroups"])

      # Remove old properties if user was renamed
      userOldValue = getOldValue(self, services, user_properties["config"], user_properties["propertyName"])
      if userOldValue is not None and userOldValue != user_name:
        putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.hosts".format(userOldValue), 'delete', 'true')
        putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(userOldValue), 'delete', 'true')
        services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.hosts".format(userOldValue)})
        services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.groups".format(userOldValue)})
        services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.hosts".format(user_name)})
        services["forced-configurations"].append({"type" : "core-site", "name" : "hadoop.proxyuser.{0}.groups".format(user_name)})

  def recommendHDFSConfigurations(self, configurations, clusterData, services, hosts):
    putHDFSProperty = self.putProperty(configurations, "hadoop-env", services)
    putHDFSSiteProperty = self.putProperty(configurations, "hdfs-site", services)
    putHDFSSitePropertyAttributes = self.putPropertyAttribute(configurations, "hdfs-site")
    putHDFSProperty('namenode_heapsize', max(int(clusterData['totalAvailableRam'] / 2), 1024))
    putHDFSProperty = self.putProperty(configurations, "hadoop-env", services)
    putHDFSProperty('namenode_opt_newsize', max(int(clusterData['totalAvailableRam'] / 8), 128))
    putHDFSProperty = self.putProperty(configurations, "hadoop-env", services)
    putHDFSProperty('namenode_opt_maxnewsize', max(int(clusterData['totalAvailableRam'] / 8), 256))

    # Check if NN HA is enabled and recommend removing dfs.namenode.rpc-address
    hdfsSiteProperties = getServicesSiteProperties(services, "hdfs-site")
    nameServices = None
    if hdfsSiteProperties and 'dfs.nameservices' in hdfsSiteProperties:
      nameServices = hdfsSiteProperties['dfs.nameservices']
    if nameServices and "dfs.ha.namenodes.%s" % nameServices in hdfsSiteProperties:
      namenodes = hdfsSiteProperties["dfs.ha.namenodes.%s" % nameServices]
      if len(namenodes.split(',')) > 1:
        putHDFSSitePropertyAttributes("dfs.namenode.rpc-address", "delete", "true")

    # recommendations for "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" properties in core-site
    self.recommendHadoopProxyUsers(configurations, services, hosts)

  def recommendHbaseConfigurations(self, configurations, clusterData, services, hosts):
    # recommendations for HBase env config
    putHbaseProperty = self.putProperty(configurations, "hbase-env", services)
    putHbaseProperty('hbase_regionserver_heapsize', int(clusterData['hbaseRam']) * 1024)
    putHbaseProperty('hbase_master_heapsize', int(clusterData['hbaseRam']) * 1024)

    # recommendations for HBase site config
    putHbaseSiteProperty = self.putProperty(configurations, "hbase-site", services)

    if 'hbase-site' in services['configurations'] and 'hbase.superuser' in services['configurations']['hbase-site']['properties'] \
            and 'hbase-env' in services['configurations'] and 'hbase_user' in services['configurations']['hbase-env']['properties'] \
            and services['configurations']['hbase-env']['properties']['hbase_user'] != services['configurations']['hbase-site']['properties']['hbase.superuser']:
      putHbaseSiteProperty("hbase.superuser", services['configurations']['hbase-env']['properties']['hbase_user'])


  def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):
    ranger_sql_connector_dict = {
      'MYSQL': '/usr/share/java/mysql-connector-java.jar',
      'ORACLE': '/usr/share/java/ojdbc6.jar',
      'POSTGRES': '/usr/share/java/postgresql.jar',
      'MSSQL': '/usr/share/java/sqljdbc4.jar',
      'SQLA': '/path_to_driver/sqla-client-jdbc.tar.gz'
    }

    putRangerAdminProperty = self.putProperty(configurations, "admin-properties", services)

    if 'admin-properties' in services['configurations'] and 'DB_FLAVOR' in services['configurations']['admin-properties']['properties']:
      rangerDbFlavor = services['configurations']["admin-properties"]["properties"]["DB_FLAVOR"]
      rangerSqlConnectorProperty = ranger_sql_connector_dict.get(rangerDbFlavor, ranger_sql_connector_dict['MYSQL'])
      putRangerAdminProperty('SQL_CONNECTOR_JAR', rangerSqlConnectorProperty)

    # Build policymgr_external_url
    protocol = 'http'
    ranger_admin_host = 'localhost'
    port = '6080'

    # Check if http is disabled. For HDP-2.3 this can be checked in ranger-admin-site/ranger.service.http.enabled
    # For Ranger-0.4.0 this can be checked in ranger-site/http.enabled
    if ('ranger-site' in services['configurations'] and 'http.enabled' in services['configurations']['ranger-site']['properties'] \
                and services['configurations']['ranger-site']['properties']['http.enabled'].lower() == 'false') or \
            ('ranger-admin-site' in services['configurations'] and 'ranger.service.http.enabled' in services['configurations']['ranger-admin-site']['properties'] \
                     and services['configurations']['ranger-admin-site']['properties']['ranger.service.http.enabled'].lower() == 'false'):
      # HTTPS protocol is used
      protocol = 'https'
      # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.https.port
      if 'ranger-admin-site' in services['configurations'] and \
                      'ranger.service.https.port' in services['configurations']['ranger-admin-site']['properties']:
        port = services['configurations']['ranger-admin-site']['properties']['ranger.service.https.port']
      # In Ranger-0.4.0 port stored in ranger-site https.service.port
      elif 'ranger-site' in services['configurations'] and \
                      'https.service.port' in services['configurations']['ranger-site']['properties']:
        port = services['configurations']['ranger-site']['properties']['https.service.port']
    else:
      # HTTP protocol is used
      # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.http.port
      if 'ranger-admin-site' in services['configurations'] and \
                      'ranger.service.http.port' in services['configurations']['ranger-admin-site']['properties']:
        port = services['configurations']['ranger-admin-site']['properties']['ranger.service.http.port']
      # In Ranger-0.4.0 port stored in ranger-site http.service.port
      elif 'ranger-site' in services['configurations'] and \
                      'http.service.port' in services['configurations']['ranger-site']['properties']:
        port = services['configurations']['ranger-site']['properties']['http.service.port']

    ranger_admin_hosts = self.getComponentHostNames(services, "RANGER", "RANGER_ADMIN")
    if ranger_admin_hosts:
      if len(ranger_admin_hosts) > 1 \
              and services['configurations'] \
              and 'admin-properties' in services['configurations'] and 'policymgr_external_url' in services['configurations']['admin-properties']['properties'] \
              and services['configurations']['admin-properties']['properties']['policymgr_external_url'] \
              and services['configurations']['admin-properties']['properties']['policymgr_external_url'].strip():

        # in case of HA deployment keep the policymgr_external_url specified in the config
        policymgr_external_url = services['configurations']['admin-properties']['properties']['policymgr_external_url']
      else:

        ranger_admin_host = ranger_admin_hosts[0]
        policymgr_external_url = "%s://%s:%s" % (protocol, ranger_admin_host, port)

    putRangerAdminProperty('policymgr_external_url', policymgr_external_url)

    rangerServiceVersion = [service['StackServices']['service_version'] for service in services["services"] if service['StackServices']['service_name'] == 'RANGER'][0]
    if rangerServiceVersion == '0.4.0':
      # Recommend ldap settings based on ambari.properties configuration
      # If 'ambari.ldap.isConfigured' == true
      # For Ranger version 0.4.0
      if 'ambari-server-properties' in services and \
                      'ambari.ldap.isConfigured' in services['ambari-server-properties'] and \
                      services['ambari-server-properties']['ambari.ldap.isConfigured'].lower() == "true":
        putUserSyncProperty = self.putProperty(configurations, "usersync-properties", services)
        serverProperties = services['ambari-server-properties']
        if 'authentication.ldap.managerDn' in serverProperties:
          putUserSyncProperty('SYNC_LDAP_BIND_DN', serverProperties['authentication.ldap.managerDn'])
        if 'authentication.ldap.primaryUrl' in serverProperties:
          ldap_protocol =  'ldap://'
          if 'authentication.ldap.useSSL' in serverProperties and serverProperties['authentication.ldap.useSSL'] == 'true':
            ldap_protocol =  'ldaps://'
          ldapUrl = ldap_protocol + serverProperties['authentication.ldap.primaryUrl'] if serverProperties['authentication.ldap.primaryUrl'] else serverProperties['authentication.ldap.primaryUrl']
          putUserSyncProperty('SYNC_LDAP_URL', ldapUrl)
        if 'authentication.ldap.userObjectClass' in serverProperties:
          putUserSyncProperty('SYNC_LDAP_USER_OBJECT_CLASS', serverProperties['authentication.ldap.userObjectClass'])
        if 'authentication.ldap.usernameAttribute' in serverProperties:
          putUserSyncProperty('SYNC_LDAP_USER_NAME_ATTRIBUTE', serverProperties['authentication.ldap.usernameAttribute'])


      # Set Ranger Admin Authentication method
      if 'admin-properties' in services['configurations'] and 'usersync-properties' in services['configurations'] and \
                      'SYNC_SOURCE' in services['configurations']['usersync-properties']['properties']:
        rangerUserSyncSource = services['configurations']['usersync-properties']['properties']['SYNC_SOURCE']
        authenticationMethod = rangerUserSyncSource.upper()
        if authenticationMethod != 'FILE':
          putRangerAdminProperty('authentication_method', authenticationMethod)

      # Recommend xasecure.audit.destination.hdfs.dir
      # For Ranger version 0.4.0
      servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
      putRangerEnvProperty = self.putProperty(configurations, "ranger-env", services)
      include_hdfs = "HDFS" in servicesList
      if include_hdfs:
        if 'core-site' in services['configurations'] and ('fs.defaultFS' in services['configurations']['core-site']['properties']):
          default_fs = services['configurations']['core-site']['properties']['fs.defaultFS']
          default_fs += '/ranger/audit/%app-type%/%time:yyyyMMdd%'
          putRangerEnvProperty('xasecure.audit.destination.hdfs.dir', default_fs)

      # Recommend Ranger Audit properties for ranger supported services
      # For Ranger version 0.4.0
      ranger_services = [
        {'service_name': 'HDFS', 'audit_file': 'ranger-hdfs-plugin-properties'},
        {'service_name': 'HBASE', 'audit_file': 'ranger-hbase-plugin-properties'},
        {'service_name': 'HIVE', 'audit_file': 'ranger-hive-plugin-properties'},
        {'service_name': 'KNOX', 'audit_file': 'ranger-knox-plugin-properties'},
        {'service_name': 'STORM', 'audit_file': 'ranger-storm-plugin-properties'}
      ]

      for item in range(len(ranger_services)):
        if ranger_services[item]['service_name'] in servicesList:
          component_audit_file =  ranger_services[item]['audit_file']
          if component_audit_file in services["configurations"]:
            ranger_audit_dict = [
              {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.db', 'target_configname': 'XAAUDIT.DB.IS_ENABLED'},
              {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs', 'target_configname': 'XAAUDIT.HDFS.IS_ENABLED'},
              {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs.dir', 'target_configname': 'XAAUDIT.HDFS.DESTINATION_DIRECTORY'}
            ]
            putRangerAuditProperty = self.putProperty(configurations, component_audit_file, services)

            for item in ranger_audit_dict:
              if item['filename'] in services["configurations"] and item['configname'] in  services["configurations"][item['filename']]["properties"]:
                if item['filename'] in configurations and item['configname'] in  configurations[item['filename']]["properties"]:
                  rangerAuditProperty = configurations[item['filename']]["properties"][item['configname']]
                else:
                  rangerAuditProperty = services["configurations"][item['filename']]["properties"][item['configname']]
                putRangerAuditProperty(item['target_configname'], rangerAuditProperty)


  def getAmsMemoryRecommendation(self, services, hosts):
    # MB per sink in hbase heapsize
    HEAP_PER_MASTER_COMPONENT = 50
    HEAP_PER_SLAVE_COMPONENT = 10

    schMemoryMap = {
      "HDFS": {
        "NAMENODE": HEAP_PER_MASTER_COMPONENT,
        "DATANODE": HEAP_PER_SLAVE_COMPONENT
      },
      "YARN": {
        "RESOURCEMANAGER": HEAP_PER_MASTER_COMPONENT,
      },
      "HBASE": {
        "HBASE_MASTER": HEAP_PER_MASTER_COMPONENT,
        "HBASE_REGIONSERVER": HEAP_PER_SLAVE_COMPONENT
      },
      "ACCUMULO": {
        "ACCUMULO_MASTER": HEAP_PER_MASTER_COMPONENT,
        "ACCUMULO_TSERVER": HEAP_PER_SLAVE_COMPONENT
      },
      "KAFKA": {
        "KAFKA_BROKER": HEAP_PER_MASTER_COMPONENT
      },
      "FLUME": {
        "FLUME_HANDLER": HEAP_PER_SLAVE_COMPONENT
      },
      "STORM": {
        "NIMBUS": HEAP_PER_MASTER_COMPONENT,
      },
      "AMBARI_METRICS": {
        "METRICS_COLLECTOR": HEAP_PER_MASTER_COMPONENT,
        "METRICS_MONITOR": HEAP_PER_SLAVE_COMPONENT
      }
    }
    total_sinks_count = 0
    # minimum heap size
    hbase_heapsize = 500
    for serviceName, componentsDict in schMemoryMap.items():
      for componentName, multiplier in componentsDict.items():
        schCount = len(
          self.getHostsWithComponent(serviceName, componentName, services,
                                     hosts))
        hbase_heapsize += int((schCount * multiplier) ** 0.9)
        total_sinks_count += schCount
    collector_heapsize = int(hbase_heapsize/4 if hbase_heapsize > 2048 else 512)

    return round_to_n(collector_heapsize), round_to_n(hbase_heapsize), total_sinks_count

  def recommendStormConfigurations(self, configurations, clusterData, services, hosts):
    putStormSiteProperty = self.putProperty(configurations, "storm-site", services)
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    # Storm AMS integration
    if 'AMBARI_METRICS' in servicesList:
      putStormSiteProperty('metrics.reporter.register', 'org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter')

  def recommendAmsConfigurations(self, configurations, clusterData, services, hosts):
    putAmsEnvProperty = self.putProperty(configurations, "ams-env", services)
    putAmsHbaseSiteProperty = self.putProperty(configurations, "ams-hbase-site", services)
    putTimelineServiceProperty = self.putProperty(configurations, "ams-site", services)
    putHbaseEnvProperty = self.putProperty(configurations, "ams-hbase-env", services)

    amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")

    rootDir = "file:///var/lib/ambari-metrics-collector/hbase"
    tmpDir = "/var/lib/ambari-metrics-collector/hbase-tmp"
    hbaseClusterDistributed = False
    if "ams-hbase-site" in services["configurations"]:
      if "hbase.rootdir" in services["configurations"]["ams-hbase-site"]["properties"]:
        rootDir = services["configurations"]["ams-hbase-site"]["properties"]["hbase.rootdir"]
      if "hbase.tmp.dir" in services["configurations"]["ams-hbase-site"]["properties"]:
        tmpDir = services["configurations"]["ams-hbase-site"]["properties"]["hbase.tmp.dir"]
      if "hbase.cluster.distributed" in services["configurations"]["ams-hbase-site"]["properties"]:
        hbaseClusterDistributed = services["configurations"]["ams-hbase-site"]["properties"]["hbase.cluster.distributed"].lower() == 'true'

    mountpoints = ["/"]
    for collectorHostName in amsCollectorHosts:
      for host in hosts["items"]:
        if host["Hosts"]["host_name"] == collectorHostName:
          mountpoints = self.getPreferredMountPoints(host["Hosts"])
          break
    isLocalRootDir = rootDir.startswith("file://")
    if isLocalRootDir:
      rootDir = re.sub("^file:///|/", "", rootDir, count=1)
      rootDir = "file://" + os.path.join(mountpoints[0], rootDir)
    tmpDir = re.sub("^file:///|/", "", tmpDir, count=1)
    if len(mountpoints) > 1 and isLocalRootDir:
      tmpDir = os.path.join(mountpoints[1], tmpDir)
    else:
      tmpDir = os.path.join(mountpoints[0], tmpDir)
    putAmsHbaseSiteProperty("hbase.rootdir", rootDir)
    putAmsHbaseSiteProperty("hbase.tmp.dir", tmpDir)

    collector_heapsize, hbase_heapsize, total_sinks_count = self.getAmsMemoryRecommendation(services, hosts)

    putAmsEnvProperty("metrics_collector_heapsize", collector_heapsize)

    # blockCache = 0.3, memstore = 0.35, phoenix-server = 0.15, phoenix-client = 0.25
    putAmsHbaseSiteProperty("hfile.block.cache.size", 0.3)
    putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 134217728)
    putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.upperLimit", 0.35)
    putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.lowerLimit", 0.3)
    putTimelineServiceProperty("timeline.metrics.host.aggregator.ttl", 86400)

    if len(amsCollectorHosts) > 1:
      pass
    else:
      # blockCache = 0.3, memstore = 0.3, phoenix-server = 0.2, phoenix-client = 0.3
      if total_sinks_count >= 2000:
        putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
        putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize", 134217728)
        putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
        putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 268435456)
        putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.upperLimit", 0.3)
        putAmsHbaseSiteProperty("hbase.regionserver.global.memstore.lowerLimit", 0.25)
        putAmsHbaseSiteProperty("phoenix.query.maxGlobalMemoryPercentage", 20)
        putTimelineServiceProperty("phoenix.query.maxGlobalMemoryPercentage", 30)
        putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 81920000)
      elif total_sinks_count >= 500:
        putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
        putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize", 134217728)
        putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
        putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 268435456)
        putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 40960000)
      else:
        putAmsHbaseSiteProperty("phoenix.coprocessor.maxMetaDataCacheSize", 20480000)
      pass

    # Distributed mode heap size
    if hbaseClusterDistributed:
      putHbaseEnvProperty("hbase_master_heapsize", "512")
      putHbaseEnvProperty("hbase_master_xmn_size", "102") #20% of 512 heap size
      putHbaseEnvProperty("hbase_regionserver_heapsize", hbase_heapsize)
      putHbaseEnvProperty("regionserver_xmn_size", round_to_n(0.15*hbase_heapsize,64))
    else:
      # Embedded mode heap size : master + regionserver
      hbase_rs_heapsize = 512
      putHbaseEnvProperty("hbase_master_heapsize", hbase_heapsize)
      putHbaseEnvProperty("hbase_master_xmn_size", round_to_n(0.15*(hbase_heapsize+hbase_rs_heapsize),64))

    # If no local DN in distributed mode
    if rootDir.startswith("hdfs://"):
      dn_hosts = self.getComponentHostNames(services, "HDFS", "DATANODE")
      if set(amsCollectorHosts).intersection(dn_hosts):
        collector_cohosted_with_dn = "true"
      else:
        collector_cohosted_with_dn = "false"
      putAmsHbaseSiteProperty("dfs.client.read.shortcircuit", collector_cohosted_with_dn)

    #split points
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    metricsDir = os.path.join(scriptDir, '../../../../common-services/AMBARI_METRICS/0.1.0/package')
    serviceMetricsDir = os.path.join(metricsDir, 'files', 'service-metrics')
    sys.path.append(os.path.join(metricsDir, 'scripts'))
    mode = 'distributed' if hbaseClusterDistributed else 'embedded'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

    from split_points import FindSplitPointsForAMSRegions

    ams_hbase_site = None
    ams_hbase_env = None

    # Overriden properties form the UI
    if "ams-hbase-site" in services["configurations"]:
      ams_hbase_site = services["configurations"]["ams-hbase-site"]["properties"]
    if "ams-hbase-env" in services["configurations"]:
      ams_hbase_env = services["configurations"]["ams-hbase-env"]["properties"]

    # Recommendations
    if not ams_hbase_site:
      ams_hbase_site = configurations["ams-hbase-site"]["properties"]
    if not ams_hbase_env:
      ams_hbase_env = configurations["ams-hbase-env"]["properties"]

    split_point_finder = FindSplitPointsForAMSRegions(
      ams_hbase_site, ams_hbase_env, serviceMetricsDir, mode, servicesList)

    result = split_point_finder.get_split_points()
    precision_splits = ' '
    aggregate_splits = ' '
    if result.precision:
      precision_splits = result.precision
    if result.aggregate:
      aggregate_splits = result.aggregate
    putTimelineServiceProperty("timeline.metrics.host.aggregate.splitpoints", ','.join(precision_splits))
    putTimelineServiceProperty("timeline.metrics.cluster.aggregate.splitpoints", ','.join(aggregate_splits))

    pass

  def getHostNamesWithComponent(self, serviceName, componentName, services):
    """
    Returns the list of hostnames on which service component is installed
    """
    if services is not None and serviceName in [service["StackServices"]["service_name"] for service in services["services"]]:
      service = [serviceEntry for serviceEntry in services["services"] if serviceEntry["StackServices"]["service_name"] == serviceName][0]
      components = [componentEntry for componentEntry in service["components"] if componentEntry["StackServiceComponents"]["component_name"] == componentName]
      if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
        componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
        return componentHostnames
    return []

  def getHostsWithComponent(self, serviceName, componentName, services, hosts):
    if services is not None and hosts is not None and serviceName in [service["StackServices"]["service_name"] for service in services["services"]]:
      service = [serviceEntry for serviceEntry in services["services"] if serviceEntry["StackServices"]["service_name"] == serviceName][0]
      components = [componentEntry for componentEntry in service["components"] if componentEntry["StackServiceComponents"]["component_name"] == componentName]
      if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
        componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
        componentHosts = [host for host in hosts["items"] if host["Hosts"]["host_name"] in componentHostnames]
        return componentHosts
    return []

  def getHostWithComponent(self, serviceName, componentName, services, hosts):
    componentHosts = self.getHostsWithComponent(serviceName, componentName, services, hosts)
    if (len(componentHosts) > 0):
      return componentHosts[0]
    return None

  def getHostComponentsByCategories(self, hostname, categories, services, hosts):
    components = []
    if services is not None and hosts is not None:
      for service in services["services"]:
        components.extend([componentEntry for componentEntry in service["components"]
                           if componentEntry["StackServiceComponents"]["component_category"] in categories
                           and hostname in componentEntry["StackServiceComponents"]["hostnames"]])
    return components

  def getZKHostPortString(self, services):
    """
    Returns the comma delimited string of zookeeper server host with the configure port installed in a cluster
    Example: zk.host1.org:2181,zk.host2.org:2181,zk.host3.org:2181
    """
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    include_zookeeper = "ZOOKEEPER" in servicesList
    zookeeper_host_port = ''

    if include_zookeeper:
      zookeeper_hosts = self.getHostNamesWithComponent("ZOOKEEPER", "ZOOKEEPER_SERVER", services)
      zookeeper_port = '2181'     #default port
      if 'zoo.cfg' in services['configurations'] and ('clientPort' in services['configurations']['zoo.cfg']['properties']):
        zookeeper_port = services['configurations']['zoo.cfg']['properties']['clientPort']

      zookeeper_host_port_arr = []
      for i in range(len(zookeeper_hosts)):
        zookeeper_host_port_arr.append(zookeeper_hosts[i] + ':' + zookeeper_port)
      zookeeper_host_port = ",".join(zookeeper_host_port_arr)
    return zookeeper_host_port

  def getConfigurationClusterSummary(self, servicesList, hosts, components, services):

    hBaseInstalled = False
    if 'HBASE' in servicesList:
      hBaseInstalled = True

    cluster = {
      "cpu": 0,
      "disk": 0,
      "ram": 0,
      "hBaseInstalled": hBaseInstalled,
      "components": components
    }

    if len(hosts["items"]) > 0:
      nodeManagerHosts = self.getHostsWithComponent("YARN", "NODEMANAGER", services, hosts)
      # NodeManager host with least memory is generally used in calculations as it will work in larger hosts.
      if nodeManagerHosts is not None and len(nodeManagerHosts) > 0:
        nodeManagerHost = nodeManagerHosts[0];
        for nmHost in nodeManagerHosts:
          if nmHost["Hosts"]["total_mem"] < nodeManagerHost["Hosts"]["total_mem"]:
            nodeManagerHost = nmHost
        host = nodeManagerHost["Hosts"]
        cluster["referenceNodeManagerHost"] = host
      else:
        host = hosts["items"][0]["Hosts"]
      cluster["referenceHost"] = host
      cluster["cpu"] = host["cpu_count"]
      cluster["disk"] = len(host["disk_info"])
      cluster["ram"] = int(host["total_mem"] / (1024 * 1024))

    ramRecommendations = [
      {"os":1, "hbase":1},
      {"os":2, "hbase":1},
      {"os":2, "hbase":2},
      {"os":4, "hbase":4},
      {"os":6, "hbase":8},
      {"os":8, "hbase":8},
      {"os":8, "hbase":8},
      {"os":12, "hbase":16},
      {"os":24, "hbase":24},
      {"os":32, "hbase":32},
      {"os":64, "hbase":64}
    ]
    index = {
      cluster["ram"] <= 4: 0,
      4 < cluster["ram"] <= 8: 1,
      8 < cluster["ram"] <= 16: 2,
      16 < cluster["ram"] <= 24: 3,
      24 < cluster["ram"] <= 48: 4,
      48 < cluster["ram"] <= 64: 5,
      64 < cluster["ram"] <= 72: 6,
      72 < cluster["ram"] <= 96: 7,
      96 < cluster["ram"] <= 128: 8,
      128 < cluster["ram"] <= 256: 9,
      256 < cluster["ram"]: 10
    }[1]
    cluster["reservedRam"] = ramRecommendations[index]["os"]
    cluster["hbaseRam"] = ramRecommendations[index]["hbase"]

    cluster["minContainerSize"] = {
      cluster["ram"] <= 4: 256,
      4 < cluster["ram"] <= 8: 512,
      8 < cluster["ram"] <= 24: 1024,
      24 < cluster["ram"]: 2048
    }[1]

    totalAvailableRam = cluster["ram"] - cluster["reservedRam"]
    if cluster["hBaseInstalled"]:
      totalAvailableRam -= cluster["hbaseRam"]
    cluster["totalAvailableRam"] = max(512, totalAvailableRam * 1024)
    '''containers = max(3, min (2*cores,min (1.8*DISKS,(Total available RAM) / MIN_CONTAINER_SIZE))))'''
    cluster["containers"] = round(max(3,
                                      min(2 * cluster["cpu"],
                                          min(ceil(1.8 * cluster["disk"]),
                                              cluster["totalAvailableRam"] / cluster["minContainerSize"]))))

    '''ramPerContainers = max(2GB, RAM - reservedRam - hBaseRam) / containers'''
    cluster["ramPerContainer"] = abs(cluster["totalAvailableRam"] / cluster["containers"])
    '''If greater than 1GB, value will be in multiples of 512.'''
    if cluster["ramPerContainer"] > 1024:
      cluster["ramPerContainer"] = int(cluster["ramPerContainer"] / 512) * 512

    cluster["mapMemory"] = int(cluster["ramPerContainer"])
    cluster["reduceMemory"] = cluster["ramPerContainer"]
    cluster["amMemory"] = max(cluster["mapMemory"], cluster["reduceMemory"])

    return cluster

  def getConfigurationsValidationItems(self, services, hosts):
    """Returns array of Validation objects about issues with configuration values provided in services"""
    items = []

    recommendations = self.recommendConfigurations(services, hosts)
    recommendedDefaults = recommendations["recommendations"]["blueprint"]["configurations"]

    configurations = services["configurations"]
    for service in services["services"]:
      serviceName = service["StackServices"]["service_name"]
      validator = self.validateServiceConfigurations(serviceName)
      if validator is not None:
        for siteName, method in validator.items():
          if siteName in recommendedDefaults:
            siteProperties = getSiteProperties(configurations, siteName)
            if siteProperties is not None:
              siteRecommendations = recommendedDefaults[siteName]["properties"]
              print("SiteName: %s, method: %s\n" % (siteName, method.__name__))
              print("Site properties: %s\n" % str(siteProperties))
              print("Recommendations: %s\n********\n" % str(siteRecommendations))
              resultItems = method(siteProperties, siteRecommendations, configurations, services, hosts)
              items.extend(resultItems)

    clusterWideItems = self.validateClusterConfigurations(configurations, services, hosts)
    items.extend(clusterWideItems)
    self.validateMinMax(items, recommendedDefaults, configurations)
    return items

  def validateClusterConfigurations(self, configurations, services, hosts):
    validationItems = []

    return self.toConfigurationValidationProblems(validationItems, "")

  def getServiceConfigurationValidators(self):
    return {
      "HDFS": {"hadoop-env": self.validateHDFSConfigurationsEnv},
      "MAPREDUCE2": {"mapred-site": self.validateMapReduce2Configurations},
      "YARN": {"yarn-site": self.validateYARNConfigurations},
      "HBASE": {"hbase-env": self.validateHbaseEnvConfigurations},
      "STORM": {"storm-site": self.validateStormConfigurations},
      "AMBARI_METRICS": {"ams-hbase-site": self.validateAmsHbaseSiteConfigurations,
                         "ams-hbase-env": self.validateAmsHbaseEnvConfigurations,
                         "ams-site": self.validateAmsSiteConfigurations}
    }

  def validateMinMax(self, items, recommendedDefaults, configurations):

    # required for casting to the proper numeric type before comparison
    def convertToNumber(number):
      try:
        return int(number)
      except ValueError:
        return float(number)

    for configName in configurations:
      validationItems = []
      if configName in recommendedDefaults and "property_attributes" in recommendedDefaults[configName]:
        for propertyName in recommendedDefaults[configName]["property_attributes"]:
          if propertyName in configurations[configName]["properties"]:
            if "maximum" in recommendedDefaults[configName]["property_attributes"][propertyName] and \
                            propertyName in recommendedDefaults[configName]["properties"]:
              userValue = convertToNumber(configurations[configName]["properties"][propertyName])
              maxValue = convertToNumber(recommendedDefaults[configName]["property_attributes"][propertyName]["maximum"])
              if userValue > maxValue:
                validationItems.extend([{"config-name": propertyName, "item": self.getWarnItem("Value is greater than the recommended maximum of {0} ".format(maxValue))}])
            if "minimum" in recommendedDefaults[configName]["property_attributes"][propertyName] and \
                            propertyName in recommendedDefaults[configName]["properties"]:
              userValue = convertToNumber(configurations[configName]["properties"][propertyName])
              minValue = convertToNumber(recommendedDefaults[configName]["property_attributes"][propertyName]["minimum"])
              if userValue < minValue:
                validationItems.extend([{"config-name": propertyName, "item": self.getWarnItem("Value is less than the recommended minimum of {0} ".format(minValue))}])
      items.extend(self.toConfigurationValidationProblems(validationItems, configName))
    pass

  def validateAmsSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []

    op_mode = properties.get("timeline.metrics.service.operation.mode")
    correct_op_mode_item = None
    if op_mode not in ("embedded", "distributed"):
      correct_op_mode_item = self.getErrorItem("Correct value should be set.")
      pass

    validationItems.extend([{"config-name":'timeline.metrics.service.operation.mode', "item": correct_op_mode_item }])
    return self.toConfigurationValidationProblems(validationItems, "ams-site")

  def validateAmsHbaseSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):

    amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")
    ams_site = getSiteProperties(configurations, "ams-site")

    collector_heapsize, hbase_heapsize, total_sinks_count = self.getAmsMemoryRecommendation(services, hosts)
    recommendedDiskSpace = 10485760
    # TODO validate configuration for multiple AMBARI_METRICS collectors
    if len(amsCollectorHosts) > 1:
      pass
    else:
      if total_sinks_count > 2000:
        recommendedDiskSpace  = 104857600  # * 1k == 100 Gb
      elif total_sinks_count > 500:
        recommendedDiskSpace  = 52428800  # * 1k == 50 Gb
      elif total_sinks_count > 250:
        recommendedDiskSpace  = 20971520  # * 1k == 20 Gb

    validationItems = []

    rootdir_item = None
    op_mode = ams_site.get("timeline.metrics.service.operation.mode")
    hbase_rootdir = properties.get("hbase.rootdir")
    hbase_tmpdir = properties.get("hbase.tmp.dir")
    if op_mode == "distributed" and hbase_rootdir.startswith("file://"):
      rootdir_item = self.getWarnItem("In distributed mode hbase.rootdir should point to HDFS.")
      pass

    distributed_item = None
    distributed = properties.get("hbase.cluster.distributed")
    if hbase_rootdir and hbase_rootdir.startswith("hdfs://") and not distributed.lower() == "true":
      distributed_item = self.getErrorItem("Distributed property should be set to true if hbase.rootdir points to HDFS.")

    validationItems.extend([{"config-name":'hbase.rootdir', "item": rootdir_item },
                            {"config-name":'hbase.cluster.distributed', "item": distributed_item }])

    for collectorHostName in amsCollectorHosts:
      for host in hosts["items"]:
        if host["Hosts"]["host_name"] == collectorHostName:
          if op_mode == 'embedded':
            validationItems.extend([{"config-name": 'hbase.rootdir', "item": self.validatorEnoughDiskSpace(properties, 'hbase.rootdir', host["Hosts"], recommendedDiskSpace)}])
            validationItems.extend([{"config-name": 'hbase.rootdir', "item": self.validatorNotRootFs(properties, recommendedDefaults, 'hbase.rootdir', host["Hosts"])}])
            validationItems.extend([{"config-name": 'hbase.tmp.dir', "item": self.validatorNotRootFs(properties, recommendedDefaults, 'hbase.tmp.dir', host["Hosts"])}])

          dn_hosts = self.getComponentHostNames(services, "HDFS", "DATANODE")
          if not hbase_rootdir.startswith("hdfs"):
            mountPoints = []
            for mountPoint in host["Hosts"]["disk_info"]:
              mountPoints.append(mountPoint["mountpoint"])
            hbase_rootdir_mountpoint = getMountPointForDir(hbase_rootdir, mountPoints)
            hbase_tmpdir_mountpoint = getMountPointForDir(hbase_tmpdir, mountPoints)
            preferred_mountpoints = self.getPreferredMountPoints(host['Hosts'])
            # hbase.rootdir and hbase.tmp.dir shouldn't point to the same partition
            # if multiple preferred_mountpoints exist
            if hbase_rootdir_mountpoint == hbase_tmpdir_mountpoint and \
                            len(preferred_mountpoints) > 1:
              item = self.getWarnItem("Consider not using {0} partition for storing metrics temporary data. "
                                      "{0} partition is already used as hbase.rootdir to store metrics data".format(hbase_tmpdir_mountpoint))
              validationItems.extend([{"config-name":'hbase.tmp.dir', "item": item}])

            # if METRICS_COLLECTOR is co-hosted with DATANODE
            # cross-check dfs.datanode.data.dir and hbase.rootdir
            # they shouldn't share same disk partition IO
            hdfs_site = getSiteProperties(configurations, "hdfs-site")
            dfs_datadirs = hdfs_site.get("dfs.datanode.data.dir").split(",") if hdfs_site and "dfs.datanode.data.dir" in hdfs_site else []
            if dn_hosts and collectorHostName in dn_hosts and ams_site and \
                    dfs_datadirs and len(preferred_mountpoints) > len(dfs_datadirs):
              for dfs_datadir in dfs_datadirs:
                dfs_datadir_mountpoint = getMountPointForDir(dfs_datadir, mountPoints)
                if dfs_datadir_mountpoint == hbase_rootdir_mountpoint:
                  item = self.getWarnItem("Consider not using {0} partition for storing metrics data. "
                                          "{0} is already used by datanode to store HDFS data".format(hbase_rootdir_mountpoint))
                  validationItems.extend([{"config-name": 'hbase.rootdir', "item": item}])
                  break
          # If no local DN in distributed mode
          elif collectorHostName not in dn_hosts and distributed.lower() == "true":
            item = self.getWarnItem("It's recommended to install Datanode component on {0} "
                                    "to speed up IO operations between HDFS and Metrics "
                                    "Collector in distributed mode ".format(collectorHostName))
            validationItems.extend([{"config-name": "hbase.cluster.distributed", "item": item}])
          # Short circuit read should be enabled in distibuted mode
          # if local DN installed
          else:
            validationItems.extend([{"config-name": "dfs.client.read.shortcircuit", "item": self.validatorEqualsToRecommendedItem(properties, recommendedDefaults, "dfs.client.read.shortcircuit")}])

    return self.toConfigurationValidationProblems(validationItems, "ams-hbase-site")

  def validateStormConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    # Storm AMS integration
    if 'AMBARI_METRICS' in servicesList and "metrics.reporter.register" in properties and \
                    "org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter" not in properties.get("metrics.reporter.register"):

      validationItems.append({"config-name": 'metrics.reporter.register',
                              "item": self.getWarnItem(
                                "Should be set to org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter to report the metrics to Ambari Metrics service.")})

    return self.toConfigurationValidationProblems(validationItems, "storm-site")

  def validateAmsHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):

    ams_env = getSiteProperties(configurations, "ams-env")
    amsHbaseSite = getSiteProperties(configurations, "ams-hbase-site")
    validationItems = []
    mb = 1024 * 1024
    gb = 1024 * mb

    regionServerItem = self.validatorLessThenDefaultValue(properties, recommendedDefaults, "hbase_regionserver_heapsize") ## FIXME if new service added
    if regionServerItem:
      validationItems.extend([{"config-name": "hbase_regionserver_heapsize", "item": regionServerItem}])

    hbaseMasterHeapsizeItem = self.validatorLessThenDefaultValue(properties, recommendedDefaults, "hbase_master_heapsize")
    if hbaseMasterHeapsizeItem:
      validationItems.extend([{"config-name": "hbase_master_heapsize", "item": hbaseMasterHeapsizeItem}])

    logDirItem = self.validatorEqualsPropertyItem(properties, "hbase_log_dir", ams_env, "metrics_collector_log_dir")
    if logDirItem:
      validationItems.extend([{"config-name": "hbase_log_dir", "item": logDirItem}])

    collector_heapsize = to_number(ams_env.get("metrics_collector_heapsize"))
    hbase_master_heapsize = to_number(properties["hbase_master_heapsize"])
    hbase_master_xmn_size = to_number(properties["hbase_master_xmn_size"])
    hbase_regionserver_heapsize = to_number(properties["hbase_regionserver_heapsize"])
    hbase_regionserver_xmn_size = to_number(properties["regionserver_xmn_size"])

    # Validate Xmn settings.
    masterXmnItem = None
    regionServerXmnItem = None
    is_hbase_distributed = amsHbaseSite.get("hbase.cluster.distributed").lower() == 'true'

    if is_hbase_distributed:
      minMasterXmn = 0.12 * hbase_master_heapsize
      maxMasterXmn = 0.2 * hbase_master_heapsize
      if hbase_master_xmn_size < minMasterXmn:
        masterXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                         "(12% of hbase_master_heapsize)".format(int(ceil(minMasterXmn))))

      if hbase_master_xmn_size > maxMasterXmn:
        masterXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                         "(20% of hbase_master_heapsize)".format(int(floor(maxMasterXmn))))

      minRegionServerXmn = 0.12 * hbase_regionserver_heapsize
      maxRegionServerXmn = 0.2 * hbase_regionserver_heapsize
      if hbase_regionserver_xmn_size < minRegionServerXmn:
        regionServerXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                               "(12% of hbase_regionserver_heapsize)"
                                               .format(int(ceil(minRegionServerXmn))))

      if hbase_regionserver_xmn_size > maxRegionServerXmn:
        regionServerXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                               "(20% of hbase_regionserver_heapsize)"
                                               .format(int(floor(maxRegionServerXmn))))
    else:
      minMasterXmn = 0.12 * (hbase_master_heapsize + hbase_regionserver_heapsize)
      maxMasterXmn = 0.2 *  (hbase_master_heapsize + hbase_regionserver_heapsize)
      if hbase_master_xmn_size < minMasterXmn:
        masterXmnItem = self.getWarnItem("Value is lesser than the recommended minimum Xmn size of {0} "
                                         "(12% of hbase_master_heapsize + hbase_regionserver_heapsize)"
                                         .format(int(ceil(minMasterXmn))))

      if hbase_master_xmn_size > maxMasterXmn:
        masterXmnItem = self.getWarnItem("Value is greater than the recommended maximum Xmn size of {0} "
                                         "(20% of hbase_master_heapsize + hbase_regionserver_heapsize)"
                                         .format(int(floor(maxMasterXmn))))
    if masterXmnItem:
      validationItems.extend([{"config-name": "hbase_master_xmn_size", "item": masterXmnItem}])

    if regionServerXmnItem:
      validationItems.extend([{"config-name": "regionserver_xmn_size", "item": regionServerXmnItem}])

    if hbaseMasterHeapsizeItem is None:
      hostMasterComponents = {}

      for service in services["services"]:
        for component in service["components"]:
          if component["StackServiceComponents"]["hostnames"] is not None:
            for hostName in component["StackServiceComponents"]["hostnames"]:
              if self.isMasterComponent(component):
                if hostName not in hostMasterComponents.keys():
                  hostMasterComponents[hostName] = []
                hostMasterComponents[hostName].append(component["StackServiceComponents"]["component_name"])

      amsCollectorHosts = self.getComponentHostNames(services, "AMBARI_METRICS", "METRICS_COLLECTOR")
      for collectorHostName in amsCollectorHosts:
        for host in hosts["items"]:
          if host["Hosts"]["host_name"] == collectorHostName:
            # AMS Collector co-hosted with other master components in bigger clusters
            if len(hosts['items']) > 31 and \
                            len(hostMasterComponents[collectorHostName]) > 2 and \
                            host["Hosts"]["total_mem"] < 32*mb: # < 32Gb(total_mem in k)
              masterHostMessage = "Host {0} is used by multiple master components ({1}). " \
                                  "It is recommended to use a separate host for the " \
                                  "Ambari Metrics Collector component and ensure " \
                                  "the host has sufficient memory available."

              hbaseMasterHeapsizeItem = self.getWarnItem(masterHostMessage.format(
                collectorHostName, str(", ".join(hostMasterComponents[collectorHostName]))))
              if hbaseMasterHeapsizeItem:
                validationItems.extend([{"config-name": "hbase_master_heapsize", "item": hbaseMasterHeapsizeItem}])

            # Check for unused RAM on AMS Collector node
            hostComponents = []
            for service in services["services"]:
              for component in service["components"]:
                if component["StackServiceComponents"]["hostnames"] is not None:
                  if collectorHostName in component["StackServiceComponents"]["hostnames"]:
                    hostComponents.append(component["StackServiceComponents"]["component_name"])

            requiredMemory = getMemorySizeRequired(hostComponents, configurations)
            unusedMemory = host["Hosts"]["total_mem"] * 1024 - requiredMemory # in bytes
            if unusedMemory > 4*gb:  # warn user, if more than 4GB RAM is unused
              heapPropertyToIncrease = "hbase_regionserver_heapsize" if is_hbase_distributed else "hbase_master_heapsize"
              xmnPropertyToIncrease = "regionserver_xmn_size" if is_hbase_distributed else "hbase_master_xmn_size"
              recommended_collector_heapsize = int((unusedMemory - 4*gb)/5) + collector_heapsize*mb
              recommended_hbase_heapsize = int((unusedMemory - 4*gb)*4/5) + to_number(properties.get(heapPropertyToIncrease))*mb
              recommended_hbase_heapsize = min(32*gb, recommended_hbase_heapsize) #Make sure heapsize <= 32GB
              recommended_xmn_size = round_to_n(0.12*recommended_hbase_heapsize/mb,128)

              if collector_heapsize < recommended_collector_heapsize or \
                              to_number(properties[heapPropertyToIncrease]) < recommended_hbase_heapsize:
                collectorHeapsizeItem = self.getWarnItem("{0} MB RAM is unused on the host {1} based on components " \
                                                         "assigned. Consider allocating  {2} MB to " \
                                                         "metrics_collector_heapsize in ams-env, " \
                                                         "{3} MB to {4} in ams-hbase-env"
                                                         .format(unusedMemory/mb, collectorHostName,
                                                                 recommended_collector_heapsize/mb,
                                                                 recommended_hbase_heapsize/mb,
                                                                 heapPropertyToIncrease))
                validationItems.extend([{"config-name": heapPropertyToIncrease, "item": collectorHeapsizeItem}])

              if to_number(properties[xmnPropertyToIncrease]) < recommended_hbase_heapsize:
                xmnPropertyToIncreaseItem = self.getWarnItem("Consider allocating {0} MB to use up some unused memory "
                                                             "on host".format(recommended_xmn_size))
                validationItems.extend([{"config-name": xmnPropertyToIncrease, "item": xmnPropertyToIncreaseItem}])
      pass

    return self.toConfigurationValidationProblems(validationItems, "ams-hbase-env")


  def validateServiceConfigurations(self, serviceName):
    return self.getServiceConfigurationValidators().get(serviceName, None)

  def toConfigurationValidationProblems(self, validationProblems, siteName):
    result = []
    for validationProblem in validationProblems:
      validationItem = validationProblem.get("item", None)
      if validationItem is not None:
        problem = {"type": 'configuration', "level": validationItem["level"], "message": validationItem["message"],
                   "config-type": siteName, "config-name": validationProblem["config-name"] }
        result.append(problem)
    return result

  def getWarnItem(self, message):
    return {"level": "WARN", "message": message}

  def getErrorItem(self, message):
    return {"level": "ERROR", "message": message}

  def getPreferredMountPoints(self, hostInfo):

    # '/etc/resolv.conf', '/etc/hostname', '/etc/hosts' are docker specific mount points
    undesirableMountPoints = ["/", "/home", "/etc/resolv.conf", "/etc/hosts",
                              "/etc/hostname", "/tmp"]
    undesirableFsTypes = ["devtmpfs", "tmpfs", "vboxsf", "CDFS"]
    mountPoints = []
    if hostInfo and "disk_info" in hostInfo:
      mountPointsDict = {}
      for mountpoint in hostInfo["disk_info"]:
        if not (mountpoint["mountpoint"] in undesirableMountPoints or
                  mountpoint["mountpoint"].startswith(("/boot", "/mnt")) or
                    mountpoint["type"] in undesirableFsTypes or
                    mountpoint["available"] == str(0)):
          mountPointsDict[mountpoint["mountpoint"]] = to_number(mountpoint["available"])
      if mountPointsDict:
        mountPoints = sorted(mountPointsDict, key=mountPointsDict.get, reverse=True)
    mountPoints.append("/")
    return mountPoints

  def validatorNotRootFs(self, properties, recommendedDefaults, propertyName, hostInfo):
    if not propertyName in properties:
      return self.getErrorItem("Value should be set")
    dir = properties[propertyName]
    if not dir.startswith("file://") or dir == recommendedDefaults.get(propertyName):
      return None

    dir = re.sub("^file://", "", dir, count=1)
    mountPoints = []
    for mountPoint in hostInfo["disk_info"]:
      mountPoints.append(mountPoint["mountpoint"])
    mountPoint = getMountPointForDir(dir, mountPoints)

    if "/" == mountPoint and self.getPreferredMountPoints(hostInfo)[0] != mountPoint:
      return self.getWarnItem("It is not recommended to use root partition for {0}".format(propertyName))

    return None

  def validatorEnoughDiskSpace(self, properties, propertyName, hostInfo, reqiuredDiskSpace):
    if not propertyName in properties:
      return self.getErrorItem("Value should be set")
    dir = properties[propertyName]
    if not dir.startswith("file://"):
      return None

    dir = re.sub("^file://", "", dir, count=1)
    mountPoints = {}
    for mountPoint in hostInfo["disk_info"]:
      mountPoints[mountPoint["mountpoint"]] = to_number(mountPoint["available"])
    mountPoint = getMountPointForDir(dir, mountPoints.keys())

    if not mountPoints:
      return self.getErrorItem("No disk info found on host %s" % hostInfo["host_name"])

    if mountPoints[mountPoint] < reqiuredDiskSpace:
      msg = "Ambari Metrics disk space requirements not met. \n" \
            "Recommended disk space for partition {0} is {1}G"
      return self.getWarnItem(msg.format(mountPoint, reqiuredDiskSpace/1048576)) # in Gb
    return None

  def validatorLessThenDefaultValue(self, properties, recommendedDefaults, propertyName):
    if propertyName not in recommendedDefaults:
      # If a property name exists in say hbase-env and hbase-site (which is allowed), then it will exist in the
      # "properties" dictionary, but not necessarily in the "recommendedDefaults" dictionary". In this case, ignore it.
      return None

    if not propertyName in properties:
      return self.getErrorItem("Value should be set")
    value = to_number(properties[propertyName])
    if value is None:
      return self.getErrorItem("Value should be integer")
    defaultValue = to_number(recommendedDefaults[propertyName])
    if defaultValue is None:
      return None
    if value < defaultValue:
      return self.getWarnItem("Value is less than the recommended default of {0}".format(defaultValue))
    return None

  def validatorEqualsPropertyItem(self, properties1, propertyName1,
                                  properties2, propertyName2,
                                  emptyAllowed=False):
    if not propertyName1 in properties1:
      return self.getErrorItem("Value should be set for %s" % propertyName1)
    if not propertyName2 in properties2:
      return self.getErrorItem("Value should be set for %s" % propertyName2)
    value1 = properties1.get(propertyName1)
    if value1 is None and not emptyAllowed:
      return self.getErrorItem("Empty value for %s" % propertyName1)
    value2 = properties2.get(propertyName2)
    if value2 is None and not emptyAllowed:
      return self.getErrorItem("Empty value for %s" % propertyName2)
    if value1 != value2:
      return self.getWarnItem("It is recommended to set equal values "
                              "for properties {0} and {1}".format(propertyName1, propertyName2))

    return None

  def validatorEqualsToRecommendedItem(self, properties, recommendedDefaults,
                                       propertyName):
    if not propertyName in properties:
      return self.getErrorItem("Value should be set for %s" % propertyName)
    value = properties.get(propertyName)
    if not propertyName in recommendedDefaults:
      return self.getErrorItem("Value should be recommended for %s" % propertyName)
    recommendedValue = recommendedDefaults.get(propertyName)
    if value != recommendedValue:
      return self.getWarnItem("It is recommended to set value {0} "
                              "for property {1}".format(recommendedValue, propertyName))
    return None

  def validateMinMemorySetting(self, properties, defaultValue, propertyName):
    if not propertyName in properties:
      return self.getErrorItem("Value should be set")
    if defaultValue is None:
      return self.getErrorItem("Config's default value can't be null or undefined")

    value = properties[propertyName]
    if value is None:
      return self.getErrorItem("Value can't be null or undefined")
    try:
      valueInt = to_number(value)
      # TODO: generify for other use cases
      defaultValueInt = int(str(defaultValue).strip())
      if valueInt < defaultValueInt:
        return self.getWarnItem("Value is less than the minimum recommended default of -Xmx" + str(defaultValue))
    except:
      return None

    return None


  def validateXmxValue(self, properties, recommendedDefaults, propertyName):
    if not propertyName in properties:
      return self.getErrorItem("Value should be set")
    value = properties[propertyName]
    defaultValue = recommendedDefaults[propertyName]
    if defaultValue is None:
      return self.getErrorItem("Config's default value can't be null or undefined")
    if not checkXmxValueFormat(value) and checkXmxValueFormat(defaultValue):
      # Xmx is in the default-value but not the value, should be an error
      return self.getErrorItem('Invalid value format')
    if not checkXmxValueFormat(defaultValue):
      # if default value does not contain Xmx, then there is no point in validating existing value
      return None
    valueInt = formatXmxSizeToBytes(getXmxSize(value))
    defaultValueXmx = getXmxSize(defaultValue)
    defaultValueInt = formatXmxSizeToBytes(defaultValueXmx)
    if valueInt < defaultValueInt:
      return self.getWarnItem("Value is less than the recommended default of -Xmx" + defaultValueXmx)
    return None

  def validateMapReduce2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'mapreduce.map.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.map.java.opts')},
                        {"config-name": 'mapreduce.reduce.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.reduce.java.opts')},
                        {"config-name": 'mapreduce.task.io.sort.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.task.io.sort.mb')},
                        {"config-name": 'mapreduce.map.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.map.memory.mb')},
                        {"config-name": 'mapreduce.reduce.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.reduce.memory.mb')},
                        {"config-name": 'yarn.app.mapreduce.am.resource.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.resource.mb')},
                        {"config-name": 'yarn.app.mapreduce.am.command-opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.command-opts')} ]
    return self.toConfigurationValidationProblems(validationItems, "mapred-site")

  def validateYARNConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    clusterEnv = getSiteProperties(configurations, "cluster-env")
    validationItems = [ {"config-name": 'yarn.nodemanager.resource.memory-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.nodemanager.resource.memory-mb')},
                        {"config-name": 'yarn.scheduler.minimum-allocation-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.scheduler.minimum-allocation-mb')},
                        {"config-name": 'yarn.nodemanager.linux-container-executor.group', "item": self.validatorEqualsPropertyItem(properties, "yarn.nodemanager.linux-container-executor.group", clusterEnv, "user_group")},
                        {"config-name": 'yarn.scheduler.maximum-allocation-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.scheduler.maximum-allocation-mb')} ]
    return self.toConfigurationValidationProblems(validationItems, "yarn-site")

  def validateHbaseEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    hbase_site = getSiteProperties(configurations, "hbase-site")
    validationItems = [ {"config-name": 'hbase_regionserver_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_regionserver_heapsize')},
                        {"config-name": 'hbase_master_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_master_heapsize')},
                        {"config-name": "hbase_user", "item": self.validatorEqualsPropertyItem(properties, "hbase_user", hbase_site, "hbase.superuser")} ]
    return self.toConfigurationValidationProblems(validationItems, "hbase-env")

  def validateHDFSConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'namenode_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_heapsize')},
                        {"config-name": 'namenode_opt_newsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_newsize')},
                        {"config-name": 'namenode_opt_maxnewsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_maxnewsize')}]
    return self.toConfigurationValidationProblems(validationItems, "hadoop-env")

  def getMastersWithMultipleInstances(self):
    return ['ZOOKEEPER_SERVER', 'HBASE_MASTER']

  def getNotValuableComponents(self):
    return ['JOURNALNODE', 'ZKFC', 'GANGLIA_MONITOR']

  def getNotPreferableOnServerComponents(self):
    return ['GANGLIA_SERVER', 'METRICS_COLLECTOR']

  def getCardinalitiesDict(self):
    return {
      'ZOOKEEPER_SERVER': {"min": 3},
      'HBASE_MASTER': {"min": 1},
    }

  def getComponentLayoutSchemes(self):
    return {
      'NAMENODE': {"else": 0},
      'SECONDARY_NAMENODE': {"else": 1},
      'HBASE_MASTER': {6: 0, 31: 2, "else": 3},

      'HISTORYSERVER': {31: 1, "else": 2},
      'RESOURCEMANAGER': {31: 1, "else": 2},

      'OOZIE_SERVER': {6: 1, 31: 2, "else": 3},

      'HIVE_SERVER': {6: 1, 31: 2, "else": 4},
      'HIVE_METASTORE': {6: 1, 31: 2, "else": 4},
      'WEBHCAT_SERVER': {6: 1, 31: 2, "else": 4},
      'METRICS_COLLECTOR': {3: 2, 6: 2, 31: 3, "else": 5},
    }

  def get_system_min_uid(self):
    login_defs = '/etc/login.defs'
    uid_min_tag = 'UID_MIN'
    comment_tag = '#'
    uid_min = uid_default = '1000'
    uid = None

    if os.path.exists(login_defs):
      with open(login_defs, 'r') as f:
        data = f.read().split('\n')
        # look for uid_min_tag in file
        uid = filter(lambda x: uid_min_tag in x, data)
        # filter all lines, where uid_min_tag was found in comments
        uid = filter(lambda x: x.find(comment_tag) > x.find(uid_min_tag) or x.find(comment_tag) == -1, uid)

      if uid is not None and len(uid) > 0:
        uid = uid[0]
        comment = uid.find(comment_tag)
        tag = uid.find(uid_min_tag)
        if comment == -1:
          uid_tag = tag + len(uid_min_tag)
          uid_min = uid[uid_tag:].strip()
        elif comment > tag:
          uid_tag = tag + len(uid_min_tag)
          uid_min = uid[uid_tag:comment].strip()

    # check result for value
    try:
      int(uid_min)
    except ValueError:
      return uid_default

    return uid_min

  def mergeValidators(self, parentValidators, childValidators):
    for service, configsDict in childValidators.iteritems():
      if service not in parentValidators:
        parentValidators[service] = {}
      parentValidators[service].update(configsDict)

def getOldValue(self, services, configType, propertyName):
  if services:
    if 'changed-configurations' in services.keys():
      changedConfigs = services["changed-configurations"]
      for changedConfig in changedConfigs:
        if changedConfig["type"] == configType and changedConfig["name"]== propertyName and "old_value" in changedConfig:
          return changedConfig["old_value"]
  return None

# Validation helper methods
def getSiteProperties(configurations, siteName):
  siteConfig = configurations.get(siteName)
  if siteConfig is None:
    return None
  return siteConfig.get("properties")

def getServicesSiteProperties(services, siteName):
  configurations = services.get("configurations")
  if not configurations:
    return None
  siteConfig = configurations.get(siteName)
  if siteConfig is None:
    return None
  return siteConfig.get("properties")

def to_number(s):
  try:
    return int(re.sub("\D", "", s))
  except ValueError:
    return None

def checkXmxValueFormat(value):
  p = re.compile('-Xmx(\d+)(b|k|m|g|p|t|B|K|M|G|P|T)?')
  matches = p.findall(value)
  return len(matches) == 1

def getXmxSize(value):
  p = re.compile("-Xmx(\d+)(.?)")
  result = p.findall(value)[0]
  if len(result) > 1:
    # result[1] - is a space or size formatter (b|k|m|g etc)
    return result[0] + result[1].lower()
  return result[0]

def formatXmxSizeToBytes(value):
  value = value.lower()
  if len(value) == 0:
    return 0
  modifier = value[-1]

  if modifier == ' ' or modifier in "0123456789":
    modifier = 'b'
  m = {
    modifier == 'b': 1,
    modifier == 'k': 1024,
    modifier == 'm': 1024 * 1024,
    modifier == 'g': 1024 * 1024 * 1024,
    modifier == 't': 1024 * 1024 * 1024 * 1024,
    modifier == 'p': 1024 * 1024 * 1024 * 1024 * 1024
  }[1]
  return to_number(value) * m

def getPort(address):
  """
  Extracts port from the address like 0.0.0.0:1019
  """
  if address is None:
    return None
  m = re.search(r'(?:http(?:s)?://)?([\w\d.]*):(\d{1,5})', address)
  if m is not None:
    return int(m.group(2))
  else:
    return None

def isSecurePort(port):
  """
  Returns True if port is root-owned at *nix systems
  """
  if port is not None:
    return port < 1024
  else:
    return False

def getMountPointForDir(dir, mountPoints):
  """
  :param dir: Directory to check, even if it doesn't exist.
  :return: Returns the closest mount point as a string for the directory.
  if the "dir" variable is None, will return None.
  If the directory does not exist, will return "/".
  """
  bestMountFound = None
  if dir:
    dir = re.sub("^file://", "", dir, count=1).strip().lower()

    # If the path is "/hadoop/hdfs/data", then possible matches for mounts could be
    # "/", "/hadoop/hdfs", and "/hadoop/hdfs/data".
    # So take the one with the greatest number of segments.
    for mountPoint in mountPoints:
      if dir.startswith(mountPoint):
        if bestMountFound is None:
          bestMountFound = mountPoint
        elif bestMountFound.count(os.path.sep) < os.path.join(mountPoint, "").count(os.path.sep):
          bestMountFound = mountPoint

  return bestMountFound

def getHeapsizeProperties():
  return { "NAMENODE": [{"config-name": "hadoop-env",
                         "property": "namenode_heapsize",
                         "default": "1024m"}],
           "DATANODE": [{"config-name": "hadoop-env",
                         "property": "dtnode_heapsize",
                         "default": "1024m"}],
           "REGIONSERVER": [{"config-name": "hbase-env",
                             "property": "hbase_regionserver_heapsize",
                             "default": "1024m"}],
           "HBASE_MASTER": [{"config-name": "hbase-env",
                             "property": "hbase_master_heapsize",
                             "default": "1024m"}],
           "HIVE_CLIENT": [{"config-name": "hive-site",
                            "property": "hive.heapsize",
                            "default": "1024m"}],
           "HISTORYSERVER": [{"config-name": "mapred-env",
                              "property": "jobhistory_heapsize",
                              "default": "1024m"}],
           "OOZIE_SERVER": [{"config-name": "oozie-env",
                             "property": "oozie_heapsize",
                             "default": "1024m"}],
           "RESOURCEMANAGER": [{"config-name": "yarn-env",
                                "property": "resourcemanager_heapsize",
                                "default": "1024m"}],
           "NODEMANAGER": [{"config-name": "yarn-env",
                            "property": "nodemanager_heapsize",
                            "default": "1024m"}],
           "APP_TIMELINE_SERVER": [{"config-name": "yarn-env",
                                    "property": "apptimelineserver_heapsize",
                                    "default": "1024m"}],
           "ZOOKEEPER_SERVER": [{"config-name": "zookeeper-env",
                                 "property": "zookeeper_heapsize",
                                 "default": "1024m"}],
           "METRICS_COLLECTOR": [{"config-name": "ams-hbase-env",
                                  "property": "hbase_master_heapsize",
                                  "default": "1024"},
                                 {"config-name": "ams-env",
                                  "property": "metrics_collector_heapsize",
                                  "default": "512"}],
           }

def getMemorySizeRequired(components, configurations):
  totalMemoryRequired = 512*1024*1024 # 512Mb for OS needs
  for component in components:
    if component in getHeapsizeProperties().keys():
      heapSizeProperties = getHeapsizeProperties()[component]
      for heapSizeProperty in heapSizeProperties:
        try:
          properties = configurations[heapSizeProperty["config-name"]]["properties"]
          heapsize = properties[heapSizeProperty["property"]]
        except KeyError:
          heapsize = heapSizeProperty["default"]

        # Assume Mb if no modifier
        if len(heapsize) > 1 and heapsize[-1] in '0123456789':
          heapsize = str(heapsize) + "m"

        totalMemoryRequired += formatXmxSizeToBytes(heapsize)

  return totalMemoryRequired

def round_to_n(mem_size, n=128):
  return int(round(mem_size / float(n))) * int(n)

class RBLight021StackAdvisor(RBLight0206StackAdvisor):

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(RBLight021StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "OOZIE": self.recommendOozieConfigurations,
      "HIVE": self.recommendHiveConfigurations,
      "TEZ": self.recommendTezConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendOozieConfigurations(self, configurations, clusterData, services, hosts):
    if "FALCON_SERVER" in clusterData["components"]:
      putOozieSiteProperty = self.putProperty(configurations, "oozie-site", services)
      falconUser = None
      if "falcon-env" in services["configurations"] and "falcon_user" in services["configurations"]["falcon-env"]["properties"]:
        falconUser = services["configurations"]["falcon-env"]["properties"]["falcon_user"]
        if falconUser is not None:
          putOozieSiteProperty("oozie.service.ProxyUserService.proxyuser.{0}.groups".format(falconUser) , "*")
          putOozieSiteProperty("oozie.service.ProxyUserService.proxyuser.{0}.hosts".format(falconUser) , "*")
        falconUserOldValue = getOldValue(self, services, "falcon-env", "falcon_user")
        if falconUserOldValue is not None:
          if 'forced-configurations' not in services:
            services["forced-configurations"] = []
          putOozieSitePropertyAttribute = self.putPropertyAttribute(configurations, "oozie-site")
          putOozieSitePropertyAttribute("oozie.service.ProxyUserService.proxyuser.{0}.groups".format(falconUserOldValue), 'delete', 'true')
          putOozieSitePropertyAttribute("oozie.service.ProxyUserService.proxyuser.{0}.hosts".format(falconUserOldValue), 'delete', 'true')
          services["forced-configurations"].append({"type" : "oozie-site", "name" : "oozie.service.ProxyUserService.proxyuser.{0}.hosts".format(falconUserOldValue)})
          services["forced-configurations"].append({"type" : "oozie-site", "name" : "oozie.service.ProxyUserService.proxyuser.{0}.groups".format(falconUserOldValue)})
          if falconUser is not None:
            services["forced-configurations"].append({"type" : "oozie-site", "name" : "oozie.service.ProxyUserService.proxyuser.{0}.hosts".format(falconUser)})
            services["forced-configurations"].append({"type" : "oozie-site", "name" : "oozie.service.ProxyUserService.proxyuser.{0}.groups".format(falconUser)})

      putMapredProperty = self.putProperty(configurations, "oozie-site")
      putMapredProperty("oozie.services.ext",
                        "org.apache.oozie.service.JMSAccessorService," +
                        "org.apache.oozie.service.PartitionDependencyManagerService," +
                        "org.apache.oozie.service.HCatAccessorService")

  def recommendHiveConfigurations(self, configurations, clusterData, services, hosts):
    containerSize = clusterData['mapMemory'] if clusterData['mapMemory'] > 2048 else int(clusterData['reduceMemory'])
    containerSize = min(clusterData['containers'] * clusterData['ramPerContainer'], containerSize)
    container_size_bytes = int(containerSize)*1024*1024
    putHiveProperty = self.putProperty(configurations, "hive-site", services)
    putHiveProperty('hive.auto.convert.join.noconditionaltask.size', int(round(container_size_bytes / 3)))
    putHiveProperty('hive.tez.java.opts', "-server -Xmx" + str(int(round((0.8 * containerSize) + 0.5)))
                    + "m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseParallelGC -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps")
    putHiveProperty('hive.tez.container.size', containerSize)

  def recommendTezConfigurations(self, configurations, clusterData, services, hosts):
    putTezProperty = self.putProperty(configurations, "tez-site")
    putTezProperty("tez.am.resource.memory.mb", int(clusterData['amMemory']))
    putTezProperty("tez.am.java.opts",
                   "-server -Xmx" + str(int(0.8 * clusterData["amMemory"]))
                   + "m -Djava.net.preferIPv4Stack=true -XX:+UseNUMA -XX:+UseParallelGC")


  def getNotPreferableOnServerComponents(self):
    return ['STORM_UI_SERVER', 'DRPC_SERVER', 'STORM_REST_API', 'NIMBUS', 'GANGLIA_SERVER', 'METRICS_COLLECTOR']

  def getNotValuableComponents(self):
    return ['JOURNALNODE', 'ZKFC', 'GANGLIA_MONITOR', 'APP_TIMELINE_SERVER']

  def getComponentLayoutSchemes(self):
    parentSchemes = super(RBLight021StackAdvisor, self).getComponentLayoutSchemes()
    childSchemes = {
      'APP_TIMELINE_SERVER': {31: 1, "else": 2},
      'FALCON_SERVER': {6: 1, 31: 2, "else": 3}
    }
    parentSchemes.update(childSchemes)
    return parentSchemes

  def getServiceConfigurationValidators(self):
    parentValidators = super(RBLight021StackAdvisor, self).getServiceConfigurationValidators()
    childValidators = {
      "HIVE": {"hive-site": self.validateHiveConfigurations},
      "TEZ": {"tez-site": self.validateTezConfigurations}
    }
    self.mergeValidators(parentValidators, childValidators)
    return parentValidators

  def validateHiveConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'hive.tez.container.size', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hive.tez.container.size')},
                        {"config-name": 'hive.tez.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'hive.tez.java.opts')},
                        {"config-name": 'hive.auto.convert.join.noconditionaltask.size', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hive.auto.convert.join.noconditionaltask.size')} ]
    yarnSiteProperties = getSiteProperties(configurations, "yarn-site")
    if yarnSiteProperties:
      yarnSchedulerMaximumAllocationMb = to_number(yarnSiteProperties["yarn.scheduler.maximum-allocation-mb"])
      hiveTezContainerSize = to_number(properties['hive.tez.container.size'])
      if hiveTezContainerSize is not None and yarnSchedulerMaximumAllocationMb is not None and hiveTezContainerSize > yarnSchedulerMaximumAllocationMb:
        validationItems.append({"config-name": 'hive.tez.container.size', "item": self.getWarnItem("hive.tez.container.size is greater than the maximum container size specified in yarn.scheduler.maximum-allocation-mb")})
    return self.toConfigurationValidationProblems(validationItems, "hive-site")

  def validateTezConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'tez.am.resource.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'tez.am.resource.memory.mb')},
                        {"config-name": 'tez.am.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'tez.am.java.opts')} ]
    return self.toConfigurationValidationProblems(validationItems, "tez-site")


class RBLight022StackAdvisor(RBLight021StackAdvisor):

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(RBLight022StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "HDFS": self.recommendHDFSConfigurations,
      "HIVE": self.recommendHIVEConfigurations,
      "HBASE": self.recommendHBASEConfigurations,
      "MAPREDUCE2": self.recommendMapReduce2Configurations,
      "TEZ": self.recommendTezConfigurations,
      "AMBARI_METRICS": self.recommendAmsConfigurations,
      "YARN": self.recommendYARNConfigurations,
      "STORM": self.recommendStormConfigurations,
      "KNOX": self.recommendKnoxConfigurations,
      "RANGER": self.recommendRangerConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendYARNConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendYARNConfigurations(configurations, clusterData, services, hosts)
    putYarnProperty = self.putProperty(configurations, "yarn-site", services)
    putYarnProperty('yarn.nodemanager.resource.cpu-vcores', clusterData['cpu'])
    putYarnProperty('yarn.scheduler.minimum-allocation-vcores', 1)
    putYarnProperty('yarn.scheduler.maximum-allocation-vcores', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])
    # Property Attributes
    putYarnPropertyAttribute = self.putPropertyAttribute(configurations, "yarn-site")
    nodeManagerHost = self.getHostWithComponent("YARN", "NODEMANAGER", services, hosts)
    if (nodeManagerHost is not None):
      cpuPercentageLimit = 0.8
      if "yarn.nodemanager.resource.percentage-physical-cpu-limit" in configurations["yarn-site"]["properties"]:
        cpuPercentageLimit = float(configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.percentage-physical-cpu-limit"])
      cpuLimit = max(1, int(floor(nodeManagerHost["Hosts"]["cpu_count"] * cpuPercentageLimit)))
      putYarnProperty('yarn.nodemanager.resource.cpu-vcores', str(cpuLimit))
      putYarnProperty('yarn.scheduler.maximum-allocation-vcores', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])
      putYarnPropertyAttribute('yarn.nodemanager.resource.memory-mb', 'maximum', int(nodeManagerHost["Hosts"]["total_mem"] / 1024)) # total_mem in kb
      putYarnPropertyAttribute('yarn.nodemanager.resource.cpu-vcores', 'maximum', nodeManagerHost["Hosts"]["cpu_count"] * 2)
      putYarnPropertyAttribute('yarn.scheduler.minimum-allocation-vcores', 'maximum', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])
      putYarnPropertyAttribute('yarn.scheduler.maximum-allocation-vcores', 'maximum', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])
      putYarnPropertyAttribute('yarn.scheduler.minimum-allocation-mb', 'maximum', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"])
      putYarnPropertyAttribute('yarn.scheduler.maximum-allocation-mb', 'maximum', configurations["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"])
      # Above is the default calculated 'maximum' values derived purely from hosts.
      # However, there are 'maximum' and other attributes that actually change based on the values
      #  of other configs. We need to update those values.
      if ("yarn-site" in services["configurations"]):
        if ("yarn.nodemanager.resource.memory-mb" in services["configurations"]["yarn-site"]["properties"]):
          putYarnPropertyAttribute('yarn.scheduler.maximum-allocation-mb', 'maximum', services["configurations"]["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"])
          putYarnPropertyAttribute('yarn.scheduler.minimum-allocation-mb', 'maximum', services["configurations"]["yarn-site"]["properties"]["yarn.nodemanager.resource.memory-mb"])
        if ("yarn.nodemanager.resource.cpu-vcores" in services["configurations"]["yarn-site"]["properties"]):
          putYarnPropertyAttribute('yarn.scheduler.maximum-allocation-vcores', 'maximum', services["configurations"]["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])
          putYarnPropertyAttribute('yarn.scheduler.minimum-allocation-vcores', 'maximum', services["configurations"]["yarn-site"]["properties"]["yarn.nodemanager.resource.cpu-vcores"])

      if "yarn-env" in services["configurations"] and "yarn_cgroups_enabled" in services["configurations"]["yarn-env"]["properties"]:
        yarn_cgroups_enabled = services["configurations"]["yarn-env"]["properties"]["yarn_cgroups_enabled"].lower() == "true"
        if yarn_cgroups_enabled:
          putYarnProperty('yarn.nodemanager.container-executor.class', 'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor')
          putYarnProperty('yarn.nodemanager.container-executor.group', 'hadoop')
          putYarnProperty('yarn.nodemanager.container-executor.resources-handler.class', 'org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler')
          putYarnProperty('yarn.nodemanager.container-executor.cgroups.hierarchy', ' /yarn')
          putYarnProperty('yarn.nodemanager.container-executor.cgroups.mount', 'true')
          putYarnProperty('yarn.nodemanager.linux-container-executor.cgroups.mount-path', '/cgroup')
        else:
          putYarnProperty('yarn.nodemanager.container-executor.class', 'org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor')
          putYarnPropertyAttribute('yarn.nodemanager.container-executor.resources-handler.class', 'delete', 'true')
          putYarnPropertyAttribute('yarn.nodemanager.container-executor.cgroups.hierarchy', 'delete', 'true')
          putYarnPropertyAttribute('yarn.nodemanager.container-executor.cgroups.mount', 'delete', 'true')
          putYarnPropertyAttribute('yarn.nodemanager.linux-container-executor.cgroups.mount-path', 'delete', 'true')

  def recommendHDFSConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendHDFSConfigurations(configurations, clusterData, services, hosts)
    putHdfsSiteProperty = self.putProperty(configurations, "hdfs-site", services)
    putHdfsSitePropertyAttribute = self.putPropertyAttribute(configurations, "hdfs-site")
    putHdfsSiteProperty("dfs.datanode.max.transfer.threads", 16384 if clusterData["hBaseInstalled"] else 4096)

    dataDirsCount = 1
    # Use users 'dfs.datanode.data.dir' first
    if "hdfs-site" in services["configurations"] and "dfs.datanode.data.dir" in services["configurations"]["hdfs-site"]["properties"]:
      dataDirsCount = len(str(services["configurations"]["hdfs-site"]["properties"]["dfs.datanode.data.dir"]).split(","))
    elif "dfs.datanode.data.dir" in configurations["hdfs-site"]["properties"]:
      dataDirsCount = len(str(configurations["hdfs-site"]["properties"]["dfs.datanode.data.dir"]).split(","))
    if dataDirsCount <= 2:
      failedVolumesTolerated = 0
    elif dataDirsCount <= 4:
      failedVolumesTolerated = 1
    else:
      failedVolumesTolerated = 2
    putHdfsSiteProperty("dfs.datanode.failed.volumes.tolerated", failedVolumesTolerated)

    namenodeHosts = self.getHostsWithComponent("HDFS", "NAMENODE", services, hosts)

    # 25 * # of cores on NameNode
    nameNodeCores = 4
    if namenodeHosts is not None and len(namenodeHosts):
      nameNodeCores = int(namenodeHosts[0]['Hosts']['cpu_count'])
    putHdfsSiteProperty("dfs.namenode.handler.count", 25 * nameNodeCores)
    if 25 * nameNodeCores > 200:
      putHdfsSitePropertyAttribute("dfs.namenode.handler.count", "maximum", 25 * nameNodeCores)

    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ('ranger-hdfs-plugin-properties' in services['configurations']) and ('ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']):
      rangerPluginEnabled = services['configurations']['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']
      if ("RANGER" in servicesList) and (rangerPluginEnabled.lower() == 'Yes'.lower()):
        putHdfsSiteProperty("dfs.permissions.enabled",'true')

    putHdfsSiteProperty("dfs.namenode.safemode.threshold-pct", "0.999" if len(namenodeHosts) > 1 else "1.000")

    putHdfsEnvProperty = self.putProperty(configurations, "hadoop-env", services)
    putHdfsEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hadoop-env")

    putHdfsEnvProperty('namenode_heapsize', max(int(clusterData['totalAvailableRam'] / 2), 1024))

    nn_heapsize_limit = None
    if (namenodeHosts is not None and len(namenodeHosts) > 0):
      if len(namenodeHosts) > 1:
        nn_max_heapsize = min(int(namenodeHosts[0]["Hosts"]["total_mem"]), int(namenodeHosts[1]["Hosts"]["total_mem"])) / 1024
        masters_at_host = max(self.getHostComponentsByCategories(namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"], services, hosts),
                              self.getHostComponentsByCategories(namenodeHosts[1]["Hosts"]["host_name"], ["MASTER"], services, hosts))
      else:
        nn_max_heapsize = int(namenodeHosts[0]["Hosts"]["total_mem"] / 1024) # total_mem in kb
        masters_at_host = self.getHostComponentsByCategories(namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"], services, hosts)

      putHdfsEnvPropertyAttribute('namenode_heapsize', 'maximum', max(nn_max_heapsize, 1024))

      nn_heapsize_limit = nn_max_heapsize
      nn_heapsize_limit -= clusterData["reservedRam"]
      if len(masters_at_host) > 1:
        nn_heapsize_limit = int(nn_heapsize_limit/2)

      putHdfsEnvProperty('namenode_heapsize', max(nn_heapsize_limit, 1024))


    datanodeHosts = self.getHostsWithComponent("HDFS", "DATANODE", services, hosts)
    if datanodeHosts is not None and len(datanodeHosts) > 0:
      min_datanode_ram_kb = 1073741824 # 1 TB
      for datanode in datanodeHosts:
        ram_kb = datanode['Hosts']['total_mem']
        min_datanode_ram_kb = min(min_datanode_ram_kb, ram_kb)

      datanodeFilesM = len(datanodeHosts)*dataDirsCount/10 # in millions, # of files = # of disks * 100'000
      nn_memory_configs = [
        {'nn_heap':1024,  'nn_opt':128},
        {'nn_heap':3072,  'nn_opt':512},
        {'nn_heap':5376,  'nn_opt':768},
        {'nn_heap':9984,  'nn_opt':1280},
        {'nn_heap':14848, 'nn_opt':2048},
        {'nn_heap':19456, 'nn_opt':2560},
        {'nn_heap':24320, 'nn_opt':3072},
        {'nn_heap':33536, 'nn_opt':4352},
        {'nn_heap':47872, 'nn_opt':6144},
        {'nn_heap':59648, 'nn_opt':7680},
        {'nn_heap':71424, 'nn_opt':8960},
        {'nn_heap':94976, 'nn_opt':8960}
      ]
      index = {
        datanodeFilesM < 1 : 0,
        1 <= datanodeFilesM < 5 : 1,
        5 <= datanodeFilesM < 10 : 2,
        10 <= datanodeFilesM < 20 : 3,
        20 <= datanodeFilesM < 30 : 4,
        30 <= datanodeFilesM < 40 : 5,
        40 <= datanodeFilesM < 50 : 6,
        50 <= datanodeFilesM < 70 : 7,
        70 <= datanodeFilesM < 100 : 8,
        100 <= datanodeFilesM < 125 : 9,
        125 <= datanodeFilesM < 150 : 10,
        150 <= datanodeFilesM : 11
      }[1]

      nn_memory_config = nn_memory_configs[index]

      #override with new values if applicable
      if nn_heapsize_limit is not None and nn_memory_config['nn_heap'] <= nn_heapsize_limit:
        putHdfsEnvProperty('namenode_heapsize', nn_memory_config['nn_heap'])

      putHdfsEnvPropertyAttribute('dtnode_heapsize', 'maximum', int(min_datanode_ram_kb/1024))

    nn_heapsize = int(configurations["hadoop-env"]["properties"]["namenode_heapsize"])
    putHdfsEnvProperty('namenode_opt_newsize', max(int(nn_heapsize / 8), 128))
    putHdfsEnvProperty('namenode_opt_maxnewsize', max(int(nn_heapsize / 8), 128))

    putHdfsSitePropertyAttribute = self.putPropertyAttribute(configurations, "hdfs-site")
    putHdfsSitePropertyAttribute('dfs.datanode.failed.volumes.tolerated', 'maximum', dataDirsCount)

    keyserverHostsString = None
    keyserverPortString = None
    if "hadoop-env" in services["configurations"] and "keyserver_host" in services["configurations"]["hadoop-env"]["properties"] and "keyserver_port" in services["configurations"]["hadoop-env"]["properties"]:
      keyserverHostsString = services["configurations"]["hadoop-env"]["properties"]["keyserver_host"]
      keyserverPortString = services["configurations"]["hadoop-env"]["properties"]["keyserver_port"]

    # Irrespective of what hadoop-env has, if Ranger-KMS is installed, we use its values.
    rangerKMSServerHosts = self.getHostsWithComponent("RANGER_KMS", "RANGER_KMS_SERVER", services, hosts)
    if rangerKMSServerHosts is not None and len(rangerKMSServerHosts) > 0:
      rangerKMSServerHostsArray = []
      for rangeKMSServerHost in rangerKMSServerHosts:
        rangerKMSServerHostsArray.append(rangeKMSServerHost["Hosts"]["host_name"])
      keyserverHostsString = ";".join(rangerKMSServerHostsArray)
      if "kms-env" in services["configurations"] and "kms_port" in services["configurations"]["kms-env"]["properties"]:
        keyserverPortString = services["configurations"]["kms-env"]["properties"]["kms_port"]

    if keyserverHostsString is not None and len(keyserverHostsString.strip()) > 0:
      urlScheme = "http"
      if "ranger-kms-site" in services["configurations"] and \
                      "ranger.service.https.attrib.ssl.enabled" in services["configurations"]["ranger-kms-site"]["properties"] and \
                      services["configurations"]["ranger-kms-site"]["properties"]["ranger.service.https.attrib.ssl.enabled"].lower() == "true":
        urlScheme = "https"

      if keyserverPortString is None or len(keyserverPortString.strip()) < 1:
        keyserverPortString = ":9292"
      else:
        keyserverPortString = ":" + keyserverPortString.strip()
      putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
      kmsPath = "kms://" + urlScheme + "@" + keyserverHostsString.strip() + keyserverPortString + "/kms"
      putCoreSiteProperty("hadoop.security.key.provider.path", kmsPath)
      putHdfsSiteProperty("dfs.encryption.key.provider.uri", kmsPath)

    if "ranger-env" in services["configurations"] and "ranger-hdfs-plugin-properties" in services["configurations"] and \
                    "ranger-hdfs-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putHdfsRangerPluginProperty = self.putProperty(configurations, "ranger-hdfs-plugin-properties", services)
      rangerEnvHdfsPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hdfs-plugin-enabled"]
      putHdfsRangerPluginProperty("ranger-hdfs-plugin-enabled", rangerEnvHdfsPluginProperty)

  def recommendHIVEConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendHiveConfigurations(configurations, clusterData, services, hosts)

    putHiveServerProperty = self.putProperty(configurations, "hiveserver2-site", services)
    putHiveEnvProperty = self.putProperty(configurations, "hive-env", services)
    putHiveSiteProperty = self.putProperty(configurations, "hive-site", services)
    putHiveSitePropertyAttribute = self.putPropertyAttribute(configurations, "hive-site")
    putHiveEnvPropertyAttributes = self.putPropertyAttribute(configurations, "hive-env")
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

    #  Storage
    putHiveEnvProperty("hive_exec_orc_storage_strategy", "SPEED")
    putHiveSiteProperty("hive.exec.orc.encoding.strategy", configurations["hive-env"]["properties"]["hive_exec_orc_storage_strategy"])
    putHiveSiteProperty("hive.exec.orc.compression.strategy", configurations["hive-env"]["properties"]["hive_exec_orc_storage_strategy"])

    putHiveSiteProperty("hive.exec.orc.default.stripe.size", "67108864")
    putHiveSiteProperty("hive.exec.orc.default.compress", "ZLIB")
    putHiveSiteProperty("hive.optimize.index.filter", "true")
    putHiveSiteProperty("hive.optimize.sort.dynamic.partition", "false")

    # Vectorization
    putHiveSiteProperty("hive.vectorized.execution.enabled", "true")
    putHiveSiteProperty("hive.vectorized.execution.reduce.enabled", "false")

    # Transactions
    putHiveEnvProperty("hive_txn_acid", "off")
    if str(configurations["hive-env"]["properties"]["hive_txn_acid"]).lower() == "on":
      putHiveSiteProperty("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
      putHiveSiteProperty("hive.support.concurrency", "true")
      putHiveSiteProperty("hive.compactor.initiator.on", "true")
      putHiveSiteProperty("hive.compactor.worker.threads", "1")
      putHiveSiteProperty("hive.enforce.bucketing", "true")
      putHiveSiteProperty("hive.exec.dynamic.partition.mode", "nonstrict")
    else:
      putHiveSiteProperty("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager")
      putHiveSiteProperty("hive.support.concurrency", "false")
      putHiveSiteProperty("hive.compactor.initiator.on", "false")
      putHiveSiteProperty("hive.compactor.worker.threads", "0")
      putHiveSiteProperty("hive.enforce.bucketing", "false")
      putHiveSiteProperty("hive.exec.dynamic.partition.mode", "strict")

    # ATS
    putHiveEnvProperty("hive_timeline_logging_enabled", "true")

    hooks_properties = ["hive.exec.pre.hooks", "hive.exec.post.hooks", "hive.exec.failure.hooks"]
    include_ats_hook = str(configurations["hive-env"]["properties"]["hive_timeline_logging_enabled"]).lower() == "true"

    ats_hook_class = "org.apache.hadoop.hive.ql.hooks.ATSHook"
    for hooks_property in hooks_properties:
      if hooks_property in configurations["hive-site"]["properties"]:
        hooks_value = configurations["hive-site"]["properties"][hooks_property]
      else:
        hooks_value = " "
      if include_ats_hook and ats_hook_class not in hooks_value:
        if hooks_value == " ":
          hooks_value = ats_hook_class
        else:
          hooks_value = hooks_value + "," + ats_hook_class
      if not include_ats_hook and ats_hook_class in hooks_value:
        hooks_classes = []
        for hook_class in hooks_value.split(","):
          if hook_class != ats_hook_class and hook_class != " ":
            hooks_classes.append(hook_class)
        if hooks_classes:
          hooks_value = ",".join(hooks_classes)
        else:
          hooks_value = " "

      putHiveSiteProperty(hooks_property, hooks_value)

    # Tez Engine
    if "TEZ" in servicesList:
      putHiveSiteProperty("hive.execution.engine", "tez")
    else:
      putHiveSiteProperty("hive.execution.engine", "mr")

    container_size = "512"

    if not "yarn-site" in configurations:
      self.recommendYARNConfigurations(configurations, clusterData, services, hosts)
    #properties below should be always present as they are provided in HDP206 stack advisor at least
    yarnMaxAllocationSize = min(30 * int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]), int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]))
    #duplicate tez task resource calc logic, direct dependency doesn't look good here (in case of Hive without Tez)
    container_size = clusterData['mapMemory'] if clusterData['mapMemory'] > 2048 else int(clusterData['reduceMemory'])
    container_size = min(clusterData['containers'] * clusterData['ramPerContainer'], container_size, yarnMaxAllocationSize)

    putHiveSiteProperty("hive.tez.container.size", min(int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]), container_size))
    putHiveSiteProperty("hive.prewarm.enabled", "false")
    putHiveSiteProperty("hive.prewarm.numcontainers", "3")
    putHiveSiteProperty("hive.tez.auto.reducer.parallelism", "true")
    putHiveSiteProperty("hive.tez.dynamic.partition.pruning", "true")

    container_size = configurations["hive-site"]["properties"]["hive.tez.container.size"]
    container_size_bytes = int(container_size)*1024*1024
    # Memory
    putHiveSiteProperty("hive.auto.convert.join.noconditionaltask.size", int(round(container_size_bytes/3)))
    putHiveSitePropertyAttribute("hive.auto.convert.join.noconditionaltask.size", "maximum", container_size_bytes)
    putHiveSiteProperty("hive.exec.reducers.bytes.per.reducer", "67108864")

    # CBO
    putHiveEnvProperty("cost_based_optimizer", "On")
    if str(configurations["hive-env"]["properties"]["cost_based_optimizer"]).lower() == "on":
      putHiveSiteProperty("hive.cbo.enable", "true")
    else:
      putHiveSiteProperty("hive.cbo.enable", "false")
    hive_cbo_enable = configurations["hive-site"]["properties"]["hive.cbo.enable"]
    putHiveSiteProperty("hive.stats.fetch.partition.stats", hive_cbo_enable)
    putHiveSiteProperty("hive.stats.fetch.column.stats", hive_cbo_enable)
    putHiveSiteProperty("hive.compute.query.using.stats", "true")

    # Interactive Query
    putHiveSiteProperty("hive.server2.tez.initialize.default.sessions", "false")
    putHiveSiteProperty("hive.server2.tez.sessions.per.default.queue", "1")
    putHiveSiteProperty("hive.server2.enable.doAs", "true")

    yarn_queues = "default"
    capacitySchedulerProperties = {}
    if "capacity-scheduler" in services['configurations'] and "capacity-scheduler" in services['configurations']["capacity-scheduler"]["properties"]:
      properties = str(services['configurations']["capacity-scheduler"]["properties"]["capacity-scheduler"]).split('\n')
      for property in properties:
        key,sep,value = property.partition("=")
        capacitySchedulerProperties[key] = value
    if "yarn.scheduler.capacity.root.queues" in capacitySchedulerProperties:
      yarn_queues = str(capacitySchedulerProperties["yarn.scheduler.capacity.root.queues"])
    # Interactive Queues property attributes
    putHiveServerPropertyAttribute = self.putPropertyAttribute(configurations, "hiveserver2-site")
    toProcessQueues = yarn_queues.split(",")
    leafQueues = []
    while len(toProcessQueues) > 0:
      queue = toProcessQueues.pop()
      queueKey = "yarn.scheduler.capacity.root." + queue + ".queues"
      if queueKey in capacitySchedulerProperties:
        # This is a parent queue - need to add children
        subQueues = capacitySchedulerProperties[queueKey].split(",")
        for subQueue in subQueues:
          toProcessQueues.append(queue + "." + subQueue)
      else:
        # This is a leaf queue
        leafQueues.append({"label": str(queue) + " queue", "value": queue})
    leafQueues = sorted(leafQueues, key=lambda q:q['value'])
    putHiveSitePropertyAttribute("hive.server2.tez.default.queues", "entries", leafQueues)
    putHiveSiteProperty("hive.server2.tez.default.queues", ",".join([leafQueue['value'] for leafQueue in leafQueues]))


    # Recommend Ranger Hive authorization as per Ranger Hive plugin property
    if "ranger-env" in services["configurations"] and "hive-env" in services["configurations"] and \
                    "ranger-hive-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hive-plugin-enabled"]
      if (rangerEnvHivePluginProperty.lower() == "yes"):
        putHiveEnvProperty("hive_security_authorization", "RANGER")

    # Security
    if ("configurations" not in services) or ("hive-env" not in services["configurations"]) or \
            ("properties" not in services["configurations"]["hive-env"]) or \
            ("hive_security_authorization" not in services["configurations"]["hive-env"]["properties"]) or \
                    str(services["configurations"]["hive-env"]["properties"]["hive_security_authorization"]).lower() == "none":
      putHiveEnvProperty("hive_security_authorization", "None")
    else:
      putHiveEnvProperty("hive_security_authorization", services["configurations"]["hive-env"]["properties"]["hive_security_authorization"])


    # Recommend Ranger Hive authorization as per Ranger Hive plugin property
    if "ranger-env" in services["configurations"] and "hive-env" in services["configurations"] and \
                    "ranger-hive-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hive-plugin-enabled"]
      rangerEnvHiveAuthProperty = services["configurations"]["hive-env"]["properties"]["hive_security_authorization"]
      if (rangerEnvHivePluginProperty.lower() == "yes"):
        putHiveEnvProperty("hive_security_authorization", "Ranger")
      elif (rangerEnvHiveAuthProperty.lower() == "ranger"):
        putHiveEnvProperty("hive_security_authorization", "None")

    # hive_security_authorization == 'none'
    # this property is unrelated to Kerberos
    if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "none":
      putHiveSiteProperty("hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory")
      if ("hive.security.authorization.manager" in configurations["hiveserver2-site"]["properties"]) or \
              ("hiveserver2-site" not in services["configurations"]) or \
              ("hiveserver2-site" in services["configurations"] and "hive.security.authorization.manager" in services["configurations"]["hiveserver2-site"]["properties"]):
        putHiveServerPropertyAttribute("hive.security.authorization.manager", "delete", "true")
      if ("hive.security.authenticator.manager" in configurations["hiveserver2-site"]["properties"]) or \
              ("hiveserver2-site" not in services["configurations"]) or \
              ("hiveserver2-site" in services["configurations"] and "hive.security.authenticator.manager" in services["configurations"]["hiveserver2-site"]["properties"]):
        putHiveServerPropertyAttribute("hive.security.authenticator.manager", "delete", "true")
      if ("hive.conf.restricted.list" in configurations["hiveserver2-site"]["properties"]) or \
              ("hiveserver2-site" not in services["configurations"]) or \
              ("hiveserver2-site" in services["configurations"] and "hive.conf.restricted.list" in services["configurations"]["hiveserver2-site"]["properties"]):
        putHiveServerPropertyAttribute("hive.conf.restricted.list", "delete", "true")
      if "KERBEROS" not in servicesList: # Kerberos security depends on this property
        putHiveSiteProperty("hive.security.authorization.enabled", "false")
    else:
      putHiveSiteProperty("hive.security.authorization.enabled", "true")

    try:
      auth_manager_value = str(configurations["hive-env"]["properties"]["hive.security.metastore.authorization.manager"])
    except KeyError:
      auth_manager_value = 'org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider'
      pass
    auth_manager_values = auth_manager_value.split(",")
    sqlstdauth_class = "org.apache.hadoop.hive.ql.security.authorization.MetaStoreAuthzAPIAuthorizerEmbedOnly"

    putHiveSiteProperty("hive.server2.enable.doAs", "true")

    # hive_security_authorization == 'sqlstdauth'
    if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "sqlstdauth":
      putHiveSiteProperty("hive.server2.enable.doAs", "false")
      putHiveServerProperty("hive.security.authorization.enabled", "true")
      putHiveServerProperty("hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory")
      putHiveServerProperty("hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
      putHiveServerProperty("hive.conf.restricted.list", "hive.security.authenticator.manager,hive.security.authorization.manager,hive.users.in.admin.role")
      putHiveSiteProperty("hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory")
      if sqlstdauth_class not in auth_manager_values:
        auth_manager_values.append(sqlstdauth_class)
    elif sqlstdauth_class in auth_manager_values:
      #remove item from csv
      auth_manager_values = [x for x in auth_manager_values if x != sqlstdauth_class]
      pass
    putHiveSiteProperty("hive.security.metastore.authorization.manager", ",".join(auth_manager_values))

    # hive_security_authorization == 'ranger'
    if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "ranger":
      putHiveSiteProperty("hive.server2.enable.doAs", "false")
      putHiveServerProperty("hive.security.authorization.enabled", "true")
      putHiveServerProperty("hive.security.authorization.manager", "com.xasecure.authorization.hive.authorizer.XaSecureHiveAuthorizerFactory")
      putHiveServerProperty("hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
      putHiveServerProperty("hive.conf.restricted.list", "hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager")

    putHiveSiteProperty("hive.server2.use.SSL", "false")

    #Hive authentication
    hive_server2_auth = None
    if "hive-site" in services["configurations"] and "hive.server2.authentication" in services["configurations"]["hive-site"]["properties"]:
      hive_server2_auth = str(services["configurations"]["hive-site"]["properties"]["hive.server2.authentication"]).lower()
    elif "hive.server2.authentication" in configurations["hive-site"]["properties"]:
      hive_server2_auth = str(configurations["hive-site"]["properties"]["hive.server2.authentication"]).lower()

    if hive_server2_auth == "ldap":
      putHiveSiteProperty("hive.server2.authentication.ldap.url", "")
    else:
      if ("hive.server2.authentication.ldap.url" in configurations["hive-site"]["properties"]) or \
              ("hive-site" not in services["configurations"]) or \
              ("hive-site" in services["configurations"] and "hive.server2.authentication.ldap.url" in services["configurations"]["hive-site"]["properties"]):
        putHiveSitePropertyAttribute("hive.server2.authentication.ldap.url", "delete", "true")

    if hive_server2_auth == "kerberos":
      putHiveSiteProperty("hive.server2.authentication.kerberos.keytab", "")
      putHiveSiteProperty("hive.server2.authentication.kerberos.principal", "")
    elif "KERBEROS" not in servicesList: # Since 'hive_server2_auth' cannot be relied on within the default, empty recommendations request
      if ("hive.server2.authentication.kerberos.keytab" in configurations["hive-site"]["properties"]) or \
              ("hive-site" not in services["configurations"]) or \
              ("hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.keytab" in services["configurations"]["hive-site"]["properties"]):
        putHiveSitePropertyAttribute("hive.server2.authentication.kerberos.keytab", "delete", "true")
      if ("hive.server2.authentication.kerberos.principal" in configurations["hive-site"]["properties"]) or \
              ("hive-site" not in services["configurations"]) or \
              ("hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.principal" in services["configurations"]["hive-site"]["properties"]):
        putHiveSitePropertyAttribute("hive.server2.authentication.kerberos.principal", "delete", "true")

    if hive_server2_auth == "pam":
      putHiveSiteProperty("hive.server2.authentication.pam.services", "")
    else:
      if ("hive.server2.authentication.pam.services" in configurations["hive-site"]["properties"]) or \
              ("hive-site" not in services["configurations"]) or \
              ("hive-site" in services["configurations"] and "hive.server2.authentication.pam.services" in services["configurations"]["hive-site"]["properties"]):
        putHiveSitePropertyAttribute("hive.server2.authentication.pam.services", "delete", "true")

    if hive_server2_auth == "custom":
      putHiveSiteProperty("hive.server2.custom.authentication.class", "")
    else:
      if ("hive.server2.authentication" in configurations["hive-site"]["properties"]) or \
              ("hive-site" not in services["configurations"]) or \
              ("hive-site" in services["configurations"] and "hive.server2.custom.authentication.class" in services["configurations"]["hive-site"]["properties"]):
        putHiveSitePropertyAttribute("hive.server2.custom.authentication.class", "delete", "true")

    # HiveServer, Client, Metastore heapsize
    hs_heapsize_multiplier = 3.0/8
    hm_heapsize_multiplier = 1.0/8
    # HiveServer2 and HiveMetastore located on the same host
    hive_server_hosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER", services, hosts)
    hive_client_hosts = self.getHostsWithComponent("HIVE", "HIVE_CLIENT", services, hosts)

    if hive_server_hosts is not None and len(hive_server_hosts):
      hs_host_ram = hive_server_hosts[0]["Hosts"]["total_mem"]/1024
      putHiveEnvProperty("hive.metastore.heapsize", max(512, int(hs_host_ram*hm_heapsize_multiplier)))
      putHiveEnvProperty("hive.heapsize", max(512, int(hs_host_ram*hs_heapsize_multiplier)))
      putHiveEnvPropertyAttributes("hive.metastore.heapsize", "maximum", max(1024, hs_host_ram))
      putHiveEnvPropertyAttributes("hive.heapsize", "maximum", max(1024, hs_host_ram))

    if hive_client_hosts is not None and len(hive_client_hosts):
      putHiveEnvProperty("hive.client.heapsize", 1024)
      putHiveEnvPropertyAttributes("hive.client.heapsize", "maximum", max(1024, int(hive_client_hosts[0]["Hosts"]["total_mem"]/1024)))


  def recommendHBASEConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendHbaseConfigurations(configurations, clusterData, services, hosts)
    putHbaseEnvPropertyAttributes = self.putPropertyAttribute(configurations, "hbase-env")

    hmaster_host = self.getHostWithComponent("HBASE", "HBASE_MASTER", services, hosts)
    if hmaster_host is not None:
      host_ram = hmaster_host["Hosts"]["total_mem"]
      putHbaseEnvPropertyAttributes('hbase_master_heapsize', 'maximum', max(1024, int(host_ram/1024)))

    rs_hosts = self.getHostsWithComponent("HBASE", "HBASE_REGIONSERVER", services, hosts)
    if rs_hosts is not None and len(rs_hosts) > 0:
      min_ram = rs_hosts[0]["Hosts"]["total_mem"]
      for host in rs_hosts:
        host_ram = host["Hosts"]["total_mem"]
        min_ram = min(min_ram, host_ram)

      putHbaseEnvPropertyAttributes('hbase_regionserver_heapsize', 'maximum', max(1024, int(min_ram*0.8/1024)))

    putHbaseSiteProperty = self.putProperty(configurations, "hbase-site", services)
    putHbaseSitePropertyAttributes = self.putPropertyAttribute(configurations, "hbase-site")
    putHbaseSiteProperty("hbase.regionserver.global.memstore.size", '0.4')

    if 'hbase-env' in services['configurations'] and 'phoenix_sql_enabled' in services['configurations']['hbase-env']['properties'] and \
                    'true' == services['configurations']['hbase-env']['properties']['phoenix_sql_enabled'].lower():
      putHbaseSiteProperty("hbase.regionserver.wal.codec", 'org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec')
      putHbaseSiteProperty("phoenix.functions.allowUserDefinedFunctions", 'true')
    else:
      putHbaseSiteProperty("hbase.regionserver.wal.codec", 'org.apache.hadoop.hbase.regionserver.wal.WALCellCodec')
      if ('hbase.rpc.controllerfactory.class' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'hbase.rpc.controllerfactory.class' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('hbase.rpc.controllerfactory.class', 'delete', 'true')
      if ('phoenix.functions.allowUserDefinedFunctions' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'phoenix.functions.allowUserDefinedFunctions' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('phoenix.functions.allowUserDefinedFunctions', 'delete', 'true')

    if "ranger-env" in services["configurations"] and "ranger-hbase-plugin-properties" in services["configurations"] and \
                    "ranger-hbase-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putHbaseRangerPluginProperty = self.putProperty(configurations, "ranger-hbase-plugin-properties", services)
      rangerEnvHbasePluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-hbase-plugin-enabled"]
      putHbaseRangerPluginProperty("ranger-hbase-plugin-enabled", rangerEnvHbasePluginProperty)

    rangerPluginEnabled = ''
    if 'ranger-hbase-plugin-properties' in configurations and 'ranger-hbase-plugin-enabled' in  configurations['ranger-hbase-plugin-properties']['properties']:
      rangerPluginEnabled = configurations['ranger-hbase-plugin-properties']['properties']['ranger-hbase-plugin-enabled']
    elif 'ranger-hbase-plugin-properties' in services['configurations'] and 'ranger-hbase-plugin-enabled' in services['configurations']['ranger-hbase-plugin-properties']['properties']:
      rangerPluginEnabled = services['configurations']['ranger-hbase-plugin-properties']['properties']['ranger-hbase-plugin-enabled']

    if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
      putHbaseSiteProperty('hbase.security.authorization','true')

    # Recommend configs for bucket cache
    threshold = 23 # 2 Gb is reserved for other offheap memory
    mb = 1024
    if (int(clusterData["hbaseRam"]) > threshold):
      # To enable cache - calculate values
      regionserver_total_ram = int(clusterData["hbaseRam"]) * mb
      regionserver_heap_size = 20480
      regionserver_max_direct_memory_size = regionserver_total_ram - regionserver_heap_size
      hfile_block_cache_size = '0.4'
      block_cache_heap = 8192 # int(regionserver_heap_size * hfile_block_cache_size)
      hbase_regionserver_global_memstore_size = '0.4'
      reserved_offheap_memory = 2048
      bucketcache_offheap_memory = regionserver_max_direct_memory_size - reserved_offheap_memory
      hbase_bucketcache_size = block_cache_heap + bucketcache_offheap_memory
      hbase_bucketcache_percentage_in_combinedcache = float(bucketcache_offheap_memory) / hbase_bucketcache_size
      hbase_bucketcache_percentage_in_combinedcache_str = "{0:.4f}".format(math.ceil(hbase_bucketcache_percentage_in_combinedcache * 10000) / 10000.0)

      # Set values in hbase-site
      putHbaseSiteProperty('hfile.block.cache.size', hfile_block_cache_size)
      putHbaseSiteProperty('hbase.regionserver.global.memstore.size', hbase_regionserver_global_memstore_size)
      putHbaseSiteProperty('hbase.bucketcache.ioengine', 'offheap')
      putHbaseSiteProperty('hbase.bucketcache.size', hbase_bucketcache_size)
      putHbaseSiteProperty('hbase.bucketcache.percentage.in.combinedcache', hbase_bucketcache_percentage_in_combinedcache_str)

      # Enable in hbase-env
      putHbaseEnvProperty = self.putProperty(configurations, "hbase-env", services)
      putHbaseEnvProperty('hbase_max_direct_memory_size', regionserver_max_direct_memory_size)
      putHbaseEnvProperty('hbase_regionserver_heapsize', regionserver_heap_size)
    else:
      # Disable
      if ('hbase.bucketcache.ioengine' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'hbase.bucketcache.ioengine' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('hbase.bucketcache.ioengine', 'delete', 'true')
      if ('hbase.bucketcache.size' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'hbase.bucketcache.size' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('hbase.bucketcache.size', 'delete', 'true')
      if ('hbase.bucketcache.percentage.in.combinedcache' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'hbase.bucketcache.percentage.in.combinedcache' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('hbase.bucketcache.percentage.in.combinedcache', 'delete', 'true')
      if ('hbase_max_direct_memory_size' in configurations["hbase-env"]["properties"]) or \
              ('hbase-env' in services['configurations'] and 'hbase_max_direct_memory_size' in services['configurations']["hbase-env"]["properties"]):
        putHbaseEnvPropertyAttributes('hbase_max_direct_memory_size', 'delete', 'true')

    # Authorization
    hbaseCoProcessorConfigs = {
      'hbase.coprocessor.region.classes': [],
      'hbase.coprocessor.regionserver.classes': [],
      'hbase.coprocessor.master.classes': []
    }
    for key in hbaseCoProcessorConfigs:
      hbase_coprocessor_classes = None
      if key in configurations["hbase-site"]["properties"]:
        hbase_coprocessor_classes = configurations["hbase-site"]["properties"][key].strip()
      elif 'hbase-site' in services['configurations'] and key in services['configurations']["hbase-site"]["properties"]:
        hbase_coprocessor_classes = services['configurations']["hbase-site"]["properties"][key].strip()
      if hbase_coprocessor_classes:
        hbaseCoProcessorConfigs[key] = hbase_coprocessor_classes.split(',')

    # If configurations has it - it has priority as it is calculated. Then, the service's configurations will be used.
    hbase_security_authorization = None
    if 'hbase-site' in configurations and 'hbase.security.authorization' in configurations['hbase-site']['properties']:
      hbase_security_authorization = configurations['hbase-site']['properties']['hbase.security.authorization']
    elif 'hbase-site' in services['configurations'] and 'hbase.security.authorization' in services['configurations']['hbase-site']['properties']:
      hbase_security_authorization = services['configurations']['hbase-site']['properties']['hbase.security.authorization']
    if hbase_security_authorization:
      if 'true' == hbase_security_authorization.lower():
        hbaseCoProcessorConfigs['hbase.coprocessor.master.classes'].append('org.apache.hadoop.hbase.security.access.AccessController')
        hbaseCoProcessorConfigs['hbase.coprocessor.regionserver.classes'].append('org.apache.hadoop.hbase.security.access.AccessController')
        # regional classes when hbase authorization is enabled
        authRegionClasses = ['org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint', 'org.apache.hadoop.hbase.security.access.AccessController']
        for item in range(len(authRegionClasses)):
          hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append(authRegionClasses[item])
      else:
        if 'org.apache.hadoop.hbase.security.access.AccessController' in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
          hbaseCoProcessorConfigs['hbase.coprocessor.master.classes'].remove('org.apache.hadoop.hbase.security.access.AccessController')
        hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint")
        if ('hbase.coprocessor.regionserver.classes' in configurations["hbase-site"]["properties"]) or \
                ('hbase-site' in services['configurations'] and 'hbase.coprocessor.regionserver.classes' in services['configurations']["hbase-site"]["properties"]):
          putHbaseSitePropertyAttributes('hbase.coprocessor.regionserver.classes', 'delete', 'true')
    else:
      hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint")
      if ('hbase.coprocessor.regionserver.classes' in configurations["hbase-site"]["properties"]) or \
              ('hbase-site' in services['configurations'] and 'hbase.coprocessor.regionserver.classes' in services['configurations']["hbase-site"]["properties"]):
        putHbaseSitePropertyAttributes('hbase.coprocessor.regionserver.classes', 'delete', 'true')

    # Authentication
    if 'hbase-site' in services['configurations'] and 'hbase.security.authentication' in services['configurations']['hbase-site']['properties']:
      if 'kerberos' == services['configurations']['hbase-site']['properties']['hbase.security.authentication'].lower():
        if 'org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint' not in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
          hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append('org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint')
        if 'org.apache.hadoop.hbase.security.token.TokenProvider' not in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
          hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append('org.apache.hadoop.hbase.security.token.TokenProvider')
      else:
        if 'org.apache.hadoop.hbase.security.token.TokenProvider' in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
          hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].remove('org.apache.hadoop.hbase.security.token.TokenProvider')

    #Remove duplicates
    for key in hbaseCoProcessorConfigs:
      uniqueCoprocessorRegionClassList = []
      [uniqueCoprocessorRegionClassList.append(i)
       for i in hbaseCoProcessorConfigs[key] if
       not i in uniqueCoprocessorRegionClassList
       and (i.strip() not in ['{{hbase_coprocessor_region_classes}}', '{{hbase_coprocessor_master_classes}}', '{{hbase_coprocessor_regionserver_classes}}'])]
      putHbaseSiteProperty(key, ','.join(set(uniqueCoprocessorRegionClassList)))


    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    rangerServiceVersion=''
    if 'RANGER' in servicesList:
      rangerServiceVersion = [service['StackServices']['service_version'] for service in services["services"] if service['StackServices']['service_name'] == 'RANGER'][0]

    if rangerServiceVersion and rangerServiceVersion == '0.4.0':
      rangerClass = 'com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor'
    else:
      rangerClass = 'org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor'

    nonRangerClass = 'org.apache.hadoop.hbase.security.access.AccessController'
    hbaseClassConfigs =  hbaseCoProcessorConfigs.keys()

    for item in range(len(hbaseClassConfigs)):
      if 'hbase-site' in services['configurations']:
        if hbaseClassConfigs[item] in services['configurations']['hbase-site']['properties']:
          if 'hbase-site' in configurations and hbaseClassConfigs[item] in configurations['hbase-site']['properties']:
            coprocessorConfig = configurations['hbase-site']['properties'][hbaseClassConfigs[item]]
          else:
            coprocessorConfig = services['configurations']['hbase-site']['properties'][hbaseClassConfigs[item]]
          coprocessorClasses = coprocessorConfig.split(",")
          coprocessorClasses = filter(None, coprocessorClasses) # Removes empty string elements from array
          if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
            if nonRangerClass in coprocessorClasses:
              coprocessorClasses.remove(nonRangerClass)
            if not rangerClass in coprocessorClasses:
              coprocessorClasses.append(rangerClass)
            putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
          elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'No'.lower():
            if rangerClass in coprocessorClasses:
              coprocessorClasses.remove(rangerClass)
              if not nonRangerClass in coprocessorClasses:
                coprocessorClasses.append(nonRangerClass)
              putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
        elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
          putHbaseSiteProperty(hbaseClassConfigs[item], rangerClass)


  def recommendTezConfigurations(self, configurations, clusterData, services, hosts):
    if not "yarn-site" in configurations:
      self.recommendYARNConfigurations(configurations, clusterData, services, hosts)
    #properties below should be always present as they are provided in HDP206 stack advisor
    yarnMaxAllocationSize = min(30 * int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]), int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]))

    putTezProperty = self.putProperty(configurations, "tez-site")
    putTezProperty("tez.am.resource.memory.mb", min(int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]), int(clusterData['amMemory']) * 2 if int(clusterData['amMemory']) < 3072 else int(clusterData['amMemory'])))

    taskResourceMemory = clusterData['mapMemory'] if clusterData['mapMemory'] > 2048 else int(clusterData['reduceMemory'])
    taskResourceMemory = min(clusterData['containers'] * clusterData['ramPerContainer'], taskResourceMemory, yarnMaxAllocationSize)
    putTezProperty("tez.task.resource.memory.mb", min(int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]), taskResourceMemory))
    putTezProperty("tez.runtime.io.sort.mb", min(int(taskResourceMemory * 0.4), 2047))
    putTezProperty("tez.runtime.unordered.output.buffer.size-mb", int(taskResourceMemory * 0.075))
    putTezProperty("tez.session.am.dag.submit.timeout.secs", "600")

    serverProperties = services["ambari-server-properties"]
    latest_tez_jar_version = None

    server_host = socket.getfqdn()
    server_port = '8080'
    server_protocol = 'http'
    views_dir = '/var/lib/ambari-server/resources/views/'

    if serverProperties:
      if 'client.api.port' in serverProperties:
        server_port = serverProperties['client.api.port']
      if 'views.dir' in serverProperties:
        views_dir = serverProperties['views.dir']
      if 'api.ssl' in serverProperties:
        if serverProperties['api.ssl'].lower() == 'true':
          server_protocol = 'https'

      views_work_dir = os.path.join(views_dir, 'work')

      if os.path.exists(views_work_dir) and os.path.isdir(views_work_dir):
        last_version = '0.0.0'
        for file in os.listdir(views_work_dir):
          if fnmatch.fnmatch(file, 'TEZ{*}'):
            current_version = file.lstrip("TEZ{").rstrip("}") # E.g.: TEZ{0.7.0.2.3.0.0-2154}
            if self.versionCompare(current_version.replace("-", "."), last_version.replace("-", ".")) >= 0:
              latest_tez_jar_version = current_version
              last_version = current_version
            pass
        pass
      pass
    pass

    if latest_tez_jar_version:
      tez_url = '{0}://{1}:{2}/#/main/views/TEZ/{3}/TEZ_CLUSTER_INSTANCE'.format(server_protocol, server_host, server_port, latest_tez_jar_version)
      putTezProperty("tez.tez-ui.history-url.base", tez_url)
    pass

  def recommendStormConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendStormConfigurations(configurations, clusterData, services, hosts)
    putStormSiteProperty = self.putProperty(configurations, "storm-site", services)
    putStormSiteAttributes = self.putPropertyAttribute(configurations, "storm-site")
    storm_site = getServicesSiteProperties(services, "storm-site")
    security_enabled = (storm_site is not None and "storm.zookeeper.superACL" in storm_site)
    if "ranger-env" in services["configurations"] and "ranger-storm-plugin-properties" in services["configurations"] and \
                    "ranger-storm-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putStormRangerPluginProperty = self.putProperty(configurations, "ranger-storm-plugin-properties", services)
      rangerEnvStormPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-storm-plugin-enabled"]
      putStormRangerPluginProperty("ranger-storm-plugin-enabled", rangerEnvStormPluginProperty)

    rangerPluginEnabled = ''
    if 'ranger-storm-plugin-properties' in configurations and 'ranger-storm-plugin-enabled' in  configurations['ranger-storm-plugin-properties']['properties']:
      rangerPluginEnabled = configurations['ranger-storm-plugin-properties']['properties']['ranger-storm-plugin-enabled']
    elif 'ranger-storm-plugin-properties' in services['configurations'] and 'ranger-storm-plugin-enabled' in services['configurations']['ranger-storm-plugin-properties']['properties']:
      rangerPluginEnabled = services['configurations']['ranger-storm-plugin-properties']['properties']['ranger-storm-plugin-enabled']

    nonRangerClass = 'backtype.storm.security.auth.authorizer.SimpleACLAuthorizer'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    rangerServiceVersion=''
    if 'RANGER' in servicesList:
      rangerServiceVersion = [service['StackServices']['service_version'] for service in services["services"] if service['StackServices']['service_name'] == 'RANGER'][0]

    if rangerServiceVersion and rangerServiceVersion == '0.4.0':
      rangerClass = 'com.xasecure.authorization.storm.authorizer.XaSecureStormAuthorizer'
    else:
      rangerClass = 'org.apache.ranger.authorization.storm.authorizer.RangerStormAuthorizer'
    # Cluster is kerberized
    if security_enabled:
      if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
        putStormSiteProperty('nimbus.authorizer',rangerClass)
      elif (services["configurations"]["storm-site"]["properties"]["nimbus.authorizer"] == rangerClass):
        putStormSiteProperty('nimbus.authorizer', nonRangerClass)
    else:
      putStormSiteAttributes('nimbus.authorizer', 'delete', 'true')

  def recommendKnoxConfigurations(self, configurations, clusterData, services, hosts):
    if "ranger-env" in services["configurations"] and "ranger-knox-plugin-properties" in services["configurations"] and \
                    "ranger-knox-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putKnoxRangerPluginProperty = self.putProperty(configurations, "ranger-knox-plugin-properties", services)
      rangerEnvKnoxPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-knox-plugin-enabled"]
      putKnoxRangerPluginProperty("ranger-knox-plugin-enabled", rangerEnvKnoxPluginProperty)

    if 'topology' in services["configurations"] and 'content' in services["configurations"]["topology"]["properties"]:
      putKnoxTopologyContent = self.putProperty(configurations, "topology", services)
      rangerPluginEnabled = ''
      if 'ranger-knox-plugin-properties' in configurations and 'ranger-knox-plugin-enabled' in  configurations['ranger-knox-plugin-properties']['properties']:
        rangerPluginEnabled = configurations['ranger-knox-plugin-properties']['properties']['ranger-knox-plugin-enabled']
      elif 'ranger-knox-plugin-properties' in services['configurations'] and 'ranger-knox-plugin-enabled' in services['configurations']['ranger-knox-plugin-properties']['properties']:
        rangerPluginEnabled = services['configurations']['ranger-knox-plugin-properties']['properties']['ranger-knox-plugin-enabled']

      # check if authorization provider already added
      topologyContent = services["configurations"]["topology"]["properties"]["content"]
      authorizationProviderExists = False
      authNameChanged = False
      root = ET.fromstring(topologyContent)
      if root is not None:
        gateway = root.find("gateway")
        if gateway is not None:
          for provider in gateway.findall('provider'):
            role = provider.find('role')
            if role is not None and role.text and role.text.lower() == "authorization":
              authorizationProviderExists = True

            name = provider.find('name')
            if name is not None and name.text == "AclsAuthz" and rangerPluginEnabled \
                    and rangerPluginEnabled.lower() == "Yes".lower():
              newAuthName = "XASecurePDPKnox"
              authNameChanged = True
            elif name is not None and (((not rangerPluginEnabled) or rangerPluginEnabled.lower() != "Yes".lower()) \
                                               and name.text == 'XASecurePDPKnox'):
              newAuthName = "AclsAuthz"
              authNameChanged = True

            if authNameChanged:
              name.text = newAuthName
              putKnoxTopologyContent('content', ET.tostring(root))

            if authorizationProviderExists:
              break

      if not authorizationProviderExists:
        if root is not None:
          gateway = root.find("gateway")
          if gateway is not None:
            provider = ET.SubElement(gateway, 'provider')

            role = ET.SubElement(provider, 'role')
            role.text = "authorization"

            name = ET.SubElement(provider, 'name')
            if rangerPluginEnabled and rangerPluginEnabled.lower() == "Yes".lower():
              name.text = "XASecurePDPKnox"
            else:
              name.text = "AclsAuthz"

            enabled = ET.SubElement(provider, 'enabled')
            enabled.text = "true"

            #TODO add pretty format for newly added provider
            putKnoxTopologyContent('content', ET.tostring(root))



  def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight022StackAdvisor, self).recommendRangerConfigurations(configurations, clusterData, services, hosts)
    putRangerEnvProperty = self.putProperty(configurations, "ranger-env")
    cluster_env = getServicesSiteProperties(services, "cluster-env")
    security_enabled = cluster_env is not None and "security_enabled" in cluster_env and \
                       cluster_env["security_enabled"].lower() == "true"
    if "ranger-env" in configurations and not security_enabled:
      putRangerEnvProperty("ranger-storm-plugin-enabled", "No")

  def getServiceConfigurationValidators(self):
    parentValidators = super(RBLight022StackAdvisor, self).getServiceConfigurationValidators()
    childValidators = {
      "HDFS": {"hdfs-site": self.validateHDFSConfigurations,
               "hadoop-env": self.validateHDFSConfigurationsEnv,
               "ranger-hdfs-plugin-properties": self.validateHDFSRangerPluginConfigurations},
      "YARN": {"yarn-env": self.validateYARNEnvConfigurations,
               "ranger-yarn-plugin-properties": self.validateYARNRangerPluginConfigurations},
      "HIVE": {"hiveserver2-site": self.validateHiveServer2Configurations,
               "hive-site": self.validateHiveConfigurations,
               "hive-env": self.validateHiveConfigurationsEnv},
      "HBASE": {"hbase-site": self.validateHBASEConfigurations,
                "hbase-env": self.validateHBASEEnvConfigurations,
                "ranger-hbase-plugin-properties": self.validateHBASERangerPluginConfigurations},
      "KNOX": {"ranger-knox-plugin-properties": self.validateKnoxRangerPluginConfigurations},
      "KAFKA": {"ranger-kafka-plugin-properties": self.validateKafkaRangerPluginConfigurations},
      "STORM": {"ranger-storm-plugin-properties": self.validateStormRangerPluginConfigurations},
      "MAPREDUCE2": {"mapred-site": self.validateMapReduce2Configurations},
      "TEZ": {"tez-site": self.validateTezConfigurations},
      "RANGER": {"ranger-env": self.validateRangerConfigurationsEnv}
    }
    self.mergeValidators(parentValidators, childValidators)
    return parentValidators

  def validateTezConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'tez.am.resource.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'tez.am.resource.memory.mb')},
                        {"config-name": 'tez.task.resource.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'tez.task.resource.memory.mb')},
                        {"config-name": 'tez.runtime.io.sort.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'tez.runtime.io.sort.mb')},
                        {"config-name": 'tez.runtime.unordered.output.buffer.size-mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'tez.runtime.unordered.output.buffer.size-mb')},]

    tez_site = properties
    prop_name1 = 'tez.am.resource.memory.mb'
    prop_name2 = 'tez.task.resource.memory.mb'
    yarnSiteProperties = getSiteProperties(configurations, "yarn-site")
    if yarnSiteProperties:
      yarnMaxAllocationSize = min(30 * int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]),int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]))
      if int(tez_site[prop_name1]) > yarnMaxAllocationSize:
        validationItems.append({"config-name": prop_name1,
                                "item": self.getWarnItem(
                                  "{0} should be less than YARN max allocation size ({1})".format(prop_name1, yarnMaxAllocationSize))})
      if int(tez_site[prop_name2]) > yarnMaxAllocationSize:
        validationItems.append({"config-name": prop_name2,
                                "item": self.getWarnItem(
                                  "{0} should be less than YARN max allocation size ({1})".format(prop_name2, yarnMaxAllocationSize))})

    return self.toConfigurationValidationProblems(validationItems, "tez-site")

  def recommendMapReduce2Configurations(self, configurations, clusterData, services, hosts):
    self.recommendYARNConfigurations(configurations, clusterData, services, hosts)
    putMapredProperty = self.putProperty(configurations, "mapred-site", services)
    nodemanagerMinRam = 1048576 # 1TB in mb
    if "referenceNodeManagerHost" in clusterData:
      nodemanagerMinRam = min(clusterData["referenceNodeManagerHost"]["total_mem"]/1024, nodemanagerMinRam)
    putMapredProperty('yarn.app.mapreduce.am.resource.mb', configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"])
    putMapredProperty('yarn.app.mapreduce.am.command-opts', "-Xmx" + str(int(0.8 * int(configurations["mapred-site"]["properties"]["yarn.app.mapreduce.am.resource.mb"]))) + "m" + " -Dhdp.version=${hdp.version}")
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    min_mapreduce_map_memory_mb = 0
    min_mapreduce_reduce_memory_mb = 0
    min_mapreduce_map_java_opts = 0
    if ("PIG" in servicesList) and clusterData["totalAvailableRam"] >= 4096:
      min_mapreduce_map_memory_mb = 1536
      min_mapreduce_reduce_memory_mb = 1536
      min_mapreduce_map_java_opts = 1024
    putMapredProperty('mapreduce.map.memory.mb', min(int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]), max(min_mapreduce_map_memory_mb, int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]))))
    putMapredProperty('mapreduce.reduce.memory.mb', min(int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]), max(min_mapreduce_reduce_memory_mb, min(2*int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]), int(nodemanagerMinRam)))))
    mapredMapXmx = int(0.8*int(configurations["mapred-site"]["properties"]["mapreduce.map.memory.mb"]));
    putMapredProperty('mapreduce.map.java.opts', "-Xmx" + str(max(min_mapreduce_map_java_opts, mapredMapXmx)) + "m")
    putMapredProperty('mapreduce.reduce.java.opts', "-Xmx" + str(int(0.8*int(configurations["mapred-site"]["properties"]["mapreduce.reduce.memory.mb"]))) + "m")
    putMapredProperty('mapreduce.task.io.sort.mb', str(min(int(0.7*mapredMapXmx), 2047)))
    # Property Attributes
    putMapredPropertyAttribute = self.putPropertyAttribute(configurations, "mapred-site")
    yarnMinAllocationSize = int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"])
    yarnMaxAllocationSize = min(30 * int(configurations["yarn-site"]["properties"]["yarn.scheduler.minimum-allocation-mb"]), int(configurations["yarn-site"]["properties"]["yarn.scheduler.maximum-allocation-mb"]))
    putMapredPropertyAttribute("mapreduce.map.memory.mb", "maximum", yarnMaxAllocationSize)
    putMapredPropertyAttribute("mapreduce.map.memory.mb", "minimum", yarnMinAllocationSize)
    putMapredPropertyAttribute("mapreduce.reduce.memory.mb", "maximum", yarnMaxAllocationSize)
    putMapredPropertyAttribute("mapreduce.reduce.memory.mb", "minimum", yarnMinAllocationSize)
    putMapredPropertyAttribute("yarn.app.mapreduce.am.resource.mb", "maximum", yarnMaxAllocationSize)
    putMapredPropertyAttribute("yarn.app.mapreduce.am.resource.mb", "minimum", yarnMinAllocationSize)
    # Hadoop MR limitation
    putMapredPropertyAttribute("mapreduce.task.io.sort.mb", "maximum", "2047")

  def validateMapReduce2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'mapreduce.map.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.map.java.opts')},
                        {"config-name": 'mapreduce.reduce.java.opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'mapreduce.reduce.java.opts')},
                        {"config-name": 'mapreduce.task.io.sort.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.task.io.sort.mb')},
                        {"config-name": 'mapreduce.map.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.map.memory.mb')},
                        {"config-name": 'mapreduce.reduce.memory.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'mapreduce.reduce.memory.mb')},
                        {"config-name": 'yarn.app.mapreduce.am.resource.mb', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.resource.mb')},
                        {"config-name": 'yarn.app.mapreduce.am.command-opts', "item": self.validateXmxValue(properties, recommendedDefaults, 'yarn.app.mapreduce.am.command-opts')}]

    if 'mapreduce.map.java.opts' in properties and \
            checkXmxValueFormat(properties['mapreduce.map.java.opts']):
      mapreduceMapJavaOpts = formatXmxSizeToBytes(getXmxSize(properties['mapreduce.map.java.opts'])) / (1024.0 * 1024)
      mapreduceMapMemoryMb = to_number(properties['mapreduce.map.memory.mb'])
      if mapreduceMapJavaOpts > mapreduceMapMemoryMb:
        validationItems.append({"config-name": 'mapreduce.map.java.opts', "item": self.getWarnItem("mapreduce.map.java.opts Xmx should be less than mapreduce.map.memory.mb ({0})".format(mapreduceMapMemoryMb))})

    if 'mapreduce.reduce.java.opts' in properties and \
            checkXmxValueFormat(properties['mapreduce.reduce.java.opts']):
      mapreduceReduceJavaOpts = formatXmxSizeToBytes(getXmxSize(properties['mapreduce.reduce.java.opts'])) / (1024.0 * 1024)
      mapreduceReduceMemoryMb = to_number(properties['mapreduce.reduce.memory.mb'])
      if mapreduceReduceJavaOpts > mapreduceReduceMemoryMb:
        validationItems.append({"config-name": 'mapreduce.reduce.java.opts', "item": self.getWarnItem("mapreduce.reduce.java.opts Xmx should be less than mapreduce.reduce.memory.mb ({0})".format(mapreduceReduceMemoryMb))})

    if 'yarn.app.mapreduce.am.command-opts' in properties and \
            checkXmxValueFormat(properties['yarn.app.mapreduce.am.command-opts']):
      yarnAppMapreduceAmCommandOpts = formatXmxSizeToBytes(getXmxSize(properties['yarn.app.mapreduce.am.command-opts'])) / (1024.0 * 1024)
      yarnAppMapreduceAmResourceMb = to_number(properties['yarn.app.mapreduce.am.resource.mb'])
      if yarnAppMapreduceAmCommandOpts > yarnAppMapreduceAmResourceMb:
        validationItems.append({"config-name": 'yarn.app.mapreduce.am.command-opts', "item": self.getWarnItem("yarn.app.mapreduce.am.command-opts Xmx should be less than yarn.app.mapreduce.am.resource.mb ({0})".format(yarnAppMapreduceAmResourceMb))})

    return self.toConfigurationValidationProblems(validationItems, "mapred-site")

  def validateHDFSConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = [ {"config-name": 'namenode_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_heapsize')},
                        {"config-name": 'namenode_opt_newsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_newsize')},
                        {"config-name": 'namenode_opt_maxnewsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'namenode_opt_maxnewsize')}]
    return self.toConfigurationValidationProblems(validationItems, "hadoop-env")

  def validateHDFSRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hdfs-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hdfs-plugin-enabled'] if ranger_plugin_properties else 'No'
    if (ranger_plugin_enabled.lower() == 'yes'):
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-hdfs-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-hdfs-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-hdfs-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-hdfs-plugin-properties/ranger-hdfs-plugin-enabled must correspond ranger-env/ranger-hdfs-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-hdfs-plugin-properties")


  def validateHDFSConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    # We can not access property hadoop.security.authentication from the
    # other config (core-site). That's why we are using another heuristics here
    hdfs_site = properties
    core_site = getSiteProperties(configurations, "core-site")

    dfs_encrypt_data_transfer = 'dfs.encrypt.data.transfer'  # Hadoop Wire encryption
    try:
      wire_encryption_enabled = hdfs_site[dfs_encrypt_data_transfer] == "true"
    except KeyError:
      wire_encryption_enabled = False

    HTTP_ONLY = 'HTTP_ONLY'
    HTTPS_ONLY = 'HTTPS_ONLY'
    HTTP_AND_HTTPS = 'HTTP_AND_HTTPS'

    VALID_HTTP_POLICY_VALUES = [HTTP_ONLY, HTTPS_ONLY, HTTP_AND_HTTPS]
    VALID_TRANSFER_PROTECTION_VALUES = ['authentication', 'integrity', 'privacy']

    validationItems = []
    address_properties = [
      # "dfs.datanode.address",
      # "dfs.datanode.http.address",
      # "dfs.datanode.https.address",
      # "dfs.datanode.ipc.address",
      # "dfs.journalnode.http-address",
      # "dfs.journalnode.https-address",
      # "dfs.namenode.rpc-address",
      # "dfs.namenode.secondary.http-address",
      "dfs.namenode.http-address",
      "dfs.namenode.https-address",
    ]
    #Validating *address properties for correct values

    for address_property in address_properties:
      if address_property in hdfs_site:
        value = hdfs_site[address_property]
        if not is_valid_host_port_authority(value):
          validationItems.append({"config-name" : address_property, "item" :
            self.getErrorItem(address_property + " does not contain a valid host:port authority: " + value)})

    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hdfs-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hdfs-plugin-enabled'] if ranger_plugin_properties else 'No'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
      if 'dfs.permissions.enabled' in hdfs_site and \
                      hdfs_site['dfs.permissions.enabled'] != 'true':
        validationItems.append({"config-name": 'dfs.permissions.enabled',
                                "item": self.getWarnItem(
                                  "dfs.permissions.enabled needs to be set to true if Ranger HDFS Plugin is enabled.")})

    if (not wire_encryption_enabled and   # If wire encryption is enabled at Hadoop, it disables all our checks
            'hadoop.security.authentication' in core_site and
            core_site['hadoop.security.authentication'] == 'kerberos' and
            'hadoop.security.authorization' in core_site and
            core_site['hadoop.security.authorization'] == 'true'):
      # security is enabled

      dfs_http_policy = 'dfs.http.policy'
      dfs_datanode_address = 'dfs.datanode.address'
      datanode_http_address = 'dfs.datanode.http.address'
      datanode_https_address = 'dfs.datanode.https.address'
      data_transfer_protection = 'dfs.data.transfer.protection'

      try: # Params may be absent
        privileged_dfs_dn_port = isSecurePort(getPort(hdfs_site[dfs_datanode_address]))
      except KeyError:
        privileged_dfs_dn_port = False
      try:
        privileged_dfs_http_port = isSecurePort(getPort(hdfs_site[datanode_http_address]))
      except KeyError:
        privileged_dfs_http_port = False
      try:
        privileged_dfs_https_port = isSecurePort(getPort(hdfs_site[datanode_https_address]))
      except KeyError:
        privileged_dfs_https_port = False
      try:
        dfs_http_policy_value = hdfs_site[dfs_http_policy]
      except KeyError:
        dfs_http_policy_value = HTTP_ONLY  # Default
      try:
        data_transfer_protection_value = hdfs_site[data_transfer_protection]
      except KeyError:
        data_transfer_protection_value = None

      if dfs_http_policy_value not in VALID_HTTP_POLICY_VALUES:
        validationItems.append({"config-name": dfs_http_policy,
                                "item": self.getWarnItem(
                                  "Invalid property value: {0}. Valid values are {1}".format(
                                    dfs_http_policy_value, VALID_HTTP_POLICY_VALUES))})

      # determine whether we use secure ports
      address_properties_with_warnings = []
      if dfs_http_policy_value == HTTPS_ONLY:
        if not privileged_dfs_dn_port and (privileged_dfs_https_port or datanode_https_address not in hdfs_site):
          important_properties = [dfs_datanode_address, datanode_https_address]
          message = "You set up datanode to use some non-secure ports. " \
                    "If you want to run Datanode under non-root user in a secure cluster, " \
                    "you should set all these properties {2} " \
                    "to use non-secure ports (if property {3} does not exist, " \
                    "just add it). You may also set up property {4} ('{5}' is a good default value). " \
                    "Also, set up WebHDFS with SSL as " \
                    "described in manual in order to be able to " \
                    "use HTTPS.".format(dfs_http_policy, dfs_http_policy_value, important_properties,
                                        datanode_https_address, data_transfer_protection,
                                        VALID_TRANSFER_PROTECTION_VALUES[0])
          address_properties_with_warnings.extend(important_properties)
      else:  # dfs_http_policy_value == HTTP_AND_HTTPS or HTTP_ONLY
        # We don't enforce datanode_https_address to use privileged ports here
        any_nonprivileged_ports_are_in_use = not privileged_dfs_dn_port or not privileged_dfs_http_port
        if any_nonprivileged_ports_are_in_use:
          important_properties = [dfs_datanode_address, datanode_http_address]
          message = "You have set up datanode to use some non-secure ports, but {0} is set to {1}. " \
                    "In a secure cluster, Datanode forbids using non-secure ports " \
                    "if {0} is not set to {3}. " \
                    "Please make sure that properties {2} use secure ports.".format(
            dfs_http_policy, dfs_http_policy_value, important_properties, HTTPS_ONLY)
          address_properties_with_warnings.extend(important_properties)

      # Generate port-related warnings if any
      for prop in address_properties_with_warnings:
        validationItems.append({"config-name": prop,
                                "item": self.getWarnItem(message)})

      # Check if it is appropriate to use dfs.data.transfer.protection
      if data_transfer_protection_value is not None:
        if dfs_http_policy_value in [HTTP_ONLY, HTTP_AND_HTTPS]:
          validationItems.append({"config-name": data_transfer_protection,
                                  "item": self.getWarnItem(
                                    "{0} property can not be used when {1} is set to any "
                                    "value other then {2}. Tip: When {1} property is not defined, it defaults to {3}".format(
                                      data_transfer_protection, dfs_http_policy, HTTPS_ONLY, HTTP_ONLY))})
        elif not data_transfer_protection_value in VALID_TRANSFER_PROTECTION_VALUES:
          validationItems.append({"config-name": data_transfer_protection,
                                  "item": self.getWarnItem(
                                    "Invalid property value: {0}. Valid values are {1}.".format(
                                      data_transfer_protection_value, VALID_TRANSFER_PROTECTION_VALUES))})
    return self.toConfigurationValidationProblems(validationItems, "hdfs-site")

  def validateHiveServer2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
    hive_server2 = properties
    validationItems = []
    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hive-plugin-properties")
    hive_env_properties = getSiteProperties(configurations, "hive-env")
    ranger_plugin_enabled = 'hive_security_authorization' in hive_env_properties and hive_env_properties['hive_security_authorization'].lower() == 'ranger'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    ##Add stack validations only if Ranger is enabled.
    if ("RANGER" in servicesList):
      ##Add stack validations for  Ranger plugin enabled.
      if ranger_plugin_enabled:
        prop_name = 'hive.security.authorization.manager'
        prop_val = "com.xasecure.authorization.hive.authorizer.XaSecureHiveAuthorizerFactory"
        if prop_name not in hive_server2 or hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authenticator.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
        if prop_name not in hive_server2 or hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authorization.enabled'
        prop_val = 'true'
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name, prop_val))})
        prop_name = 'hive.conf.restricted.list'
        prop_vals = 'hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager'.split(',')
        current_vals = []
        missing_vals = []
        if hive_server2 and prop_name in hive_server2:
          current_vals = hive_server2[prop_name].split(',')
          current_vals = [x.strip() for x in current_vals]

        for val in prop_vals:
          if not val in current_vals:
            missing_vals.append(val)

        if missing_vals:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem("If Ranger Hive Plugin is enabled." \
                                                           " {0} under hiveserver2-site needs to contain missing value {1}".format(prop_name, ','.join(missing_vals)))})
      ##Add stack validations for  Ranger plugin disabled.
      elif not ranger_plugin_enabled:
        prop_name = 'hive.security.authorization.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is disabled." \
                                    " {0} needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authenticator.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is disabled." \
                                    " {0} needs to be set to {1}".format(prop_name,prop_val))})
    return self.toConfigurationValidationProblems(validationItems, "hiveserver2-site")

  def validateHiveConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    hive_env = properties
    hive_site = getSiteProperties(configurations, "hive-site")
    if "hive_security_authorization" in hive_env and \
                    str(hive_env["hive_security_authorization"]).lower() == "none" \
            and str(hive_site["hive.security.authorization.enabled"]).lower() == "true":
      authorization_item = self.getErrorItem("hive_security_authorization should not be None "
                                             "if hive.security.authorization.enabled is set")
      validationItems.append({"config-name": "hive_security_authorization", "item": authorization_item})
    if "hive_security_authorization" in hive_env and \
                    str(hive_env["hive_security_authorization"]).lower() == "ranger":
      # ranger-hive-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-hive-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-hive-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'hive_security_authorization',
                                "item": self.getWarnItem(
                                  "ranger-env/ranger-hive-plugin-enabled must be enabled when hive_security_authorization is set to Ranger")})
    return self.toConfigurationValidationProblems(validationItems, "hive-env")

  def validateHiveConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    parentValidationProblems = super(RBLight022StackAdvisor, self).validateHiveConfigurations(properties, recommendedDefaults, configurations, services, hosts)
    hive_site = properties
    validationItems = []
    stripe_size_values = [8388608, 16777216, 33554432, 67108864, 134217728, 268435456]
    stripe_size_property = "hive.exec.orc.default.stripe.size"
    if stripe_size_property in properties and \
                    int(properties[stripe_size_property]) not in stripe_size_values:
      validationItems.append({"config-name": stripe_size_property,
                              "item": self.getWarnItem("Correct values are {0}".format(stripe_size_values))
                              }
                             )
    authentication_property = "hive.server2.authentication"
    ldap_baseDN_property = "hive.server2.authentication.ldap.baseDN"
    ldap_domain_property = "hive.server2.authentication.ldap.Domain"
    if authentication_property in properties and properties[authentication_property].lower() == "ldap" \
            and not (ldap_baseDN_property in properties or ldap_domain_property in properties):
      validationItems.append({"config-name" : authentication_property, "item" :
        self.getWarnItem("According to LDAP value for " + authentication_property + ", you should add " +
                         ldap_domain_property + " property, if you are using AD, if not, then " + ldap_baseDN_property + "!")})


    configurationValidationProblems = self.toConfigurationValidationProblems(validationItems, "hive-site")
    configurationValidationProblems.extend(parentValidationProblems)
    return configurationValidationProblems

  def validateHBASEConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    hbase_site = properties
    validationItems = []

    prop_name1 = 'hbase.regionserver.global.memstore.size'
    prop_name2 = 'hfile.block.cache.size'
    props_max_sum = 0.8

    if prop_name1 in hbase_site and not is_number(hbase_site[prop_name1]):
      validationItems.append({"config-name": prop_name1,
                              "item": self.getWarnItem(
                                "{0} should be float value".format(prop_name1))})
    elif prop_name2 in hbase_site and not is_number(hbase_site[prop_name2]):
      validationItems.append({"config-name": prop_name2,
                              "item": self.getWarnItem(
                                "{0} should be float value".format(prop_name2))})
    elif prop_name1 in hbase_site and prop_name2 in hbase_site and \
                            float(hbase_site[prop_name1]) + float(hbase_site[prop_name2]) > props_max_sum:
      validationItems.append({"config-name": prop_name1,
                              "item": self.getWarnItem(
                                "{0} and {1} sum should not exceed {2}".format(prop_name1, prop_name2, props_max_sum))})

    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hbase-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hbase-plugin-enabled'] if ranger_plugin_properties else 'No'
    prop_name = 'hbase.security.authorization'
    prop_val = "true"
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
      if hbase_site[prop_name] != prop_val:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  "{0} needs to be set to {1}".format(prop_name,prop_val))})
      prop_name = "hbase.coprocessor.master.classes"
      prop_val = "com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor"
      exclude_val = "org.apache.hadoop.hbase.security.access.AccessController"
      if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
        pass
      else:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  " {0} needs to contain {1} instead of {2}".format(prop_name,prop_val,exclude_val))})
      prop_name = "hbase.coprocessor.region.classes"
      prop_val = "com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor"
      if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
        pass
      else:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  " {0} needs to contain {1} instead of {2}".format(prop_name,prop_val,exclude_val))})

    # Validate bucket cache correct config
    prop_name = "hbase.bucketcache.ioengine"
    prop_val = "offheap"
    if prop_name in hbase_site and not (not hbase_site[prop_name] or hbase_site[prop_name] == prop_val):
      validationItems.append({"config-name": prop_name,
                              "item": self.getWarnItem(
                                "Recommended values of " \
                                " {0} is empty or '{1}'".format(prop_name,prop_val))})

    prop_name1 = "hbase.bucketcache.ioengine"
    prop_name2 = "hbase.bucketcache.size"
    prop_name3 = "hbase.bucketcache.percentage.in.combinedcache"

    if prop_name1 in hbase_site and prop_name2 in hbase_site and hbase_site[prop_name1] and not hbase_site[prop_name2]:
      validationItems.append({"config-name": prop_name2,
                              "item": self.getWarnItem(
                                "If bucketcache ioengine is enabled, {0} should be set".format(prop_name2))})
    if prop_name1 in hbase_site and prop_name3 in hbase_site and hbase_site[prop_name1] and not hbase_site[prop_name3]:
      validationItems.append({"config-name": prop_name3,
                              "item": self.getWarnItem(
                                "If bucketcache ioengine is enabled, {0} should be set".format(prop_name3))})

    # Validate hbase.security.authentication.
    # Kerberos works only when security enabled.
    if "hbase.security.authentication" in properties:
      hbase_security_kerberos = properties["hbase.security.authentication"].lower() == "kerberos"
      core_site_properties = getSiteProperties(configurations, "core-site")
      security_enabled = False
      if core_site_properties:
        security_enabled = core_site_properties['hadoop.security.authentication'] == 'kerberos' and core_site_properties['hadoop.security.authorization'] == 'true'
      if not security_enabled and hbase_security_kerberos:
        validationItems.append({"config-name": "hbase.security.authentication",
                                "item": self.getWarnItem("Cluster must be secured with Kerberos before hbase.security.authentication's value of kerberos will have effect")})

    return self.toConfigurationValidationProblems(validationItems, "hbase-site")

  def validateHBASEEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    hbase_env = properties
    validationItems = [ {"config-name": 'hbase_regionserver_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_regionserver_heapsize')},
                        {"config-name": 'hbase_master_heapsize', "item": self.validatorLessThenDefaultValue(properties, recommendedDefaults, 'hbase_master_heapsize')} ]
    prop_name = "hbase_max_direct_memory_size"
    hbase_site_properties = getSiteProperties(configurations, "hbase-site")
    prop_name1 = "hbase.bucketcache.ioengine"

    if prop_name1 in hbase_site_properties and prop_name in hbase_env and hbase_site_properties[prop_name1] and hbase_site_properties[prop_name1] == "offheap" and not hbase_env[prop_name]:
      validationItems.append({"config-name": prop_name,
                              "item": self.getWarnItem(
                                "If bucketcache ioengine is enabled, {0} should be set".format(prop_name))})

    return self.toConfigurationValidationProblems(validationItems, "hbase-env")

  def validateHBASERangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hbase-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hbase-plugin-enabled'] if ranger_plugin_properties else 'No'
    if ranger_plugin_enabled.lower() == 'yes':
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-hbase-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-hbase-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-hbase-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-hbase-plugin-properties/ranger-hbase-plugin-enabled must correspond ranger-env/ranger-hbase-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-hbase-plugin-properties")

  def validateKnoxRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-knox-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-knox-plugin-enabled'] if ranger_plugin_properties else 'No'
    if ranger_plugin_enabled.lower() == 'yes':
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-knox-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-knox-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-knox-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-knox-plugin-properties/ranger-knox-plugin-enabled must correspond ranger-env/ranger-knox-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-knox-plugin-properties")

  def validateKafkaRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-kafka-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-kafka-plugin-enabled'] if ranger_plugin_properties else 'No'
    if ranger_plugin_enabled.lower() == 'yes':
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-kafka-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-kafka-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-kafka-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-kafka-plugin-properties/ranger-kafka-plugin-enabled must correspond ranger-env/ranger-kafka-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-kafka-plugin-properties")

  def validateStormRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-storm-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-storm-plugin-enabled'] if ranger_plugin_properties else 'No'
    if ranger_plugin_enabled.lower() == 'yes':
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-storm-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-storm-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-storm-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-storm-plugin-properties/ranger-storm-plugin-enabled must correspond ranger-env/ranger-storm-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-storm-plugin-properties")

  def validateYARNEnvConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    if "yarn_cgroups_enabled" in properties:
      yarn_cgroups_enabled = properties["yarn_cgroups_enabled"].lower() == "true"
      core_site_properties = getSiteProperties(configurations, "core-site")
      security_enabled = False
      if core_site_properties:
        security_enabled = core_site_properties['hadoop.security.authentication'] == 'kerberos' and core_site_properties['hadoop.security.authorization'] == 'true'
      if not security_enabled and yarn_cgroups_enabled:
        validationItems.append({"config-name": "yarn_cgroups_enabled",
                                "item": self.getWarnItem("CPU Isolation should only be enabled if security is enabled")})
    return self.toConfigurationValidationProblems(validationItems, "yarn-env")

  def validateYARNRangerPluginConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-yarn-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-yarn-plugin-enabled'] if ranger_plugin_properties else 'No'
    if ranger_plugin_enabled.lower() == 'yes':
      # ranger-hdfs-plugin must be enabled in ranger-env
      ranger_env = getServicesSiteProperties(services, 'ranger-env')
      if not ranger_env or not 'ranger-yarn-plugin-enabled' in ranger_env or \
                      ranger_env['ranger-yarn-plugin-enabled'].lower() != 'yes':
        validationItems.append({"config-name": 'ranger-yarn-plugin-enabled',
                                "item": self.getWarnItem(
                                  "ranger-yarn-plugin-properties/ranger-yarn-plugin-enabled must correspond ranger-env/ranger-yarn-plugin-enabled")})
    return self.toConfigurationValidationProblems(validationItems, "ranger-yarn-plugin-properties")

  def validateRangerConfigurationsEnv(self, properties, recommendedDefaults, configurations, services, hosts):
    validationItems = []
    if "ranger-storm-plugin-enabled" in properties and "ranger-storm-plugin-enabled" in recommendedDefaults and \
                    properties["ranger-storm-plugin-enabled"] != recommendedDefaults["ranger-storm-plugin-enabled"]:
      validationItems.append({"config-name": "ranger-storm-plugin-enabled",
                              "item": self.getWarnItem(
                                "Ranger Storm plugin should not be enabled in non-kerberos environment.")})

    return self.toConfigurationValidationProblems(validationItems, "ranger-env")

  def getMastersWithMultipleInstances(self):
    result = super(RBLight022StackAdvisor, self).getMastersWithMultipleInstances()
    result.extend(['METRICS_COLLECTOR'])
    return result

  def getNotValuableComponents(self):
    result = super(RBLight022StackAdvisor, self).getNotValuableComponents()
    result.extend(['METRICS_MONITOR'])
    return result

  def getCardinalitiesDict(self):
    result = super(RBLight022StackAdvisor, self).getCardinalitiesDict()
    result['METRICS_COLLECTOR'] = {"min": 1}
    return result

  def getAffectedConfigs(self, services):
    affectedConfigs = super(RBLight022StackAdvisor, self).getAffectedConfigs(services)

    # There are configs that are not defined in the stack but added/removed by
    # stack-advisor. Here we add such configs in order to clear the config
    # filtering down in base class
    configsList = [affectedConfig["type"] + "/" + affectedConfig["name"] for affectedConfig in affectedConfigs]
    if 'yarn-env/yarn_cgroups_enabled' in configsList:
      if 'yarn-site/yarn.nodemanager.container-executor.class' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.container-executor.class"})
      if 'yarn-site/yarn.nodemanager.container-executor.group' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.container-executor.group"})
      if 'yarn-site/yarn.nodemanager.container-executor.resources-handler.class' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.container-executor.resources-handler.class"})
      if 'yarn-site/yarn.nodemanager.container-executor.cgroups.hierarchy' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.container-executor.cgroups.hierarchy"})
      if 'yarn-site/yarn.nodemanager.container-executor.cgroups.mount' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.container-executor.cgroups.mount"})
      if 'yarn-site/yarn.nodemanager.linux-container-executor.cgroups.mount-path' not in configsList:
        affectedConfigs.append({"type": "yarn-site", "name": "yarn.nodemanager.linux-container-executor.cgroups.mount-path"})

    return affectedConfigs;

def is_number(s):
  try:
    float(s)
    return True
  except ValueError:
    pass

  return False

def is_valid_host_port_authority(target):
  has_scheme = "://" in target
  if not has_scheme:
    target = "dummyscheme://"+target
  try:
    result = urlparse(target)
    if result.hostname is not None and result.port is not None:
      return True
  except ValueError:
    pass
  return False


class RBLight023StackAdvisor(RBLight022StackAdvisor):

  def createComponentLayoutRecommendations(self, services, hosts):
    parentComponentLayoutRecommendations = super(RBLight023StackAdvisor, self).createComponentLayoutRecommendations(services, hosts)

    # remove HAWQSTANDBY on a single node
    hostsList = [host["Hosts"]["host_name"] for host in hosts["items"]]
    if len(hostsList) == 1:
      servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
      if "HAWQ" in servicesList:
        components = parentComponentLayoutRecommendations["blueprint"]["host_groups"][0]["components"]
        components = [ component for component in components if component["name"] != 'HAWQSTANDBY' ]
        parentComponentLayoutRecommendations["blueprint"]["host_groups"][0]["components"] = components

    return parentComponentLayoutRecommendations

  def getComponentLayoutValidations(self, services, hosts):
    parentItems = super(RBLight023StackAdvisor, self).getComponentLayoutValidations(services, hosts)

    hiveExists = "HIVE" in [service["StackServices"]["service_name"] for service in services["services"]]
    sparkExists = "SPARK" in [service["StackServices"]["service_name"] for service in services["services"]]

    if not "HAWQ" in [service["StackServices"]["service_name"] for service in services["services"]] and not sparkExists:
      return parentItems

    childItems = []
    hostsList = [host["Hosts"]["host_name"] for host in hosts["items"]]
    hostsCount = len(hostsList)

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item for sublist in componentsListList for item in sublist]
    hawqMasterHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HAWQMASTER"]
    hawqStandbyHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HAWQSTANDBY"]

    # single node case is not analyzed because HAWQ Standby Master will not be present in single node topology due to logic in createComponentLayoutRecommendations()
    if len(hawqMasterHosts) > 0 and len(hawqStandbyHosts) > 0:
      commonHosts = [host for host in hawqMasterHosts[0] if host in hawqStandbyHosts[0]]
      for host in commonHosts:
        message = "HAWQ Standby Master and HAWQ Master should not be deployed on the same host."
        childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'HAWQSTANDBY', "host": host } )

    if len(hawqMasterHosts) > 0 and hostsCount > 1:
      ambariServerHosts = [host for host in hawqMasterHosts[0] if self.isLocalHost(host)]
      for host in ambariServerHosts:
        message = "HAWQ Master and Ambari Server should not be deployed on the same host. " \
                  "If you leave them collocated, make sure to set HAWQ Master Port property " \
                  "to a value different from the port number used by Ambari Server database."
        childItems.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'HAWQMASTER', "host": host } )

    if len(hawqStandbyHosts) > 0 and hostsCount > 1:
      ambariServerHosts = [host for host in hawqStandbyHosts[0] if self.isLocalHost(host)]
      for host in ambariServerHosts:
        message = "HAWQ Standby Master and Ambari Server should not be deployed on the same host. " \
                  "If you leave them collocated, make sure to set HAWQ Master Port property " \
                  "to a value different from the port number used by Ambari Server database."
        childItems.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'HAWQSTANDBY', "host": host } )

    if "SPARK_THRIFTSERVER" in [service["StackServices"]["service_name"] for service in services["services"]]:
      if not "HIVE_SERVER" in [service["StackServices"]["service_name"] for service in services["services"]]:
        message = "SPARK_THRIFTSERVER requires HIVE services to be selected."
        childItems.append( {"type": 'host-component', "level": 'ERROR', "message": messge, "component-name": 'SPARK_THRIFTSERVER'} )

    hmsHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "HIVE_METASTORE"][0] if hiveExists else []
    sparkTsHosts = [component["StackServiceComponents"]["hostnames"] for component in componentsList if component["StackServiceComponents"]["component_name"] == "SPARK_THRIFTSERVER"][0] if sparkExists else []

    # if Spark Thrift Server is deployed but no Hive Server is deployed
    if len(sparkTsHosts) > 0 and len(hmsHosts) == 0:
      message = "SPARK_THRIFTSERVER requires HIVE_METASTORE to be selected/deployed."
      childItems.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'SPARK_THRIFTSERVER' } )

    parentItems.extend(childItems)
    return parentItems

  def getNotPreferableOnServerComponents(self):
    parentComponents = super(RBLight023StackAdvisor, self).getNotPreferableOnServerComponents()
    parentComponents.extend(['HAWQMASTER', 'HAWQSTANDBY'])
    return parentComponents

  def getComponentLayoutSchemes(self):
    parentSchemes = super(RBLight023StackAdvisor, self).getComponentLayoutSchemes()
    # key is max number of cluster hosts + 1, value is index in host list where to put the component
    childSchemes = {
      'HAWQMASTER' : {6: 2, 31: 1, "else": 5},
      'HAWQSTANDBY': {6: 1, 31: 2, "else": 3}
    }
    parentSchemes.update(childSchemes)
    return parentSchemes

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(RBLight023StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "TEZ": self.recommendTezConfigurations,
      "HDFS": self.recommendHDFSConfigurations,
      "YARN": self.recommendYARNConfigurations,
      "HIVE": self.recommendHIVEConfigurations,
      "HBASE": self.recommendHBASEConfigurations,
      "KAFKA": self.recommendKAFKAConfigurations,
      "RANGER": self.recommendRangerConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendTezConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendTezConfigurations(configurations, clusterData, services, hosts)

    putTezProperty = self.putProperty(configurations, "tez-site")
    # remove 2gb limit for tez.runtime.io.sort.mb
    # in HDP 2.3 "tez.runtime.sorter.class" is set by default to PIPELINED, in other case comment calculation code below
    taskResourceMemory = clusterData['mapMemory'] if clusterData['mapMemory'] > 2048 else int(clusterData['reduceMemory'])
    taskResourceMemory = min(clusterData['containers'] * clusterData['ramPerContainer'], taskResourceMemory)
    putTezProperty("tez.runtime.io.sort.mb", int(taskResourceMemory * 0.4))

    if "tez-site" in services["configurations"] and "tez.runtime.sorter.class" in services["configurations"]["tez-site"]["properties"]:
      if services["configurations"]["tez-site"]["properties"]["tez.runtime.sorter.class"] == "LEGACY":
        putTezAttribute = self.putPropertyAttribute(configurations, "tez-site")
        putTezAttribute("tez.runtime.io.sort.mb", "maximum", 2047)
    pass

    serverProperties = services["ambari-server-properties"]
    latest_tez_jar_version = None

    server_host = socket.getfqdn()
    server_port = '8080'
    server_protocol = 'http'
    views_dir = '/var/lib/ambari-server/resources/views/'

    if serverProperties:
      if 'client.api.port' in serverProperties:
        server_port = serverProperties['client.api.port']
      if 'views.dir' in serverProperties:
        views_dir = serverProperties['views.dir']
      if 'api.ssl' in serverProperties:
        if serverProperties['api.ssl'].lower() == 'true':
          server_protocol = 'https'

      views_work_dir = os.path.join(views_dir, 'work')

      if os.path.exists(views_work_dir) and os.path.isdir(views_work_dir):
        last_version = '0.0.0'
        for file in os.listdir(views_work_dir):
          if fnmatch.fnmatch(file, 'TEZ{*}'):
            current_version = file.lstrip("TEZ{").rstrip("}") # E.g.: TEZ{0.7.0.2.3.0.0-2154}
            if self.versionCompare(current_version.replace("-", "."), last_version.replace("-", ".")) >= 0:
              latest_tez_jar_version = current_version
              last_version = current_version
            pass
        pass
      pass
    pass

    if latest_tez_jar_version:
      tez_url = '{0}://{1}:{2}/#/main/views/TEZ/{3}/TEZ_CLUSTER_INSTANCE'.format(server_protocol, server_host, server_port, latest_tez_jar_version)
      putTezProperty("tez.tez-ui.history-url.base", tez_url)
    pass

    # TEZ JVM options
    jvmGCParams = "-XX:+UseParallelGC"
    if "ambari-server-properties" in services and "java.home" in services["ambari-server-properties"]:
      # JDK8 needs different parameters
      match = re.match(".*\/jdk(1\.\d+)[\-\_\.][^/]*$", services["ambari-server-properties"]["java.home"])
      if match and len(match.groups()) > 0:
        # Is version >= 1.8
        versionSplits = re.split("\.", match.group(1))
        if versionSplits and len(versionSplits) > 1 and int(versionSplits[0]) > 0 and int(versionSplits[1]) > 7:
          jvmGCParams = "-XX:+UseG1GC -XX:+ResizeTLAB"
    putTezProperty('tez.am.launch.cmd-opts', "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA " + jvmGCParams)
    putTezProperty('tez.task.launch.cmd-opts', "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA " + jvmGCParams)


  def recommendHBASEConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendHBASEConfigurations(configurations, clusterData, services, hosts)
    putHbaseSiteProperty = self.putProperty(configurations, "hbase-site", services)
    putHbaseSitePropertyAttributes = self.putPropertyAttribute(configurations, "hbase-site")
    putHbaseEnvProperty = self.putProperty(configurations, "hbase-env", services)
    putHbaseEnvPropertyAttributes = self.putPropertyAttribute(configurations, "hbase-env")

    # bucket cache for 1.x is configured slightly differently, HBASE-11520
    threshold = 23 # 2 Gb is reserved for other offheap memory
    if (int(clusterData["hbaseRam"]) > threshold):
      # To enable cache - calculate values
      regionserver_total_ram = int(clusterData["hbaseRam"]) * 1024
      regionserver_heap_size = 20480
      regionserver_max_direct_memory_size = regionserver_total_ram - regionserver_heap_size
      hfile_block_cache_size = '0.4'
      block_cache_heap = 8192 # int(regionserver_heap_size * hfile_block_cache_size)
      hbase_regionserver_global_memstore_size = '0.4'
      reserved_offheap_memory = 2048
      bucketcache_offheap_memory = regionserver_max_direct_memory_size - reserved_offheap_memory
      hbase_bucketcache_size = bucketcache_offheap_memory

      # Set values in hbase-site
      putHbaseSiteProperty('hfile.block.cache.size', hfile_block_cache_size)
      putHbaseSiteProperty('hbase.regionserver.global.memstore.size', hbase_regionserver_global_memstore_size)
      putHbaseSiteProperty('hbase.bucketcache.ioengine', 'offheap')
      putHbaseSiteProperty('hbase.bucketcache.size', hbase_bucketcache_size)
      # 2.2 stack method was called earlier, unset
      putHbaseSitePropertyAttributes('hbase.bucketcache.percentage.in.combinedcache', 'delete', 'true')

      # Enable in hbase-env
      putHbaseEnvProperty('hbase_max_direct_memory_size', regionserver_max_direct_memory_size)
      putHbaseEnvProperty('hbase_regionserver_heapsize', regionserver_heap_size)
    else:
      # Disable
      putHbaseSitePropertyAttributes('hbase.bucketcache.ioengine', 'delete', 'true')
      putHbaseSitePropertyAttributes('hbase.bucketcache.size', 'delete', 'true')
      putHbaseSitePropertyAttributes('hbase.bucketcache.percentage.in.combinedcache', 'delete', 'true')

      putHbaseEnvPropertyAttributes('hbase_max_direct_memory_size', 'delete', 'true')

    if 'hbase-env' in services['configurations'] and 'phoenix_sql_enabled' in services['configurations']['hbase-env']['properties'] and \
                    'true' == services['configurations']['hbase-env']['properties']['phoenix_sql_enabled'].lower():
      putHbaseSiteProperty("hbase.rpc.controllerfactory.class", "org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory")
      putHbaseSiteProperty("hbase.region.server.rpc.scheduler.factory.class", "org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory")
    else:
      putHbaseSitePropertyAttributes('hbase.region.server.rpc.scheduler.factory.class', 'delete', 'true')


  def recommendHIVEConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendHIVEConfigurations(configurations, clusterData, services, hosts)
    putHiveSiteProperty = self.putProperty(configurations, "hive-site", services)
    putHiveServerProperty = self.putProperty(configurations, "hiveserver2-site", services)
    putHiveSitePropertyAttribute = self.putPropertyAttribute(configurations, "hive-site")
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    # hive_security_authorization == 'ranger'
    if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "ranger":
      putHiveServerProperty("hive.security.authorization.manager", "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory")

    # TEZ JVM options
    jvmGCParams = "-XX:+UseParallelGC"
    if "ambari-server-properties" in services and "java.home" in services["ambari-server-properties"]:
      # JDK8 needs different parameters
      match = re.match(".*\/jdk(1\.\d+)[\-\_\.][^/]*$", services["ambari-server-properties"]["java.home"])
      if match and len(match.groups()) > 0:
        # Is version >= 1.8
        versionSplits = re.split("\.", match.group(1))
        if versionSplits and len(versionSplits) > 1 and int(versionSplits[0]) > 0 and int(versionSplits[1]) > 7:
          jvmGCParams = "-XX:+UseG1GC -XX:+ResizeTLAB"
    putHiveSiteProperty('hive.tez.java.opts', "-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA " + jvmGCParams + " -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps")

    # if hive using sqla db, then we should add DataNucleus property
    sqla_db_used = 'hive-env' in services['configurations'] and 'hive_database' in services['configurations']['hive-env']['properties'] and \
                   services['configurations']['hive-env']['properties']['hive_database'] == 'Existing SQL Anywhere Database'
    if sqla_db_used:
      putHiveSiteProperty('datanucleus.rdbms.datastoreAdapterClassName','org.datanucleus.store.rdbms.adapter.SQLAnywhereAdapter')
    else:
      putHiveSitePropertyAttribute('datanucleus.rdbms.datastoreAdapterClassName', 'delete', 'true')

    # atlas
    hooks_property = "hive.exec.post.hooks"
    if hooks_property in configurations["hive-site"]["properties"]:
      hooks_value = configurations["hive-site"]["properties"][hooks_property]
    else:
      hooks_value = " "

    include_atlas = "ATLAS" in servicesList
    atlas_hook_class = "org.apache.atlas.hive.hook.HiveHook"
    if include_atlas and atlas_hook_class not in hooks_value:
      if hooks_value == " ":
        hooks_value = atlas_hook_class
      else:
        hooks_value = hooks_value + "," + atlas_hook_class
    if not include_atlas and atlas_hook_class in hooks_value:
      hooks_classes = []
      for hook_class in hooks_value.split(","):
        if hook_class != atlas_hook_class and hook_class != " ":
          hooks_classes.append(hook_class)
      if hooks_classes:
        hooks_value = ",".join(hooks_classes)
      else:
        hooks_value = " "
    putHiveSiteProperty(hooks_property, hooks_value)

    atlas_server_host_info = self.getHostWithComponent("ATLAS", "ATLAS_SERVER", services, hosts)
    if include_atlas and atlas_server_host_info:
      cluster_name = 'default'
      putHiveSiteProperty('atlas.cluster.name', cluster_name)
      atlas_rest_host = atlas_server_host_info['Hosts']['host_name']
      scheme = "http"
      metadata_port = "21000"
      if 'application-properties' in services['configurations']:
        tls_enabled = services['configurations']['application-properties']['properties']['atlas.enableTLS']
        metadata_port =  services['configurations']['application-properties']['properties']['atlas.server.http.port']
        if tls_enabled.lower() == "true":
          scheme = "https"
          metadata_port =  services['configurations']['application-properties']['properties']['atlas.server.https.port']
      putHiveSiteProperty('atlas.rest.address', '{0}://{1}:{2}'.format(scheme, atlas_rest_host, metadata_port))
    else:
      putHiveSitePropertyAttribute('atlas.cluster.name', 'delete', 'true')
      putHiveSitePropertyAttribute('atlas.rest.address', 'delete', 'true')

  def recommendHDFSConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendHDFSConfigurations(configurations, clusterData, services, hosts)

    putHdfsSiteProperty = self.putProperty(configurations, "hdfs-site", services)
    putHdfsSitePropertyAttribute = self.putPropertyAttribute(configurations, "hdfs-site")

    if ('ranger-hdfs-plugin-properties' in services['configurations']) and ('ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']):
      rangerPluginEnabled = ''
      if 'ranger-hdfs-plugin-properties' in configurations and 'ranger-hdfs-plugin-enabled' in  configurations['ranger-hdfs-plugin-properties']['properties']:
        rangerPluginEnabled = configurations['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']
      elif 'ranger-hdfs-plugin-properties' in services['configurations'] and 'ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties']['properties']:
        rangerPluginEnabled = services['configurations']['ranger-hdfs-plugin-properties']['properties']['ranger-hdfs-plugin-enabled']

      if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
        putHdfsSiteProperty("dfs.namenode.inode.attributes.provider.class",'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer')
      else:
        putHdfsSitePropertyAttribute('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')
    else:
      putHdfsSitePropertyAttribute('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')

  def recommendKAFKAConfigurations(self, configurations, clusterData, services, hosts):
    kafka_broker = getServicesSiteProperties(services, "kafka-broker")

    # kerberos security for kafka is decided from `security.inter.broker.protocol` property value
    security_enabled = (kafka_broker is not None and 'security.inter.broker.protocol' in  kafka_broker
                        and 'SASL' in kafka_broker['security.inter.broker.protocol'])
    putKafkaBrokerProperty = self.putProperty(configurations, "kafka-broker", services)
    putKafkaLog4jProperty = self.putProperty(configurations, "kafka-log4j", services)
    putKafkaBrokerAttributes = self.putPropertyAttribute(configurations, "kafka-broker")

    #If AMS is part of Services, use the KafkaTimelineMetricsReporter for metric reporting. Default is ''.
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if "AMBARI_METRICS" in servicesList:
      putKafkaBrokerProperty('kafka.metrics.reporters', 'org.apache.hadoop.metrics2.sink.kafka.KafkaTimelineMetricsReporter')

    if "ranger-env" in services["configurations"] and "ranger-kafka-plugin-properties" in services["configurations"] and \
                    "ranger-kafka-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putKafkaRangerPluginProperty = self.putProperty(configurations, "ranger-kafka-plugin-properties", services)
      rangerEnvKafkaPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-kafka-plugin-enabled"]
      putKafkaRangerPluginProperty("ranger-kafka-plugin-enabled", rangerEnvKafkaPluginProperty)

    if 'ranger-kafka-plugin-properties' in services['configurations'] and ('ranger-kafka-plugin-enabled' in services['configurations']['ranger-kafka-plugin-properties']['properties']):
      kafkaLog4jRangerLines = [{
        "name": "log4j.appender.rangerAppender",
        "value": "org.apache.log4j.DailyRollingFileAppender"
      },
        {
          "name": "log4j.appender.rangerAppender.DatePattern",
          "value": "'.'yyyy-MM-dd-HH"
        },
        {
          "name": "log4j.appender.rangerAppender.File",
          "value": "${kafka.logs.dir}/ranger_kafka.log"
        },
        {
          "name": "log4j.appender.rangerAppender.layout",
          "value": "org.apache.log4j.PatternLayout"
        },
        {
          "name": "log4j.appender.rangerAppender.layout.ConversionPattern",
          "value": "%d{ISO8601} %p [%t] %C{6} (%F:%L) - %m%n"
        },
        {
          "name": "log4j.logger.org.apache.ranger",
          "value": "INFO, rangerAppender"
        }]

      rangerPluginEnabled=''
      if 'ranger-kafka-plugin-properties' in configurations and 'ranger-kafka-plugin-enabled' in  configurations['ranger-kafka-plugin-properties']['properties']:
        rangerPluginEnabled = configurations['ranger-kafka-plugin-properties']['properties']['ranger-kafka-plugin-enabled']
      elif 'ranger-kafka-plugin-properties' in services['configurations'] and 'ranger-kafka-plugin-enabled' in services['configurations']['ranger-kafka-plugin-properties']['properties']:
        rangerPluginEnabled = services['configurations']['ranger-kafka-plugin-properties']['properties']['ranger-kafka-plugin-enabled']

      if  rangerPluginEnabled and rangerPluginEnabled.lower() == "Yes".lower():
        # recommend authorizer.class.name
        putKafkaBrokerProperty("authorizer.class.name", 'org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer')
        # change kafka-log4j when ranger plugin is installed

        if 'kafka-log4j' in services['configurations'] and 'content' in services['configurations']['kafka-log4j']['properties']:
          kafkaLog4jContent = services['configurations']['kafka-log4j']['properties']['content']
          for item in range(len(kafkaLog4jRangerLines)):
            if kafkaLog4jRangerLines[item]["name"] not in kafkaLog4jContent:
              kafkaLog4jContent+= '\n' + kafkaLog4jRangerLines[item]["name"] + '=' + kafkaLog4jRangerLines[item]["value"]
          putKafkaLog4jProperty("content",kafkaLog4jContent)


      else:
        # Kerberized Cluster with Ranger plugin disabled
        if security_enabled and 'kafka-broker' in services['configurations'] and 'authorizer.class.name' in services['configurations']['kafka-broker']['properties'] and \
                        services['configurations']['kafka-broker']['properties']['authorizer.class.name'] == 'org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer':
          putKafkaBrokerProperty("authorizer.class.name", 'kafka.security.auth.SimpleAclAuthorizer')
        # Non-kerberos Cluster with Ranger plugin disabled
        else:
          putKafkaBrokerAttributes('authorizer.class.name', 'delete', 'true')

    # Non-Kerberos Cluster without Ranger
    elif not security_enabled:
      putKafkaBrokerAttributes('authorizer.class.name', 'delete', 'true')


  def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendRangerConfigurations(configurations, clusterData, services, hosts)
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    putRangerAdminProperty = self.putProperty(configurations, "ranger-admin-site", services)
    putRangerEnvProperty = self.putProperty(configurations, "ranger-env", services)
    putRangerUgsyncSite = self.putProperty(configurations, "ranger-ugsync-site", services)

    if 'admin-properties' in services['configurations'] and ('DB_FLAVOR' in services['configurations']['admin-properties']['properties']) \
            and ('db_host' in services['configurations']['admin-properties']['properties']) and ('db_name' in services['configurations']['admin-properties']['properties']):

      rangerDbFlavor = services['configurations']["admin-properties"]["properties"]["DB_FLAVOR"]
      rangerDbHost =   services['configurations']["admin-properties"]["properties"]["db_host"]
      rangerDbName =   services['configurations']["admin-properties"]["properties"]["db_name"]
      ranger_db_url_dict = {
        'MYSQL': {'ranger.jpa.jdbc.driver': 'com.mysql.jdbc.Driver', 'ranger.jpa.jdbc.url': 'jdbc:mysql://' + rangerDbHost + '/' + rangerDbName},
        'ORACLE': {'ranger.jpa.jdbc.driver': 'oracle.jdbc.driver.OracleDriver', 'ranger.jpa.jdbc.url': 'jdbc:oracle:thin:@//' + rangerDbHost + ':1521/' + rangerDbName},
        'POSTGRES': {'ranger.jpa.jdbc.driver': 'org.postgresql.Driver', 'ranger.jpa.jdbc.url': 'jdbc:postgresql://' + rangerDbHost + ':5432/' + rangerDbName},
        'MSSQL': {'ranger.jpa.jdbc.driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver', 'ranger.jpa.jdbc.url': 'jdbc:sqlserver://' + rangerDbHost + ';databaseName=' + rangerDbName},
        'SQLA': {'ranger.jpa.jdbc.driver': 'sap.jdbc4.sqlanywhere.IDriver', 'ranger.jpa.jdbc.url': 'jdbc:sqlanywhere:host=' + rangerDbHost + ';database=' + rangerDbName}
      }
      rangerDbProperties = ranger_db_url_dict.get(rangerDbFlavor, ranger_db_url_dict['MYSQL'])
      for key in rangerDbProperties:
        putRangerAdminProperty(key, rangerDbProperties.get(key))

      if 'admin-properties' in services['configurations'] and ('DB_FLAVOR' in services['configurations']['admin-properties']['properties']) \
              and ('db_host' in services['configurations']['admin-properties']['properties']):

        rangerDbFlavor = services['configurations']["admin-properties"]["properties"]["DB_FLAVOR"]
        rangerDbHost =   services['configurations']["admin-properties"]["properties"]["db_host"]
        ranger_db_privelege_url_dict = {
          'MYSQL': {'ranger_privelege_user_jdbc_url': 'jdbc:mysql://' + rangerDbHost},
          'ORACLE': {'ranger_privelege_user_jdbc_url': 'jdbc:oracle:thin:@//' + rangerDbHost + ':1521'},
          'POSTGRES': {'ranger_privelege_user_jdbc_url': 'jdbc:postgresql://' + rangerDbHost + ':5432/postgres'},
          'MSSQL': {'ranger_privelege_user_jdbc_url': 'jdbc:sqlserver://' + rangerDbHost + ';'},
          'SQLA': {'ranger_privelege_user_jdbc_url': 'jdbc:sqlanywhere:host=' + rangerDbHost + ';'}
        }
        rangerPrivelegeDbProperties = ranger_db_privelege_url_dict.get(rangerDbFlavor, ranger_db_privelege_url_dict['MYSQL'])
        for key in rangerPrivelegeDbProperties:
          putRangerEnvProperty(key, rangerPrivelegeDbProperties.get(key))

    # Recommend ldap settings based on ambari.properties configuration
    if 'ambari-server-properties' in services and \
                    'ambari.ldap.isConfigured' in services['ambari-server-properties'] and \
                    services['ambari-server-properties']['ambari.ldap.isConfigured'].lower() == "true":
      serverProperties = services['ambari-server-properties']
      if 'authentication.ldap.baseDn' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.ldap.searchBase', serverProperties['authentication.ldap.baseDn'])
      if 'authentication.ldap.groupMembershipAttr' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.group.memberattributename', serverProperties['authentication.ldap.groupMembershipAttr'])
      if 'authentication.ldap.groupNamingAttr' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.group.nameattribute', serverProperties['authentication.ldap.groupNamingAttr'])
      if 'authentication.ldap.groupObjectClass' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.group.objectclass', serverProperties['authentication.ldap.groupObjectClass'])
      if 'authentication.ldap.managerDn' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.ldap.binddn', serverProperties['authentication.ldap.managerDn'])
      if 'authentication.ldap.primaryUrl' in serverProperties:
        ldap_protocol =  'ldap://'
        if 'authentication.ldap.useSSL' in serverProperties and serverProperties['authentication.ldap.useSSL'] == 'true':
          ldap_protocol =  'ldaps://'
        ldapUrl = ldap_protocol + serverProperties['authentication.ldap.primaryUrl'] if serverProperties['authentication.ldap.primaryUrl'] else serverProperties['authentication.ldap.primaryUrl']
        putRangerUgsyncSite('ranger.usersync.ldap.url', ldapUrl)
      if 'authentication.ldap.userObjectClass' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.ldap.user.objectclass', serverProperties['authentication.ldap.userObjectClass'])
      if 'authentication.ldap.usernameAttribute' in serverProperties:
        putRangerUgsyncSite('ranger.usersync.ldap.user.nameattribute', serverProperties['authentication.ldap.usernameAttribute'])


    # Recommend Ranger Authentication method
    authMap = {
      'org.apache.ranger.unixusersync.process.UnixUserGroupBuilder': 'UNIX',
      'org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder': 'LDAP'
    }

    if 'ranger-ugsync-site' in services['configurations'] and 'ranger.usersync.source.impl.class' in services['configurations']["ranger-ugsync-site"]["properties"]:
      rangerUserSyncClass = services['configurations']["ranger-ugsync-site"]["properties"]["ranger.usersync.source.impl.class"]
      if rangerUserSyncClass in authMap:
        rangerSqlConnectorProperty = authMap.get(rangerUserSyncClass)
        putRangerAdminProperty('ranger.authentication.method', rangerSqlConnectorProperty)


    if 'ranger-env' in services['configurations'] and 'is_solrCloud_enabled' in services['configurations']["ranger-env"]["properties"]:
      isSolrCloudEnabled = services['configurations']["ranger-env"]["properties"]["is_solrCloud_enabled"]  == "true"
    else:
      isSolrCloudEnabled = False

    if isSolrCloudEnabled:
      zookeeper_host_port = self.getZKHostPortString(services)
      ranger_audit_zk_port = ''
      if zookeeper_host_port:
        ranger_audit_zk_port = '{0}/{1}'.format(zookeeper_host_port, 'ranger_audits')
        putRangerAdminProperty('ranger.audit.solr.zookeepers', ranger_audit_zk_port)
    else:
      putRangerAdminProperty('ranger.audit.solr.zookeepers', 'NONE')

    # Recommend ranger.audit.solr.zookeepers and xasecure.audit.destination.hdfs.dir
    include_hdfs = "HDFS" in servicesList
    if include_hdfs:
      if 'core-site' in services['configurations'] and ('fs.defaultFS' in services['configurations']['core-site']['properties']):
        default_fs = services['configurations']['core-site']['properties']['fs.defaultFS']
        putRangerEnvProperty('xasecure.audit.destination.hdfs.dir', '{0}/{1}/{2}'.format(default_fs,'ranger','audit'))

    # Recommend Ranger supported service's audit properties
    ranger_services = [
      {'service_name': 'HDFS', 'audit_file': 'ranger-hdfs-audit'},
      {'service_name': 'YARN', 'audit_file': 'ranger-yarn-audit'},
      {'service_name': 'HBASE', 'audit_file': 'ranger-hbase-audit'},
      {'service_name': 'HIVE', 'audit_file': 'ranger-hive-audit'},
      {'service_name': 'KNOX', 'audit_file': 'ranger-knox-audit'},
      {'service_name': 'KAFKA', 'audit_file': 'ranger-kafka-audit'},
      {'service_name': 'STORM', 'audit_file': 'ranger-storm-audit'}
    ]

    for item in range(len(ranger_services)):
      if ranger_services[item]['service_name'] in servicesList:
        component_audit_file =  ranger_services[item]['audit_file']
        if component_audit_file in services["configurations"]:
          ranger_audit_dict = [
            {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.db', 'target_configname': 'xasecure.audit.destination.db'},
            {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs', 'target_configname': 'xasecure.audit.destination.hdfs'},
            {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs.dir', 'target_configname': 'xasecure.audit.destination.hdfs.dir'},
            {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.solr', 'target_configname': 'xasecure.audit.destination.solr'},
            {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.urls', 'target_configname': 'xasecure.audit.destination.solr.urls'},
            {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.zookeepers', 'target_configname': 'xasecure.audit.destination.solr.zookeepers'}
          ]
          putRangerAuditProperty = self.putProperty(configurations, component_audit_file, services)

          for item in ranger_audit_dict:
            if item['filename'] in services["configurations"] and item['configname'] in  services["configurations"][item['filename']]["properties"]:
              if item['filename'] in configurations and item['configname'] in  configurations[item['filename']]["properties"]:
                rangerAuditProperty = configurations[item['filename']]["properties"][item['configname']]
              else:
                rangerAuditProperty = services["configurations"][item['filename']]["properties"][item['configname']]
              putRangerAuditProperty(item['target_configname'], rangerAuditProperty)



  def recommendYARNConfigurations(self, configurations, clusterData, services, hosts):
    super(RBLight023StackAdvisor, self).recommendYARNConfigurations(configurations, clusterData, services, hosts)
    putYarnSiteProperty = self.putProperty(configurations, "yarn-site", services)
    putYarnSitePropertyAttributes = self.putPropertyAttribute(configurations, "yarn-site")
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

    if "tez-site" not in services["configurations"]:
      putYarnSiteProperty('yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes', '')
    else:
      putYarnSiteProperty('yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes', 'org.apache.tez.dag.history.logging.ats.TimelineCachePluginImpl')

    if "ranger-env" in services["configurations"] and "ranger-yarn-plugin-properties" in services["configurations"] and \
                    "ranger-yarn-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
      putYarnRangerPluginProperty = self.putProperty(configurations, "ranger-yarn-plugin-properties", services)
      rangerEnvYarnPluginProperty = services["configurations"]["ranger-env"]["properties"]["ranger-yarn-plugin-enabled"]
      putYarnRangerPluginProperty("ranger-yarn-plugin-enabled", rangerEnvYarnPluginProperty)
    rangerPluginEnabled = ''
    if 'ranger-yarn-plugin-properties' in configurations and 'ranger-yarn-plugin-enabled' in  configurations['ranger-yarn-plugin-properties']['properties']:
      rangerPluginEnabled = configurations['ranger-yarn-plugin-properties']['properties']['ranger-yarn-plugin-enabled']
    elif 'ranger-yarn-plugin-properties' in services['configurations'] and 'ranger-yarn-plugin-enabled' in services['configurations']['ranger-yarn-plugin-properties']['properties']:
      rangerPluginEnabled = services['configurations']['ranger-yarn-plugin-properties']['properties']['ranger-yarn-plugin-enabled']

    if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
      putYarnSiteProperty('yarn.acl.enable','true')
      putYarnSiteProperty('yarn.authorization-provider','org.apache.ranger.authorization.yarn.authorizer.RangerYarnAuthorizer')
    else:
      putYarnSitePropertyAttributes('yarn.authorization-provider', 'delete', 'true')

    if 'RANGER_KMS' in servicesList and 'KERBEROS' in servicesList:
      if 'yarn-site' in services["configurations"] and 'yarn.resourcemanager.proxy-user-privileges.enabled' in services["configurations"]["yarn-site"]["properties"]:
        putYarnSiteProperty('yarn.resourcemanager.proxy-user-privileges.enabled', 'false')

  def getServiceConfigurationValidators(self):
    parentValidators = super(RBLight023StackAdvisor, self).getServiceConfigurationValidators()
    childValidators = {
      "HDFS": {"hdfs-site": self.validateHDFSConfigurations},
      "HIVE": {"hiveserver2-site": self.validateHiveServer2Configurations,
               "hive-site": self.validateHiveConfigurations},
      "HBASE": {"hbase-site": self.validateHBASEConfigurations},
      "KAKFA": {"kafka-broker": self.validateKAFKAConfigurations},
      "YARN": {"yarn-site": self.validateYARNConfigurations}
    }
    self.mergeValidators(parentValidators, childValidators)
    return parentValidators

  def validateHDFSConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    super(RBLight023StackAdvisor, self).validateHDFSConfigurations(properties, recommendedDefaults, configurations, services, hosts)

    # We can not access property hadoop.security.authentication from the
    # other config (core-site). That's why we are using another heuristics here
    hdfs_site = properties
    validationItems = [] #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hdfs-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hdfs-plugin-enabled'] if ranger_plugin_properties else 'No'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
      if 'dfs.namenode.inode.attributes.provider.class' not in hdfs_site or \
                      hdfs_site['dfs.namenode.inode.attributes.provider.class'].lower() != 'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer'.lower():
        validationItems.append({"config-name": 'dfs.namenode.inode.attributes.provider.class',
                                "item": self.getWarnItem(
                                  "dfs.namenode.inode.attributes.provider.class needs to be set to 'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer' if Ranger HDFS Plugin is enabled.")})
    return self.toConfigurationValidationProblems(validationItems, "hdfs-site")


  def validateHiveConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    parentValidationProblems = super(RBLight023StackAdvisor, self).validateHiveConfigurations(properties, recommendedDefaults, configurations, services, hosts)
    hive_site = properties
    hive_env_properties = getSiteProperties(configurations, "hive-env")
    validationItems = []
    sqla_db_used = "hive_database" in hive_env_properties and \
                   hive_env_properties['hive_database'] == 'Existing SQL Anywhere Database'
    prop_name = "datanucleus.rdbms.datastoreAdapterClassName"
    prop_value = "org.datanucleus.store.rdbms.adapter.SQLAnywhereAdapter"
    if sqla_db_used:
      if not prop_name in hive_site:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Hive using SQL Anywhere db." \
                                  " {0} needs to be added with value {1}".format(prop_name,prop_value))})
      elif prop_name in hive_site and hive_site[prop_name] != "org.datanucleus.store.rdbms.adapter.SQLAnywhereAdapter":
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Hive using SQL Anywhere db." \
                                  " {0} needs to be set to {1}".format(prop_name,prop_value))})

    configurationValidationProblems = self.toConfigurationValidationProblems(validationItems, "hive-site")
    configurationValidationProblems.extend(parentValidationProblems)
    return configurationValidationProblems

  def validateHiveServer2Configurations(self, properties, recommendedDefaults, configurations, services, hosts):
    super(RBLight023StackAdvisor, self).validateHiveServer2Configurations(properties, recommendedDefaults, configurations, services, hosts)
    hive_server2 = properties
    validationItems = []
    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hive-plugin-properties")
    hive_env_properties = getSiteProperties(configurations, "hive-env")
    ranger_plugin_enabled = 'hive_security_authorization' in hive_env_properties and hive_env_properties['hive_security_authorization'].lower() == 'ranger'
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    ##Add stack validations only if Ranger is enabled.
    if ("RANGER" in servicesList):
      ##Add stack validations for  Ranger plugin enabled.
      if ranger_plugin_enabled:
        prop_name = 'hive.security.authorization.manager'
        prop_val = "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authenticator.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authorization.enabled'
        prop_val = 'true'
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is enabled." \
                                    " {0} under hiveserver2-site needs to be set to {1}".format(prop_name, prop_val))})
        prop_name = 'hive.conf.restricted.list'
        prop_vals = 'hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager'.split(',')
        current_vals = []
        missing_vals = []
        if hive_server2 and prop_name in hive_server2:
          current_vals = hive_server2[prop_name].split(',')
          current_vals = [x.strip() for x in current_vals]

        for val in prop_vals:
          if not val in current_vals:
            missing_vals.append(val)

        if missing_vals:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem("If Ranger Hive Plugin is enabled." \
                                                           " {0} under hiveserver2-site needs to contain missing value {1}".format(prop_name, ','.join(missing_vals)))})
      ##Add stack validations for  Ranger plugin disabled.
      elif not ranger_plugin_enabled:
        prop_name = 'hive.security.authorization.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is disabled." \
                                    " {0} needs to be set to {1}".format(prop_name,prop_val))})
        prop_name = 'hive.security.authenticator.manager'
        prop_val = "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
        if prop_name in hive_server2 and hive_server2[prop_name] != prop_val:
          validationItems.append({"config-name": prop_name,
                                  "item": self.getWarnItem(
                                    "If Ranger Hive Plugin is disabled." \
                                    " {0} needs to be set to {1}".format(prop_name,prop_val))})
    return self.toConfigurationValidationProblems(validationItems, "hiveserver2-site")

  def validateHBASEConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    super(RBLight023StackAdvisor, self).validateHBASEConfigurations(properties, recommendedDefaults, configurations, services, hosts)
    hbase_site = properties
    validationItems = []

    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-hbase-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-hbase-plugin-enabled'] if ranger_plugin_properties else 'No'
    prop_name = 'hbase.security.authorization'
    prop_val = "true"
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
      if hbase_site[prop_name] != prop_val:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  "{0} needs to be set to {1}".format(prop_name,prop_val))})
      prop_name = "hbase.coprocessor.master.classes"
      prop_val = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor"
      exclude_val = "org.apache.hadoop.hbase.security.access.AccessController"
      if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
        pass
      else:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  " {0} needs to contain {1} instead of {2}".format(prop_name,prop_val,exclude_val))})
      prop_name = "hbase.coprocessor.region.classes"
      prop_val = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor"
      if (prop_val in hbase_site[prop_name] and exclude_val not in hbase_site[prop_name]):
        pass
      else:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger HBase Plugin is enabled." \
                                  " {0} needs to contain {1} instead of {2}".format(prop_name,prop_val,exclude_val))})

    return self.toConfigurationValidationProblems(validationItems, "hbase-site")

  def validateKAFKAConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    kafka_broker = properties
    validationItems = []

    #Adding Ranger Plugin logic here
    ranger_plugin_properties = getSiteProperties(configurations, "ranger-kafka-plugin-properties")
    ranger_plugin_enabled = ranger_plugin_properties['ranger-kafka-plugin-enabled']
    prop_name = 'authorizer.class.name'
    prop_val = "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer"
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if ("RANGER" in servicesList) and (ranger_plugin_enabled.lower() == 'Yes'.lower()):
      if kafka_broker[prop_name] != prop_val:
        validationItems.append({"config-name": prop_name,
                                "item": self.getWarnItem(
                                  "If Ranger Kafka Plugin is enabled." \
                                  "{0} needs to be set to {1}".format(prop_name,prop_val))})

    return self.toConfigurationValidationProblems(validationItems, "kafka-broker")

  def validateYARNConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    yarn_site = properties
    validationItems = []
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    if 'RANGER_KMS' in servicesList and 'KERBEROS' in servicesList:
      yarn_resource_proxy_enabled = yarn_site['yarn.resourcemanager.proxy-user-privileges.enabled']
      if yarn_resource_proxy_enabled.lower() == 'true':
        validationItems.append({"config-name": 'yarn.resourcemanager.proxy-user-privileges.enabled',
                                "item": self.getWarnItem("If Ranger KMS service is installed set yarn.resourcemanager.proxy-user-privileges.enabled " \
                                                         "property value as false under yarn-site"
                                                         )})

    return self.toConfigurationValidationProblems(validationItems, "yarn-site")

  def isComponentUsingCardinalityForLayout(self, componentName):
    return componentName in ['NFS_GATEWAY', 'PHOENIX_QUERY_SERVER', 'SPARK_THRIFTSERVER']


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
    print("getServiceConfigurationRecommenderDict")
    childRecommendConfDict = {
      "FLINK": self.recommendFlinkConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendFlinkConfigurations(self, configurations, clusterData, services, hosts):
    print("recommendFlinkConfigurations")
    alluxioMasterHosts = self.getHostWithComponent("ALLUXIO", "ALLUXIO_MASTER", services, hosts)
    hdfsNameNodes = self.getHostWithComponent("HDFS", "NAMENODE", services, hosts)
    flinkJobManagers = self.getHostWithComponent("FLINK", "NAMENODE", services, hosts)

    putFlinkProperty = self.putProperty(configurations, "flink-conf", services)

    putFlinkProperty("taskmanager.numberOfTaskSlots", multiprocessing.cpu_count())

    #config['configurations']['core-site']['fs.defaultFS']
    #services["configurations"]["core-site"]["properties"]["fs.defaultFS"]

    #recovery.zookeeper.quorum
    #recovery.zookeeper.storageDir = format('{hdfs_default_name}{recovery_zookeeper_path_root}')

    if alluxioMasterHosts is not None:
      putFlinkProperty("fs.default-scheme", 'alluxio-ft://' + alluxioMasterHosts[0] + ':19998/')
    elif hdfsNameNodes is not None:
      putFlinkProperty("fs.default-scheme", 'hdfs://' + hdfsNameNodes[0] + ':50010/')
    else:
      putFlinkProperty("fs.default-scheme", "file:///")
