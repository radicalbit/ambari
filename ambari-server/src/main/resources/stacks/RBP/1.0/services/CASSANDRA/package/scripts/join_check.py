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
import subprocess
import re
# import argparse
from time import sleep
from resource_management import *

def join_check(ip_address, seed_node):

  def valid_ip(address):
    a = address.split('.')
    if len(a) != 4:
      return False
    for x in a:
      if not x.isdigit():
        return False
      i = int(x)
      if i < 0 or i > 255:
        return False
    return True


  def moving_nodes(exclude, seed_node):
    output = subprocess.check_output(["nodetool", "--host", seed_node, "status"])
    Logger.info("[nodetool --host {0} status] result:\n{1}".format(seed_node, output))
    lines = output.splitlines()

    r = re.compile("[U|D][J|L|M]\s\s.*")

    filtered = filter(r.match, lines)
    res = [k for k in filtered if exclude not in k]
    Logger.info("Moving nodes count: {0}".format(len(res)))
    return res


  def check(node, seed_node):
    moving = moving_nodes(node, seed_node)
    pause = 0.75
    max_count = 50
    count = 0

    while len(moving) > 0 and count < max_count:
      sleep(pause)
      count += 1
      moving = moving_nodes(node, seed_node)

    Logger.info("Finished after {0} try".format(count))
    result = len(moving) == 0
    Logger.info("Node {0} ready to join".format("is" if result else "is not"))
    return result


  # if __name__ == "__main__":
  #   parser = argparse.ArgumentParser(description='Allow node join')
  #   parser.add_argument('ip', help='IP address of node')
  #   args = parser.parse_args()

  if valid_ip(ip_address):
    Logger.info("Asking to seed node {0} the cluster status excluded node {1}".format(seed_node, ip_address))
    return check(ip_address, seed_node)
  else:
    Logger.error("Invalid IP {0}".format(ip_address))
    return False