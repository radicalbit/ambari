#!/usr/bin/env python
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
import subprocess

class FlinkServiceCheck(Script):
  # Service check for VSFTPD service
  def service_check(self, env):

    bin_dir = '/usr/lib/flink/bin'
    example_dir = '/usr/share/doc/flink/examples/batch'
    full_command = format("{bin_dir}/flink run -m yarn-cluster -yn 4 -yjm 1024 -ytm 4096 {example_dir}/WordCount.jar")
    proc = subprocess.Popen(full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate()
    response = stdout

    # response is
    # Passed the test
    # or
    # Failed the test!

    if 'Failed' in response:
      raise ComponentIsNotRunning()

if __name__ == "__main__":
  FlinkServiceCheck().execute()
