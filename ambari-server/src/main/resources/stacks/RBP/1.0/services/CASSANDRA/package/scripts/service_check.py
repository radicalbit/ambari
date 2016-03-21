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
import time

class ServiceCheck(Script):
  def service_check(self, env):
    import params
    env.set_params(params)
    time.sleep(10)
    seeds = params.seeds.split(",")
    host=seeds[0]
    cmdfile=format("/tmp/cmds")
    File(cmdfile,
         mode=0600,
         content=InlineTemplate("CREATE KEYSPACE IF NOT EXISTS smokedemotest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };\n"
                                "Use smokedemotest;\n"
                                "CREATE TABLE IF NOT EXISTS smokeusers (firstname text,lastname text,age int,email text,city text,PRIMARY KEY (lastname));\n"
                                "INSERT INTO smokeusers (firstname, lastname, age, email, city) VALUES ('John', 'Smith', 46, 'johnsmith@email.com', 'Sacramento');\n"
                                "DROP TABLE smokedemotest.smokeusers;\n"
                                "DROP KEYSPACE smokedemotest;\n\n")
         )
    Execute(format("{cassandra_bin_dir}/cqlsh {host} 9042 -f {cmdfile}"))


if __name__ == "__main__":
  ServiceCheck().execute()