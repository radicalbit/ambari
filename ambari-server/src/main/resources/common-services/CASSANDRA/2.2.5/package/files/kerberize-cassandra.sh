#!/usr/bin/env bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

CURRENT_NODE=$1
CASSANDRA_NODES_NUMBER=$2
CQLSH_PATH=$3
NODETOOL_PATH=$4
CASSANDRA_PID_DIR=$5
CASSANDRA_INTERNALS_DIR=$6

COUNTER=0

while read IP; do
  if ssh -n $IP test -e /var/run/cassandra/cassandra.pid &> /dev/null; then
    COUNTER=$(($COUNTER + 1))
  fi
done <cassandra_hosts

if [ $COUNTER == $CASSANDRA_NODES_NUMBER ]; then
  SLEEP_TIME = $(($CASSANDRA_NODES_NUMBER * 15))
  sleep $SLEEP_TIME"s"

  while read IP; do
    cat > $CASSANDRA_INTERNALS_DIR/alter-keyspace-script <<EOF
ALTER KEYSPACE "system_auth" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $CASSANDRA_NODES_NUMBER };

EOF
    ssh -n $IP "$CQLSH_PATH $CURRENT_NODE 9042 -f $CASSANDRA_INTERNALS_DIR/alter-keyspace-script" &> /dev/null
  done <cassandra_hosts

  while read IP; do
    ssh -n $IP "$NODETOOL_PATH repair" &> /dev/null
  done <cassandra_hosts

#  while read IP; do
#    ssh -n $IP "kill `cat $CASSANDRA_PID_DIR/cassandra.pid`" &> /dev/null
#  done <cassandra_hosts
fi