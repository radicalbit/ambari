#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


echo "#########################################"
echo "#                                       #"
echo "#  BUILDING CUSTOM AMBARI DISTRIBUTION  #"
echo "#                                       #"
echo "#########################################"
echo ""

echo "-------------------------------"
echo "| Setting system environments |"
echo "-------------------------------"
# update the PATH as needed by the build
PATH=$PATH:/root/node-v0.10.41-linux-x64/bin:/root/apache-maven-3.0.5/bin:/root
PKG=$1
echo "PATH=$PATH"
# create the JAVA_OPTIONS needed by the build
export _JAVA_OPTIONS="-Xmx2048m -XX:MaxPermSize=1024m -Djava.awt.headless=true"

echo "-------------------------"
echo "| Install support tools |"
echo "-------------------------"
npm install -g brunch@1.7.20
npm config set unsafe-perm true
sh /root/setuptools-0.6c11-py2.7.egg

echo "-------------------------"
echo "| Building Distribution |"
echo "-------------------------"
cd /root/ambari
mvn versions:set -DnewVersion="2.2.0.0.0"
pushd ambari-metrics
mvn versions:set -DnewVersion="2.2.0.0.0"
popd
mvn -B clean install package -DnewVersion="2.2.0.0.0" -Dstack.distribution="RBD" $PKG -DskipTests -Dpython.ver="python >= 2.7" -Dfindbugs.skip=true -Preplaceurl
cd ..
