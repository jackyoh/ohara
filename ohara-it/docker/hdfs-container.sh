#!/bin/bash
#
# Copyright 2019 is-land
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage="USAGE: $0 [start|stop|--help] arg1 arg2 ..."
if [ $# -lt 1 ];
then
  echo $usage
  exit 1
fi

COMMAND=$1
case $COMMAND in
  start)
    start="true"
    shift
    ;;
  stop)
    stop="true"
    shift
    ;;
  --help)
    help="true"
    shift
    ;;
  *)
    echo $usage
    exit 1
    ;;
esac

while getopts n:s:u: option
do
 case "${option}"
 in
 n) nameNode=${OPTARG};;
 s) dataNodes=${OPTARG};;
 u) userName=${OPTARG};;
 esac
done

if [ "${help}" == "true" ];
then
  echo $usage
  echo "Argument             Description"
  echo "--------             -----------"
  echo "-n                   Set HDFS namenode hostname and port to start the datanode. example: -n host1:9000"
  echo "-s                   Set HDFS datanode hostname list. example: -s host1,host2,host3"
  echo "-u                   Set ssh user name to remote deploy datanode"
  exit 1
fi

if [[ -z "${userName}" ]];
then
  userName="ohara"
fi

if [[ -z "${nameNode}" ]] && [[ "$start" == "true" ]];
then
  echo 'Please setting the -n ${NAMENODE_HOST_AND_PORT} argument'
  exit 1
fi

nameNodeImageName="oharastream/ohara:hdfs-namenode"
dataNodeImageName="oharastream/ohara:hdfs-datanode"

nameNodeContainerName="namenode"
dataNodeContainerName="datanode"

IFS=","
if [ "$start" == "true" ];
then
  echo "Starting HDFS container"
  echo "Starting ${HOSTNAME} node namenode......"
  docker run -d -it --name ${nameNodeContainerName} --net host ${nameNodeImageName}

  for dataNode in $dataNodes;
  do
    echo "Starting ${dataNode} node datanode......"
    ssh ${userName}@$dataNode docker run -d -it --name ${dataNodeContainerName} --env HADOOP_NAMENODE=${nameNode} --net host ${dataNodeImageName}
  done
fi

if [ "$stop" == "true" ];
then
    echo "Stoping HDFS container"
    for dataNode in $dataNodes;
    do
      echo "Stoping ${dataNode} node datanode......"
      ssh ${userName}@$dataNode docker rm -f ${dataNodeContainerName}
    done
    echo "Stoping ${HOSTNAME} node namenode......"
    docker rm -f ${nameNodeContainerName}
fi
