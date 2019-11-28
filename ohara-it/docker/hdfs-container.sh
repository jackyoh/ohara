#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [start-all|start-namenode|start-datanode|stop-all|stop-namenode|stop-datanode]" arg1 arg2 ...
  exit 1
fi

COMMAND=$1
case $COMMAND in
  start-all)
    start_all="true"
    shift
    ;;
  start-namenode)
    start_namenode="true"
    shift
    ;;
  start-datanode)
    start_datanode="true"
    shift
    ;;
  stop-all)
    stop_all="true"
    shift
    ;;
  stop-namenode)
    stop_namenode="true"
    shift
    ;;
  stop-datanode)
    stop_datanode="true"
    shift
    ;;
  *)
    echo "USAGE: $0 [start-all|start-namenode|start-datanode|stop-all|stop-namenode|stop-datanode]" arg1 arg2 ...
    ;;
esac

while getopts n: option
do
 case "${option}"
 in
 n) nameNode=${OPTARG};;
 esac
done

if [ -z "${nameNode}" ] && ([ "$start_all" == "true" ] || [ "$start_datanode" == "true" ]);then
  echo 'Please setting the -n ${NAMENODE_HOST_AND_PORT} argument'
  exit 1
fi

nameNodeContainerName="namenode"
dataNodeContainerName="datanode_${HOSTNAME}"

if [ "$start_all" == "true" ];
then
  echo "Start HDFS container"
  docker run -d -it --name ${nameNodeContainerName} --net host oharastream/ohara:hadoop-namenode
  docker run -d -it --name ${dataNodeContainerName} --env HADOOP_NAMENODE=${nameNode} --net host oharastream/ohara:hadoop-datanode
fi

if [ "$start_namenode" == "true" ];
then
  echo "Start HDFS Namenode container"
  docker run -d -it --name ${nameNodeContainerName} --net host oharastream/ohara:hadoop-namenode
fi

if [ "$start_datanode" == "true" ];
then
  echo "Start HDFS Datanode container"
  docker run -d -it --name ${dataNodeContainerName} --env HADOOP_NAMENODE=${nameNode} --net host oharastream/ohara:hadoop-datanode
fi

if [ "$stop_all" == "true" ];
then
  echo "Stop HDFS container"
  docker rm -f ${nameNodeContainerName}
  docker rm -f ${dataNodeContainerName}
fi

if [ "$stop_namenode" == "true" ];
then
  echo "Stop HDFS Namenode container"
  docker rm -f ${nameNodeContainerName}
fi

if [ "$stop_datanode" == "true" ];
then
  echo "Stop HDFS Datanode container"
  docker rm -f ${dataNodeContainerName}
fi
