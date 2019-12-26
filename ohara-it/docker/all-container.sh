#!/bin/bash

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

userName="ohara"
password="island123"
nameNode="samba"
dataNodes="samba"
oracleNode="samba"
ftpNode="samba"
FTP_SERVER_IP="10.100.0.220"
sambaNode="samba"


if [[ "${start}" == "true" ]];
then
  echo "Starting hdfs container"
  bash hdfs-container.sh start -n ${nameNode} -s ${dataNodes}

  echo "Starting oracle database container"
  bash oracle-container.sh start --user ${userName} --password ${password} --host ${oracleNode}

  echo "Starting ftp container"
  bash ftp-container.sh start --user ${userName} --password ${password} --host ${ftpNode} --ip ${FTP_SERVER_IP} --dataPortRange 30000-30004

  echo "Starting samba container"
  bash samba-container.sh start --user ${userName} --password ${password} --host ${sambaNode} --sport 139 --dport 445
fi

#if [[ "${stop}" == "true" ]];
#then
#fi