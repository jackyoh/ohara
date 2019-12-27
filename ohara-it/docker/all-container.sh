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

if [ "${help}" == "true" ];
then
  echo $usage
  echo "Argument             Description"
  echo "--------             -----------"
  echo "--userName           Set all service user name"
  echo "--password           Set all service password"
  echo "--nameNode           Set HDFS namenode hostname"
  echo "--dataNodes          Set HDFS datanode hostname"
  echo "--oracleNode         Set Oracle database hostname"
  echo "--oraclePort         Set Oracle database port number"
  echo "--ftpNode            Set FTP server hostname"
  echo "--ftpServerIP        Set FTP server IP for expose ftp server"
  echo "--ftpPort            Set FTP server port number"
  echo "--ftpDataPortRange   Set FTP data port range. for example: --ftpDataPortRange 30000-30004"
  echo "--sambaNode          Set Samba server hostname"
  echo "--sambaPort          Set Samba server port number"
  exit 1
fi

ARGUMENT_LIST=("userName" "password" "nameNode" "dataNodes" "oracleNode" \
               "ftpNode" "sambaNode" "ftpServerIP" "ftpPort" "sambaPort" \
               "oraclePort" "ftpDataPortRange")

opts=$(getopt \
    --longoptions "$(printf "%s:," "${ARGUMENT_LIST[@]}")" \
    --name "$(basename "$0")" \
    --options "" \
    -- "$@"
)
eval set --$opts

while [[ $# -gt 0 ]]; do
  case "$1" in
    --userName)
      userName=$2
      shift 2
      ;;
    --password)
      password=$2
      shift 2
      ;;
    --nameNode)
      nameNode=$2
      shift 2
      ;;
    --dataNodes)
      dataNodes=$2
      shift 2
      ;;
    --oracleNode)
      oracleNode=$2
      shift 2
      ;;
    --oraclePort)
      oraclePort=$2
      shift 2
      ;;
    --ftpNode)
      ftpNode=$2
      shift 2
      ;;
    --sambaNode)
      sambaNode=$2
      shift 2
      ;;
    --ftpServerIP)
      ftpServerIP=$2
      shift 2
      ;;
    --ftpPort)
      ftpPort=$2
      shift 2
      ;;
    --ftpDataPortRange)
      ftpDataPortRange=$2
      shift 2
      ;;
    --sambaPort)
      sambaPort=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z ${nameNode} ]];
then
  echo "Please setting --nameNode argument"
  exit 1
fi

if [[ -z ${dataNodes} ]];
then
  echo "Please setting --dataNodes argument"
  exit 1
fi

if [[ -z ${oracleNode} ]];
then
  echo "Please setting --oracleNode argument"
  exit 1
fi

if [[ -z ${ftpNode} ]];
then
  echo "Please setting --ftpNode argument"
  exit 1
fi

if [[ -z ${sambaNode} ]];
then
  echo "Please setting --sambaNode argument"
  exit 1
fi

if [[ -z ${userName} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --userName argument"
  exit 1
fi

if [[ -z ${password} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --password argument"
  exit 1
fi

if [[ -z ${oraclePort} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --oraclePort argument"
  exit 1
fi

if [[ -z ${ftpServerIP} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --ftpServerIP argument"
  exit 1
fi

if [[ -z ${ftpPort} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --ftpPort argument"
  exit 1
fi

if [[ -z ${ftpDataPortRange} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --ftpDataPortRange argument. for example: --ftpDataPortRange 30000-30004"
  exit 1
fi

if [[ -z ${sambaPort} ]] && [[ "${start}" == "true" ]];
then
  echo "Please setting --sambaPort argument"
  exit 1
fi

if [[ "${start}" == "true" ]];
then
  echo "Starting hdfs container"
  bash hdfs-container.sh start -n ${nameNode} -s ${dataNodes}

  echo "Starting ftp container"
  bash ftp-container.sh start --user ${userName} --password ${password} --host ${ftpNode} --ip ${ftpServerIP} --port ${ftpPort} --dataPortRange ${ftpDataPortRange}

  echo "Starting samba container"
  bash samba-container.sh start --user ${userName} --password ${password} --host ${sambaNode} --sport 139 --dport ${sambaPort}

  echo "Starting oracle database container"
  bash oracle-container.sh start --user ${userName} --password ${password} --host ${oracleNode}

elif [[ "${stop}" == "true" ]];
then
  echo "Stop hdfs container"
  bash hdfs-container.sh stop -n ${nameNode} -s ${dataNodes}

  echo "Stop ftp container"
  bash ftp-container.sh stop --host ${ftpNode}

  echo "Stop samba container"
  bash samba-container.sh stop --host ${sambaNode}

  echo "Stop oracle database container"
  bash oracle-container.sh stop --host ${oracleNode}
fi
