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


usage="USAGE: $0 [hdfs|ftp|samba|oracle] [--help | arg1 arg2 ...]"
if [ $# -lt 1 ];
then
  echo $usage
  exit 1
fi

COMMAND=$1
case $COMMAND in
  hdfs)
    hdfs="true"
    shift
    ;;
  ftp)
    ftp="true"
    shift
    ;;
  samba)
    samba="true"
    shift
    ;;
  oracle)
    oracle="true"
    shift
    ;;
  *)
    echo $usage
    exit 1
    ;;
esac

if [[ "${hdfs}" == "true" ]];
then
   echo "Stop HDFS service"
   if [[ "$1" == "--help" ]];
   then
     bash hdfs-container.sh --help
   else
     bash hdfs-container.sh stop $@
   fi
fi

if [[ "${ftp}" == "true" ]];
then
  echo "Stop FTP service"
  if [[ "$1" == "--help" ]];
  then
    bash ftp-container.sh --help
  else
    bash ftp-container.sh stop $@
  fi
fi

if [[ "${samba}" == "true" ]];
then
  echo "Stop Samba service"
  if [[ "$1" == "--help" ]];
  then
    bash samba-container.sh --help
  else
    bash samba-container.sh stop $@
  fi
fi

if [[ "${oracle}" == "true" ]];
then
  echo "Stop Oracle service"
  if [[ "$1" == "--help" ]];
  then
    bash oracle-container.sh --help
  else
    bash oracle-container.sh stop $@
  fi
fi
