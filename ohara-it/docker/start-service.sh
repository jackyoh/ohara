#!/bin/bash

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
   echo "Start HDFS service"
   bash hdfs-container.sh start $@
fi

if [[ "${ftp}" == "true" ]];
then
  echo "Start Oracle service"
  bash ftp-container.sh start $@
fi

if [[ "${samba}" == "true" ]];
then
  echo "Start Samba service"
  bash samba-container.sh start $@
fi

if [[ "${oracle}" == "true" ]];
then
  echo "Start Oracle service"
  bash oracle-container.sh start $@
fi
