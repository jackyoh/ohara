#!/bin/bash
usage="USAGE: $0 [start|stop|--help] -u"' ${USER_NAME} -p ${PASSWORD} ....'

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
  echo "--user               Set oracle database user name"
  echo "--password           Set oracle database password"
  echo "--port               Set connection port for client"
  echo "--sid                Set connection sid"
  exit 1
fi

sid="xe"
port="1521"
containerName="oracle-benchmark-test"

ARGUMENT_LIST=("user" "password" "port" "sid")

opts=$(getopt \
    --longoptions "$(printf "%s:," "${ARGUMENT_LIST[@]}")" \
    --name "$(basename "$0")" \
    --options "" \
    -- "$@"
)
eval set --$opts

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      user=$2
      shift 2
      ;;
    --password)
      password=$2
      shift 2
      ;;
    --port)
      port=$2
      shift 2
      ;;
    --sid)
      sid=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z ${user} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --user ${USER_NAME} argument'
  exit 1
fi

if [[ -z ${password} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --password ${PASSWORD} argument'
  exit 1
fi

if [[ "${start}" == "true" ]];
then
  echo "Starting oracle database container"
  echo "Port is ${port}"
  docker run -d -i --name ${containerName} --restart=always -p ${port}:1521 --env DB_SID=${sid} store/oracle/database-enterprise:12.2.0.1
  sleep 3m
  docker exec -i $containerName bash -c "source /home/oracle/.bashrc;echo -e 'alter session set \"_ORACLE_SCRIPT\"=true;\ncreate user ${user} identified by ${password};\nGRANT CONNECT, RESOURCE, DBA TO ${user};'|sqlplus sys/Oradoc_db1@${sid} as sysdba"
  echo "Start oracle database complete. User name is ${user}"
fi

if [[ "${stop}" == "true" ]];
then
  echo "Stoping oracle database container"
  docker rm -f $containerName
fi
