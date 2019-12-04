#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [start|stop] -u"' ${USER_NAME} -p ${PASSWORD} ....'
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
  *)
    echo "USAGE: $0 [start|stop] -u"' ${USER_NAME} -p ${PASSWORD} ....'
    exit 1
    ;;
esac

sid="ORACLE_SID"
domain="localdomain"
port="1521"

ARGUMENT_LIST=("user" "password" "port" "sid" "domain")

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
    --domain)
      domain=$2
      shift 2
      ;;
    --help)
      domain=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z ${user} ]];
then
  echo 'Please set the --user ${USER_NAME} argument'
  exit 1
fi

if [[ -z ${password} ]];
then
  echo 'Please set the --password ${PASSWORD} argument'
  exit 1
fi

echo "user=$user"
echo "password=$password"
echo "port=$port"
echo "sid=$sid"
echo "domain=$domain"

