#!/bin/bash
pureFTPConfPath="/opt/pureftpd/etc/pure-ftpd.conf"
if [[ -z "${FORCE_PASSIVE_IP}" ]];
then
  echo 'Please setting the ${FORCE_PASSIVE_IP} evnvironment variable'
  exit 1
fi

if [[ -z "${FTP_USER_NAME}" ]];
then
  FTP_USER_NAME="ohara"
fi

if [[ -z "${FTP_USER_PASS}" ]];
then
  FTP_USER_PASS="ohara"
fi

bash /opt/pureftpd/bin/pure-ftpd.sh > $pureFTPConfPath

pwdFile="/tmp/password.txt"
echo "$FTP_USER_PASS
$FTP_USER_PASS" > $pwdFile
mkdir -p /tmp/storage
chown -R ohara:ohara /tmp/storage

pure-pw useradd ${FTP_USER_NAME} -u ohara -g ohara -d /tmp/storage -m < $pwdFile
pure-pw mkdb
pure-ftpd $pureFTPConfPath
