#!/bin/bash
pwdFile="/tmp/password.txt"
echo "$FTP_USER_PASS
$FTP_USER_PASS" > $pwdFile
mkdir -p /tmp/storage
chown -R ohara:ohara /tmp/storage

pure-pw useradd ${FTP_USER_NAME} -u ohara -g ohara -d /tmp/storage -m < $pwdFile
pure-pw mkdb
pure-ftpd /opt/pureftpd/etc/pure-ftpd.conf
