#!/bin/bash
pwdFile="/tmp/password.txt"

if [[ -z "${SAMBA_USER_NAME}" ]];
then
  SAMBA_USER_NAME="ohara"
fi

if [[ -z "${SAMBA_USER_PASS}" ]];
then
  SAMBA_USER_PASS="ohara"
fi

# create password file
echo "${SAMBA_USER_PASS}
${SAMBA_USER_PASS}" > $pwdFile

groupadd smbgrp
useradd ${SAMBA_USER_NAME} -G smbgrp
smbpasswd -a ${SAMBA_USER_NAME} < $pwdFile

/usr/sbin/smbd --foreground --no-process-group
