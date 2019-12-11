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

echo "PureDB                       /opt/pureftpd/etc/pureftpd.pdb
PassivePortRange             30000 30009
ForcePassiveIP               ${FORCE_PASSIVE_IP}" > $pureFTPConfPath

pwdFile="/tmp/password.txt"
echo "$FTP_USER_PASS
$FTP_USER_PASS" > $pwdFile
mkdir -p /tmp/storage
chown -R ohara:ohara /tmp/storage

pure-pw useradd ${FTP_USER_NAME} -u ohara -g ohara -d /tmp/storage -m < $pwdFile
pure-pw mkdb
pure-ftpd $pureFTPConfPath
