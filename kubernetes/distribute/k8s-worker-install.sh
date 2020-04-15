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

if [ "$#" -ne 3 ]; then
    echo 'Your input format error. Ex: bash k8s-worker-install.sh ${K8S_MASTER_IP} ${TOKEN} ${HASH_CODE}'
    exit 2
fi

SOURCE="${BASH_SOURCE[0]}"
BIN_DIR="$( dirname "$SOURCE" )"
while [ -h "$SOURCE" ]
do
  SOURCE="$(readlink "$SOURCE")"
  BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
done
BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# The following command must be executed by root or sudo account
bash $BIN_DIR/k8s-pre-install.sh
masterHostIP=$1
token=$2
hashCode=$3

kubeadm join $masterHostIP:6443 --token $token --discovery-token-ca-cert-hash $hashCode
