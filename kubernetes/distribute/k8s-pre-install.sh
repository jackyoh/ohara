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

username=$(whoami)
if [ "${username}" = "root" ]
then
  echo "Why use the root account?"
  echo "Use the root user install Kubernetes benefit is simple and convenient."
  echo "Avoid changing not an admin user. Of course, you can use the admin user "
  echo "and add the "sudo" keyword to execute install the Kubernetes shell script."
fi


yum install -y yum-utils device-mapper-persistent-data lvm2

# Is it exist docker command
if ! hash docker 2>/dev/null; then
  echo "Command 'docker' cannot be found on your system, install it."
  rm -rf /var/lib/docker/network/file/local-ky.db
  yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
  yum install -y docker-ce
  systemctl enable docker
  systemctl start docker
else
  dockerVersion=$(docker version --format '{{.Server.Version}}')
  echo "You have installed docker ${dockerVersion}"
  dockerMajorVersion="$(cut -d'.' -f1 <<<"$dockerVersion")"
  major=$((dockerMajorVersion))

  # check docer version
  if [[ $major < 17 ]]
  then
    echo "Your docker version is too old, please upgrade your docker version to 17+"
    exit 1
  fi
fi

systemctl disable firewalld
systemctl stop firewalld # You can only open the 6443 and 8080 port
sed -i s/^SELINUX=.*$/SELINUX=disabled/ /etc/selinux/config
setenforce 0 # SELinux must disable
sed -i '/ swap / s/^/#/' /etc/fstab
swapoff -a
echo '1' > /proc/sys/net/bridge/bridge-nf-call-iptables
echo -e "[kubernetes]\nname=Kubernetes\nbaseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64\nenabled=1\ngpgcheck=1\nrepo_gpgcheck=1\ngpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg\n       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg\n">/etc/yum.repos.d/kubernetes.repo

# Is it exist kubelet command
k8sVersion=1.14.1
if ! hash kubelet 2>/dev/null; then
  yum install -y kubelet-${k8sVersion} kubeadm-${k8sVersion} kubectl-${k8sVersion}
  systemctl enable kubelet
  systemctl start kubelet
else
  echo "You already installed the Kubernetes, OharaStream suggest you install the Kubernetes is ${k8sVersion}"
fi
